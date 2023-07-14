import sys
import boto3
import json
import base64
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col, when, countDistinct
from botocore.exceptions import ClientError
from datetime import datetime, timedelta

# Get Glue context and parameters
glue_context = GlueContext(SparkContext.getOrCreate())
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'TempDir'])

job = Job(glue_context)
job.init(args['JOB_NAME'], args)

secret_name = "sis/stg/redshift"
region_name = "ap-southeast-1"
conn_name = "redshift-carrieverse-sis-stg"
glue_db = "sis_stg"
glue_login_table = "sis_stg_public_log_login"
glue_register_table = "sis_stg_public_log_register"
target_table = "public.mau"

# Get AWS Secrets
session = boto3.session.Session()
aws_client = session.client(
    service_name='secretsmanager',
    region_name=region_name
)
try:
    get_secret_value_response = aws_client.get_secret_value(
        SecretId=secret_name
    )
except ClientError as e:
    raise e

if 'SecretString' in get_secret_value_response:
	secret = json.loads(get_secret_value_response['SecretString'])
else:
	secret = json.loads(base64.b64decode(get_secret_value_response['SecretBinary']))

# Define tables
login_table_dyf = glue_context.create_dynamic_frame.from_catalog(
    database = glue_db, 
    table_name = glue_login_table,
    redshift_tmp_dir = args["TempDir"],
    transformation_ctx = "login_table_dyf"
)
register_table_dyf = glue_context.create_dynamic_frame.from_catalog(
    database = glue_db,
    table_name = glue_register_table,
    redshift_tmp_dir = args["TempDir"],
    transformation_ctx = "register_table_dyf"
)
register_table_df = register_table_dyf.toDF().withColumnRenamed("created_dt", "created_dt_2")

# Define target date and filter source data
now = datetime.now()
start_date_last_month = datetime(now.year, now.month - 1, 1)
end_date_last_month = datetime(now.year, now.month, 1) - timedelta(days=1)
start_date = start_date_last_month.date()
end_date = end_date_last_month.date()
print("\nStart Date : ", start_date_last_month.date(), "(UTC)\n")
print("\nEnd Date : ", end_date_last_month.date(), "(UTC)\n")

mapped_source_dyf = login_table_dyf.map(f=lambda x: {"id": x["id"], "item_id": x["item_id"], "env_code": x["env_code"], "game_id": x["game_id"], "os_code": x["os_code"], "uid": x["uid"], "created_dt": x["created_dt"].date()})
mapped_source_dyf = mapped_source_dyf.filter(f=lambda x: start_date <= x["created_dt"] <= end_date).map(lambda record: {
    **record,
    'login_start_date': start_date,
    'login_end_date': end_date
})
mapped_source_df = mapped_source_dyf.toDF()

target_cnt = mapped_source_dyf.count()
print("\nTarget Count : ", target_cnt, "\n")
if target_cnt > 0:
    target_table_options = {
        "url": secret.get('jdbcUrl'),
        "dbtable": target_table,
        "redshiftTmpDir": args["TempDir"],
        "user": secret.get('username'),
        "password": secret.get('password'),
        "postactions": "COMMIT"
    }

    # Join tables on uid, env_code, game_id, start_date, end_date
    joined_df = mapped_source_df.join(register_table_df, on=['uid', 'env_code', 'game_id'], how='left')
    joined_df = joined_df.withColumn("country_code", when(col("country_code").isNull(), "XX").otherwise(col("country_code")))

    # Group by env_code, game_id, country_code, and os_code
    grouped_df = joined_df.\
        groupBy(col("env_code"), col("game_id"), col("country_code"), col("os_code"), col("login_start_date"), col("login_end_date")).\
        agg(countDistinct(col("uid")).alias("login_count"))
    # Convert back to dynamic frame and write to destination
    output_dyf = DynamicFrame.fromDF(grouped_df, glue_context, "output_dyf")
    print("\nOutPut Table: \n")
    output_dyf.toDF().show()
    # drop_null_fields = DropNullFields.apply(frame=output_dyf, transformation_ctx="dropnullfields")
    glue_context.write_dynamic_frame.from_options(frame=output_dyf, connection_type="redshift", connection_options=target_table_options)
    
job.commit()