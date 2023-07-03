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
glueContext = GlueContext(SparkContext.getOrCreate())
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'TempDir'])

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

secret_name = "sis/stg/redshift"
region_name = "ap-southeast-1"
conn_name = "redshift-carrieverse-sis-stg"
glue_db = "sis_stg"
glue_register_table = "sis_stg_public_log_register"
target_table = "public.nru"

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
register_table_dyf = glueContext.create_dynamic_frame.from_catalog(
    database = glue_db,
    table_name = glue_register_table,
    redshift_tmp_dir = args["TempDir"],
    transformation_ctx = "register_table_dyf"
)
log_cnt = register_table_dyf.count()
print("\nOriginal Count : ", log_cnt, "\n")

# Define target date and filter source data
strdDate = (datetime.now() - timedelta(1)).date()
print("\nTarget Date : ", strdDate, "(UTC)\n")
mapped_source_dyf = register_table_dyf.map(f=lambda x: {"id": x["id"], "item_id": x["item_id"], "env_code": x["env_code"], "game_id": x["game_id"], "country_code": x["country_code"], "uid": x["uid"], "created_dt": x["created_dt"].date()})
mapped_source_dyf = mapped_source_dyf.filter(f=lambda x: x["created_dt"] == strdDate)
mapped_source_df = mapped_source_dyf.toDF()

target_cnt = mapped_source_dyf.count()
print("\nTarget Count : ", target_cnt, "\n")
if target_cnt > 0:
    target_table_options = {
        "url": secret.get('jdbcUrl'),
        "dbtable": target_table,
        "redshiftTmpDir": args["TempDir"],
        "user": secret.get('username'),
        "password": secret.get('password')
    }

    # Group by env_code, game_id, country_code, created_dt
    grouped_df = mapped_source_df.\
        groupBy(col("env_code"), col("game_id"), col("country_code"), col("created_dt")).\
        agg(countDistinct(col("uid")).alias("register_count")).\
        withColumnRenamed("created_dt", "register_date")
    # Convert back to dynamic frame and write to destination
    output_dyf = DynamicFrame.fromDF(grouped_df, glueContext, "output_dyf")
    print("\nOutPut Table: \n")
    output_dyf.toDF().show()
    # drop_null_fields = DropNullFields.apply(frame=output_dyf, transformation_ctx="dropnullfields")
    glueContext.write_dynamic_frame.from_options(frame=output_dyf, connection_type="redshift", connection_options=target_table_options)

job.commit()