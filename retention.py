import sys
import boto3
import json
import base64
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from botocore.exceptions import ClientError
from datetime import datetime, timedelta
from pyspark.sql.functions import *

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()

secret_name = "sis/stg/redshift"
region_name = "ap-southeast-1"
conn_name = "redshift-carrieverse-sis-stg"
glue_source_database = "sis_stg"
glue_source_table_1 = "sis_stg_public_log_login"
glue_source_table_2 = "sis_stg_public_log_register"

target_tbl_name = "public.retention"

session = boto3.session.Session()
client = session.client(
    service_name='secretsmanager',
    region_name=region_name
)

try:
    get_secret_value_response = client.get_secret_value(
        SecretId=secret_name
    )
except ClientError as e:
    raise e

if 'SecretString' in get_secret_value_response:
	secret = json.loads(get_secret_value_response['SecretString'])
else:
	secret = json.loads(base64.b64decode(get_secret_value_response['SecretBinary']))

target_options = {
    "url": secret.get('jdbcUrl'),
    "dbtable": target_tbl_name,
    "redshiftTmpDir": args["TempDir"],
    "user": secret.get('username'),
    "password": secret.get('password'),
    "postactions": "COMMIT"
}

#strdDate = datetime.strptime("2023-07-06", "%Y-%m-%d").date()
strdDate = (datetime.now() - timedelta(1)).date()

print("Target Date : ", strdDate, "(UTC)\n")

def TimestampToDate(rec):
    rec["created_dt"] = rec["created_dt"].date()
    return rec
    
def filter_date(rec):
    return rec["created_dt"] <= strdDate
    
log_login = glueContext.create_dynamic_frame.from_catalog(
    database=glue_source_database, 
    table_name=glue_source_table_1,
    redshift_tmp_dir = args["TempDir"],
    transformation_ctx = "log_login"
)

log_login = log_login.map(f = TimestampToDate).filter(f = lambda x : x["created_dt"] in [strdDate])
cnt = log_login.count()
print("\nTarget Count : ", cnt,"\n")

log_login_df = log_login.toDF()
log_login_df = log_login_df.groupby(['game_id','created_dt','uid']).agg(countDistinct('uid').alias('distinct_uids'))
log_login = DynamicFrame.fromDF(log_login_df, glueContext, "log_login").rename_field("created_dt", "curr_date").rename_field("game_id", "game_id_uv").rename_field("uid", "uid_uv").drop_fields(['distinct_uids'])

log_register = glueContext.create_dynamic_frame.from_catalog(
    database=glue_source_database, 
    table_name=glue_source_table_2,
    redshift_tmp_dir = args["TempDir"],
    transformation_ctx = "log_register"
)

log_register = log_register.map(TimestampToDate).filter(filter_date)

joined_table_dyf = log_register.join(
    paths1=["game_id", "uid"], paths2=["game_id_uv", "uid_uv"], frame2=log_login
).drop_fields(["game_id_uv","uid_uv", "os_code", "country_code", "id", "item_id"]).rename_field('created_dt','reg_date')

# DynamicFrame을 DataFrame으로 변환
print(joined_table_dyf.toDF().show())

cnt = joined_table_dyf.count()
print("\nTarget Count : ", cnt,"\n")

if cnt > 0:
    mapped_source_df = joined_table_dyf.toDF()
    source_df = mapped_source_df.groupby(['game_id', 'env_code', 'reg_date']).agg(count('uid').alias('ret_count'), first('curr_date').alias('curr_date'))
    joined_table_dyf = DynamicFrame.fromDF(source_df, glueContext, "joined_table_dyf")
    print("\nOutPut Table: \n")
    print(joined_table_dyf.toDF().show())
    dropnullfields = DropNullFields.apply(frame = joined_table_dyf, transformation_ctx = "dropnullfields")
    print("\nOutPut Table: \n")
    print(dropnullfields.toDF().show())
    glueContext.write_dynamic_frame.from_options(frame = dropnullfields, connection_type="redshift", connection_options=target_options)

job.commit()
