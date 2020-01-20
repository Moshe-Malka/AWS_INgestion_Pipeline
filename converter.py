import sys
from datetime import datetime

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'bucket', 'db', 'table'])

now = datetime.now()
print(f"Start Time : {now.strftime('%Y-%m-%d %H:%M:%S')}")

db = args['db']
table = args['table']
bucket = args['bucket']

target = f"s3://{bucket}/parquet/{table}/year={now.strftime('%Y')}/month={now.strftime('%m')}/day={now.strftime('%d')}/"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read files 
df = glueContext.create_dynamic_frame_from_catalog(database=db, table_name=table)

# Write data to folder
out = glueContext.write_dynamic_frame.from_options(frame = df, connection_type = "s3", connection_options = { "path": target }, format = "parquet")

end = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print(f"End Time : {end}")
job.commit()