import os
import boto3
from botocore.exceptions import ClientError

glue_client = boto3.client('glue', region_name='us-west-2')

def run_etl_job(db, table, bucket):
    print("Running Glue ETL Job.")
    try:
        job_response = glue_client.start_job_run(
            JobName=os.environ['ETL_JOB_NAME'],
            WorkerType='G.1X',
            NumberOfWorkers=20,
            Arguments={
                '--db' : db,
                '--table' : table,
                '--bucket' : bucket
            },
            Timeout=60*4
        )
        print(f"Successfully ran job {os.environ['ETL_JOB_NAME']}")
        print(f"Response : {job_response}")
        return job_response['JobRunId']
    except ClientError as ce:
        print(f"Failed to run job <{os.environ['ETL_JOB_NAME']}> : {ce}")

def lambda_handler(event, context):
    print(f"Event : {event}")
    table = event['dest_path'].split('/')[1]
    job_run_id = run_etl_job("maindb_parquet", table, event['dest_bucket'])
    return {
        'glue_job_run_id' : job_run_id,
        'glue_job_name': os.environ['ETL_JOB_NAME'],
        'dest_bucket' : event['dest_bucket'],
        'dest_path' : event['dest_path']
    }