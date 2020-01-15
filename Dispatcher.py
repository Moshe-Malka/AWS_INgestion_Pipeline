import json, os, datetime, uuid
import boto3
from botocore.exceptions import ClientError

glue_client = boto3.client('glue')
sf_client = boto3.client('stepfunctions')

def start_glue_job(job_name, args=False):
    try:
        if args:
            response = glue_client.start_job_run(JobName=job_name, Arguments=args)
        else:
            response = glue_client.start_job_run(JobName=job_name)
    except ClientError as ce:
        print(f"Failed to start glue job : {ce}")

def lambda_handler(event, context):
    print(f"Event : {event}")
    for record in event.get('Records', []):
        message_body = json.loads(record['body'])
        if 'message_source' in message_body and message_body['message_source'] == 'unziper-script':
            payload = {
                'args': message_body['args'],
                'tables': [x.split('/')[-1].replace('.csv', '').replace(' ', '_') for x in message_body['processed']],
                'crawler_destination': message_body['crawler_destination']
            } 
            response = sf_client.start_execution(
                stateMachineArn=os.environ['SF_ARN'],
                name=f'Crawler-Run_{uuid.uuid4()}',
                input=json.dumps(payload))
            print("Successfully started step function run")
            print(f"Response : {response}")
        elif 'Records' in message_body:
            s3_event = message_body['Records'][0]['s3']
            bucket_name = s3_event['bucket']['name']
            key = s3_event['object']['key']
            start_glue_job(job_name='unziper-script', args={
                '--source_bucket': bucket_name,
                '--source_key': key,
                '--destination_bucket': bucket_name
                })
        else:
            print("No Viable data in event")
    return
