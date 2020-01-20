import os, json
import boto3
from botocore.exceptions import ClientError

sqs_client = boto3.client('sqs', region_name='us-west-2')

def send_message(msg):
    try:
        response = sqs_client.send_message(QueueUrl=os.environ['TRANSFORMATIONS_QUEUE_URL'], MessageBody=json.dumps(msg))
        print(f"Successfully sent meessage to queue url <{os.environ['TRANSFORMATIONS_QUEUE_URL']}>.")
        print(f"Response : {response}")
    except ClientError as ce:
        print(f"Failed to send message to queue : {ce}")

def lambda_handler(event, context):
    print(f"Event : {event}")
    with open('manifest-file.json') as json_file:
        manifest_data = json.load(json_file)['manifest-list']
    for record in event['Records']:
        if record['eventName'] in ['MODIFY', 'INSERT']:
            rec = record['dynamodb']['NewImage']
            key = rec['etl-date']['S']
            files = list(rec['files']['M'].keys())
            for etl in manifest_data:
                print(f"ETL Record : {etl}")
                if set(files) == set(etl['files']):
                    print(f"Finished ETL processing")
                    send_message({
                        'etlName' : etl['etl-id'],
                        'eventID' : record['eventID'],
                        'eventName' : record['eventName'],
                        'filesDone' : files
                    })
                else:
                    print(f"Not Done with ETL")
                
                print(f"Expected Files : {etl['files']}")
                print(f"Finished Files : {files}")
        else:
            print("Record Event Name is 'REMOVE' - No Action Needed")
    return 