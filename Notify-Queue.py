import os, json
import boto3
from botocore.exceptions import ClientError

sqs_client = boto3.client('sqs', 'us-west-2')

def send_message(message):
    try:
        response = sqs_client.send_message(QueueUrl=os.environ['QUEUE_URL'], MessageBody=json.dumps(message))
        print("Successfully sent message to queue")
        print(f"Response : {response}")
    except ClientError as ce:
        print(f"Failed to send message")
        print(ce)
    
def lambda_handler(event, context):
    print(f"Event : {event}")
    send_message({
          "crawler_name": event['crawler_name'],
          "tables": event['tables'],
          "destinations": [f"{event['crawler_destination']}/{x}" for x in event['tables']]
    })
    
    
    """
        {
          "attempt_number": 1,
          "crawler_state": "STOPPING",
          "crawler_name": "Crawler_test-files.zip",
          "tables": [
            "Sales_rand",
            "Sales_Records"
          ],
          "crawler_destination": "staging/test-files.zip/data-for-Moshe-Idan-lab"
        }
    """