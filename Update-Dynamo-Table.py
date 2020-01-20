import os
import boto3
from botocore.exceptions import ClientError
from datetime import datetime

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['TABLE_NAME'])

def create_item(key, filename):
    try:
        response = table.put_item( Item={ 'etl-date': key, 'files': { filename : True } } )
        print(f"Successfully added new item")
        print(f"Response : {response}")
    except ClientError as ce:
        print(f"Failed to creat new item - key : {key}, filename : {filename}")
        print(ce)

def update_item(key, filename):
    try:
        response = table.update_item( Key={ 'etl-date': key },
            UpdateExpression='SET #head.#key = :value',
            ExpressionAttributeNames={ '#head': 'files', '#key' : filename },
            ExpressionAttributeValues={ ':value': True },
            ReturnValues='ALL_NEW'
        )
        print("Successfully created/updated item.")
        print(f"Response : {response}")
    except ClientError as ce:
        if ce.response['Error']['Code'] == 'ValidationException':
            print("Failed to update item - trying to create one")
            create_item(key, filename)
        else:
            print(f"Failed to create/update item : {ce}")
    
def lambda_handler(event, context):
    print(f"Event : {event}")
    date = str(datetime.now().date())
    filename = event['dest_path'].split('/')[1]
    update_item(date, filename)
    return "Done" 