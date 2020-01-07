import json, os
import boto3

sfn_client = boto3.client('stepfunctions', region_name='us-west-2')
sf_arn = os.environ['STEP_FUNCTION_ARN']

def lambda_handler(event, context):
    print(f"Event : {event}")
    
    records = event['Records']
    for i, record in enumerate(records):
        state_id = json.loads(record['body'])['state_id']
        try:
            sf_name = f"StepFunction_{state_id}_{i}"
            response = sfn_client.start_execution(stateMachineArn=sf_arn, name=sf_name, input=record['body'])
            print(f"Succesfully Initiated an exacution run for SF <{sf_arn}>")
            print(f"Response :\n{response}")
        except Exception as e:
            print(f"Failed to run exacution for SF <{sf_arn}> : {e}")
    
    