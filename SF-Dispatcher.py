import json, os
import boto3

sfn_client = boto3.client('stepfunctions', region_name='us-west-2')
sf_arn = os.environ['STEP_FUNCTION_ARN']

def lambda_handler(event, context):
    """
    This Lambda handler cosumes messages from an SQS queue and invokes a Step Function for each message.
    """
    print(f"Event : {event}")
    
    for i, record in enumerate(event['Records']):
        state_id = json.loads(record['body'])['state_id']
        try:
            sf_name = f"StepFunction_{state_id}_{i}"
            response = sfn_client.start_execution(stateMachineArn=sf_arn, name=sf_name, input=record['body'])
            print(f"Succesfully Initiated an exacution run for SF <{sf_arn}>")
            print(f"Response :\n{response}")
        except Exception as e:
            print(f"Failed to run exacution for SF <{sf_arn}> : {e}")
    
    