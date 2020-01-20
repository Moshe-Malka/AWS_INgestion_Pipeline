import boto3
from botocore.exceptions import ClientError

glue_client = boto3.client('glue')

def get_job_state(job_run_id, job_name):
    """ State Types : STARTING'|'RUNNING'|'STOPPING'|'STOPPED'|'SUCCEEDED'|'FAILED'|'TIMEOUT' """
    try:
        response = glue_client.get_job_run(JobName=job_name, RunId=job_run_id)
        print(f"Job Status Response : {response}")
        state = response['JobRun']['JobRunState']
        print(f"Job <{job_name}> |  {state}")
        return state
    except ClientError as ce:
        print(f"Failed to get job state for job run id : <{job_run_id}> : {ce}")

def lambda_handler(event, context):
    print(f"Event : {event}")
    glue_job_run_id = event['glue_job_run_id']
    glue_job_name = event['glue_job_name']
    state = get_job_state(glue_job_run_id, glue_job_name)
    if 'attempt_number' in event: attempt_number = event['attempt_number'] + 1
    else: attempt_number = 1
    return {
            'job_state' : get_job_state(glue_job_run_id, glue_job_name),
            'attempt_number': attempt_number,
            'glue_job_run_id' : glue_job_run_id,
            'glue_job_name' : glue_job_name,
            'dest_bucket' : event['dest_bucket'],
            'dest_path' : event['dest_path']
            }
    
    