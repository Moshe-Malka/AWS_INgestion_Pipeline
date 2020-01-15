import boto3

glue_client = boto3.client('glue', region_name='us-west-2')

def get_crawler_state(crawler_name):
    try:
        return glue_client.get_crawler(Name=crawler_name)['Crawler']['State']
    except Exception as e:
        print(f"Failed to get crawler state : {e}")
        return None

def lambda_handler(event, context):
    print(f"Event : {event}")
    if 'attempt_number' in event:
        attempt_number = event['attempt_number'] + 1
    else:
        attempt_number = 1
    return {
        'attempt_number': attempt_number,
        'crawler_state' : get_crawler_state(event['crawler_name']),
        'crawler_name': event['crawler_name'],
        'tables':  event['tables']
    }
