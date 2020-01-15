import os
import boto3

glue_client = boto3.client('glue', region_name='us-west-2')
crawler_role_arn = os.environ['CRAWLER_ARN']
glue_db_name = os.environ['DB_NAME']
        
def create_crawler(name, dest):
    try:
        glue_client.create_crawler(Name=name, Role='<FILL_THIS>', DatabaseName=glue_db_name, Targets={'S3Targets' : [{'Path': f"s3://{dest}"}]})
        print(f"Succefully created new crawler with name <{name}>.")
    except Exception as e:
        print(f"Failed to create new crawler with name <{name}>.")
        print(e)

def crawler_exists(name):
    try:
        glue_client.get_crawler(Name=name)
        return True
    except glue_client.exceptions.EntityNotFoundException:
        return False
        
def run_crawler(name):
    try:
        glue_client.start_crawler(Name=name)
        print(f"Succefully Started Crawler <{name}>")
    except glue_client.exceptions.CrawlerRunningException:
        print(f"Crawler <{name}> already running")
    except glue_client.exceptions.EntityNotFoundException:
        print(f"Crawler <{name}> Not Found")

def lambda_handler(event, context):
    print(f"Event : {event}")
    data = event['args']
    destination_bucket = data['destination_bucket']
    crawler_destination = event['crawler_destination']
    crawler_dest = f"{destination_bucket}/{crawler_destination}"
    crawler_name = f"Crawler_{crawler_destination.split('/')[-2]}"
    
    if not crawler_exists(crawler_name):
        print(f"Crawler <{crawler_name}> Not Found ==> creating a new one.")
        create_crawler(crawler_name, crawler_dest)
    run_crawler(crawler_name)
    
    return { 'crawler_name' : crawler_name, 'tables' : event['tables'], 'crawler_destination': crawler_destination }
  
