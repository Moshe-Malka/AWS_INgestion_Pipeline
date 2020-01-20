import os
import boto3

glue_client = boto3.client('glue', region_name='us-west-2')

def create_crawler(name, dest):
    try:
        glue_client.create_crawler(
            Name=name,
            Role=os.environ['CRAWLER_ARN'],
            DatabaseName=os.environ['DB_NAME'],
            Targets={'S3Targets' : [{'Path': f"s3://{dest}"}]}
            )
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
    folder = event['dest_path'].split('/')[1]
    target = f"s3://{event['dest_bucket']}/parquet/{folder}"
    crawler_name = f"Crawler_{folder}_parquet"
    
    print(f"Crawler Params: folder: {folder} , target : {target} , crawler_name : {crawler_name}")
    
    if not crawler_exists(crawler_name):
        print(f"Crawler <{crawler_name}> Not Found ==> creating a new one.")
        create_crawler(crawler_name, target)
    run_crawler(crawler_name)
    
    return {
        'crawler_name' : crawler_name,
        'dest_bucket': event['dest_bucket'],
        'dest_path': event['dest_path']
        }