import os
import boto3

glue_client = boto3.client('glue', region_name='us-west-2')
crawler_role_arn = os.environ['CRAWLER_ARN']
glue_db_name = os.environ['DB_NAME']
        
def create_crawler(name, dest):
    try:
        glue_client.create_crawler(Name=name, Role='arn:aws:iam::458558152824:role/GlueCrawlerRole', DatabaseName=glue_db_name, Targets={'S3Targets' : [{'Path': f"s3://{dest}"}]})
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
    
    """
    {
      "job_state": "RUNNING",
      "glue_job_run_id": "jr_d09fbb3af196eb45952a98ed95c7b0ddfded3271949fd852be6eba6e7470c197",
      "glue_job_name": "unziper-script",
      "s3_event": {
        "s3SchemaVersion": "1.0",
        "configurationId": "ingestion",
        "bucket": {
          "name": "files-drop-point",
          "ownerIdentity": {
            "principalId": "A1DJ495R6WJ3IV"
          },
          "arn": "arn:aws:s3:::files-drop-point"
        },
        "object": {
          "key": "raw/comp.csv.zip",
          "size": 565,
          "eTag": "96ac59907ab1ab60adb49abc5368fce7",
          "sequencer": "005E04B2CD549FFA79"
        }
      }
    }

    """