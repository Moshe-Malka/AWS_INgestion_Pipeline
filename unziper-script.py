import sys, json, io, zipfile
import boto3
from awsglue.utils import getResolvedOptions

sqs_client = boto3.client('sqs')
queue_url = "<FILL_THIS>"

def send_message(message):
    try:
        response = sqs_client.send_message(QueueUrl=queue_url, MessageBody=json.dumps(message))
        print(f"Succesfully sent message : {message}")
        print(f"Response : {response}")
    except ClientError as ce:
        print(f"Failed to send message : {ce}")

def main():
    # get arguments that are passes to the glue job
    args = getResolvedOptions(sys.argv, ['source_bucket', 'source_key', 'destination_bucket'])

    print(f'args parsed : {args}')

    source_bucket = args["source_bucket"]
    source_key = args["source_key"]
    destination_bucket = args["destination_bucket"]

    # get zip file from s3
    s3_object = boto3.resource('s3').Object(bucket_name=source_bucket, key=source_key)
    zip_file_byte_object = io.BytesIO(s3_object.get()["Body"].read())
    zip_file = zipfile.ZipFile(zip_file_byte_object)
    name_list = [x for x in zip_file.namelist() if '.csv' in x]
    
    total = len(name_list)
    # unzip the zip file and write contents to s3
    for i, file_path in enumerate(name_list):
        print(f'processing {i} out of {total} - <{file_path}>')
        with zip_file.open(file_path) as f:
            file_byte_object = io.BytesIO(f.read())
            top_folder = source_key.split('/')[-1].replace('.zip', '')
            full_destination_key = f"staging/{top_folder}/{file_path.split('/')[-1]}"
            print(f'Full Destination Key : <{full_destination_key}>')
            boto3.client('s3').upload_fileobj(file_byte_object, destination_bucket, full_destination_key)

    print(f'Finished Unziping <{source_bucket}/{source_key}>')
    message = { 'message_source': 'unziper-script', 'dest_bucket' : destination_bucket, 'dest_path': f"staging/{source_key.split('/')[-1].replace('.zip', '')}/" }
    send_message(message)
    return "Done"

if __name__ == '__main__':
    main()