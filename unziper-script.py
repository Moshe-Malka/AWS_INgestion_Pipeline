import sys, json, io, zipfile
import boto3
# these libraries are not available via pypi but they are available in the glue environment on aws
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
    """
    get the sources via the arguments that
    are passed to the glue job,
    get the zip file from s3, unzip
    and dump the result to s3.
    """
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
    
    processed = []
    # unzip the zip file and write contents to s3
    for file_path in name_list:
        print(f'processing email path {file_path}')
        with zip_file.open(file_path) as f:
            print(f"File Path : {file_path}")
            file_byte_object = io.BytesIO(f.read())
            full_destination_key = f"staging/{source_key.split('/')[-1]}/{'/'.join(file_path.split('/')[:-1])}/{file_path.split('/')[-1].replace('.csv', '').replace(' ', '_')}/{file_path.split('/')[-1]}"
            print(f'Full Destination Key {full_destination_key}')
            boto3.client('s3').upload_fileobj(file_byte_object, destination_bucket, full_destination_key)
            processed.append(full_destination_key)

    print(f'Finished Unziping {source_bucket}/{source_key}')
    message = { 'message_source': 'unziper-script', 'args': args, 'processed': processed, 'crawler_destination': '/'.join(processed[0].split('/')[:-2]) }
    send_message(message)
    return "Done"

if __name__ == '__main__':
    main()
