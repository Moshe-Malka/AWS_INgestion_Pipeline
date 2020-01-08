import io
import sys
import zipfile
import boto3
from awsglue.utils import getResolvedOptions

def main():
    # get arguments that are passes to the glue job
    args = getResolvedOptions(sys.argv, ['source_bucket',
                                         'source_key',
                                         'destination_bucket',
                                         'destination_key'])

    print(f'args parsed : {args}')

    source_bucket = args["source_bucket"]
    source_key = args["source_key"]
    destination_bucket = args["destination_bucket"]
    destination_key = args["destination_key"]

    # get zip file from s3
    s3_object = boto3.resource('s3').Object(bucket_name=source_bucket, key=source_key)
    zip_file_byte_object = io.BytesIO(s3_object.get()["Body"].read())
    zip_file = zipfile.ZipFile(zip_file_byte_object)
    name_list = zip_file.namelist()
    ret_val = []

    # unzip the zip file and write contents to s3
    for file_path in name_list:
        print(f'processing path {file_path}')
        with zip_file.open(file_path) as f:
            file_byte_object = io.BytesIO(f.read())
            full_destination_key = f"{destination_key}{file_path}"
            print(f'uploading object to {full_destination_key}')
            boto3.client('s3').upload_fileobj(file_byte_object, destination_bucket, full_destination_key)
        ret_val.append(full_destination_key)
    
    print(f"Procced : {ret_val}")
    print('stopped unzip script')
    return ret_val

if __name__ == '__main__':
    main()