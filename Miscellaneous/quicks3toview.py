import boto3

bucket_name = 'data-handling-public'
key = 'products.csv'
local_file_path = 'products.csv'

s3 = boto3.client('s3')

try:
    s3.download_file(bucket_name, key, local_file_path)
    print(f"File downloaded: {local_file_path}")
except Exception as e:
    print(f"Error: {e}")
