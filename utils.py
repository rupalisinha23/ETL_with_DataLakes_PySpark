import boto3
from botocore.exceptions import ClientError

def create_bucket(s3, bucket_name):
    """
    This function create an Amazon S3 bucket
    :param bucket_name: Unique string name
    :return: True if bucket is created, else False
    """
    
    try:
        s3.create_bucket(Bucket=bucket_name)
    except ClientError as e:
        print(f'ERROR: {e}')
        return False
    return True
