import boto3
import os
from src.constants import AWS_SECRET_ACCESS_KEY, AWS_ACCESS_KEY_ID, AWS_REGION_NAME


class S3Client:

    s3_client = None
    s3_resource = None

    def __init__(self, region_name = AWS_REGION_NAME):
        """ 
        This Class gets aws credentials from src/constant and creates an connection with s3 bucket 
        and raise exception when environment variable is not set
        """

        if S3Client.s3_resource == None or S3Client.s3_client == None:
            __access_key_id = AWS_ACCESS_KEY_ID
            __secret_access_key = AWS_SECRET_ACCESS_KEY
        
            S3Client.s3_resource = boto3.resource('s3',
                                            aws_access_key_id = __access_key_id,
                                            aws_secret_access_key = __secret_access_key,
                                            region_name = region_name)
            
            S3Client.s3_client = boto3.client('s3',
                                        aws_access_key_id = __access_key_id,
                                        aws_secret_access_key = __secret_access_key,
                                        region_name = region_name)
            
        self.s3_resource = S3Client.s3_resource
        self.s3_client = S3Client.s3_client