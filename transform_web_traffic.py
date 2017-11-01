import io
import re
import pandas as pd
import boto3
import botocore
from botocore import UNSIGNED
from botocore.client import Config

#BUCKET_NAME = 'https://s3-us-west-2.amazonaws.com/cauldron-workshop/data'
S3_BUCKET_NAME = 'cauldron-workshop'
S3_FOLDER_NAME='data'
#KEY = 'a.csv'

#s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))

s3 = boto3.resource('s3', config=Config(signature_version=UNSIGNED))

mybucket = s3.Bucket(S3_BUCKET_NAME)



for s3_obj in mybucket.objects.filter(Prefix=S3_FOLDER_NAME):

    if s3_obj.key
    
    print(s3_obj)
    
#    try:
#        #s3.Bucket(BUCKET_NAME).download_file(KEY, 'a.csv')
#        #obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=KEY)
#        df = pd.read_csv(io.BytesIO(obj['Body'].read()), encoding='utf8')
#    except botocore.exceptions.ClientError as e:
#        if e.response['Error']['Code'] == "404":
#            print("The object does not exist.")
#        else:
#            raise
#            