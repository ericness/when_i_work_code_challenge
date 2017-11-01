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
OUTPUT_CSV_NAME='web_traffic.csv'

s3 = boto3.resource('s3', config=Config(signature_version=UNSIGNED))

mybucket = s3.Bucket(S3_BUCKET_NAME)

web_traffic_list = []

for s3_obj in mybucket.objects.filter(Prefix=S3_FOLDER_NAME):

    if re.match('.*\.csv$',s3_obj.key):
    
        try:
            #s3.Bucket(BUCKET_NAME).download_file(KEY, 'a.csv')
            #obj = s3.O(s3_obj.bucket_name).obj, Key=s3_obj.key)
            obj = s3.Object(s3_obj.bucket_name, s3_obj.key)
            
            
            df = pd.read_csv(io.BytesIO(obj.get()['Body'].read()), encoding='utf8')
            
            web_traffic_list.append(df)
            
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                print("The object does not exist.")
            else:
                raise

web_traffic = pd.concat(web_traffic_list, ignore_index=True)

web_traffic_user_path = web_traffic.groupby(['user_id','path'])['length'].sum()

web_traffic_user = web_traffic_user_path.reset_index()

web_traffic_user['length'] = web_traffic_user['length'].astype('int')
web_traffic_user = web_traffic_user.pivot(index='user_id',columns='path',values='length')
web_traffic_user = web_traffic_user.fillna(0)
web_traffic_user = web_traffic_user.astype(dtype='int')
web_traffic_user.to_csv(OUTPUT_CSV_NAME)
