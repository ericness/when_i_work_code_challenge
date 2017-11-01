import argparse
import io
import re
import pandas as pd
import boto3
import botocore

# parse command args to set run configuration
parser = argparse.ArgumentParser(description='Transform raw web traffic data into a pivoted table of aggregated time per path by user.')
parser.add_argument('bucket',type=str,
                    help='Name of S3 bucket that contains web traffic data')
parser.add_argument('--prefix',type=str,
                    help='Prefix to filter S3 keys by')
parser.add_argument('--output',type=str,
                    help='Name of output CSV file')

args = parser.parse_args()

# set configuration variables from command line args
S3_BUCKET_NAME = args.bucket
if args.prefix:
    S3_PREFIX=args.prefix
else:
    S3_PREFIX=''
if args.output:
    OUTPUT_CSV_NAME=args.output
else:
    OUTPUT_CSV_NAME='web_traffic.csv'

# use the UNSIGNED signature version for anonymous access
s3 = boto3.resource('s3', config=botocore.client.Config(signature_version=botocore.UNSIGNED))

# set up objects to iterate through list of S3 objects
s3_bucket = s3.Bucket(S3_BUCKET_NAME)

if S3_PREFIX!='':
    s3_bucket_objects = s3_bucket.objects.filter(Prefix=S3_PREFIX)
else:
    s3_bucket_objects = s3_bucket.objects.all()

# list of dataframes created from the CSV files
web_traffic_list = []

# iterate through CSV files and parse them into dataframes
try:
    for s3_obj in s3_bucket_objects:
    
        # only process CSV files
        if re.match('.*\.csv$',s3_obj.key):
        
            obj = s3.Object(s3_obj.bucket_name, s3_obj.key)
            web_traffic_subset = pd.read_csv(io.BytesIO(obj.get()['Body'].read()), encoding='utf8')
 
            # check structure and contents of dataframe
            
            if set(['user_id','path','length']).issubset(web_traffic_subset.columns):
                
                
                
                web_traffic_list.append(web_traffic_subset[['user_id','path','length']])
                
except botocore.exceptions.ClientError as e:
    print(e.response['Error']['Message'])
    exit()


# check length of list
web_traffic = pd.concat(web_traffic_list, ignore_index=True)

web_traffic_user_path = web_traffic.groupby(['user_id','path'])['length'].sum()

web_traffic_user = web_traffic_user_path.reset_index()

web_traffic_user['length'] = web_traffic_user['length'].astype('int')
web_traffic_user = web_traffic_user.pivot(index='user_id',columns='path',values='length')
web_traffic_user = web_traffic_user.fillna(0)
web_traffic_user = web_traffic_user.astype(dtype='int')
web_traffic_user.to_csv(OUTPUT_CSV_NAME)
