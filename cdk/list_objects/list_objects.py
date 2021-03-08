from __future__ import print_function
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
from botocore.exceptions import ClientError
from elasticsearch import Elasticsearch
import datetime as datetime
import botocore
import logging
import boto3
import base64
import gzip
import time
import json
import io
import csv
import sys
import os
import gzip


################################################################################################################
#   References
################################################################################################################
# https://alexwlchan.net/2017/07/listing-s3-keys/


################################################################################################################
#   Config
################################################################################################################
region = os.environ['AWS_REGION']
QUEUEURL = os.environ['QUEUEURL']
S3_BUCKET_NAME = os.environ['S3_BUCKET_NAME']
S3_BUCKET_PREFIX = os.environ['S3_BUCKET_PREFIX']
S3_BUCKET_SUFFIX = os.environ['S3_BUCKET_SUFFIX']
ELASTICSEARCH_HOST = os.environ['ELASTICSEARCH_HOST']
logging_level_name = os.getenv('LOGGING_LEVEL', default = 'INFO')
logging_level = logging._nameToLevel[logging_level_name]
logging.basicConfig(stream=sys.stdout, level=logging_level)
logger = logging.getLogger()
logger.setLevel(logging_level)

sqs_client = boto3.client('sqs')
s3_client = boto3.client('s3')

# Connect to Elasticsearch Service Domain
# service = 'es'
# credentials = boto3.Session().get_credentials()
# awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)
# elasticsearchclient = Elasticsearch(
#     hosts = [{'host': ELASTICSEARCH_HOST, 'port': 443}],
#     http_auth = awsauth,
#     use_ssl = True,
#     verify_certs = True,
#     connection_class = RequestsHttpConnection
# )



def get_matching_s3_keys_v1(bucket, prefix='', suffix=''):
    """
    Generate the keys in an S3 bucket.

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch keys that start with this prefix (optional).
    :param suffix: Only fetch keys that end with this suffix (optional).
    """
    keys = []
    kwargs = {'Bucket': bucket, 'Prefix': prefix}
    while True:
        resp = s3_client.list_objects_v2(**kwargs)
        for obj in resp['Contents']:
            key = obj['Key']
            if key.endswith(suffix):
                keys.append(obj['Key'])
        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break

    return keys



################################################################################################################
#   List files from S3  
################################################################################################################
def get_matching_s3_keys(bucket, prefix='', suffix=''):
    """
    Generate the keys in an S3 bucket.

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch keys that start with this prefix (optional).
    :param suffix: Only fetch keys that end with this suffix (optional).
    """
    objects = []
    kwargs = {'Bucket': bucket, 'Prefix': prefix}
    while True:
        resp = s3_client.list_objects_v2(**kwargs)
        for obj in resp['Contents']:
            key = obj['Key']
            if key.endswith(suffix):
                objects.append(obj)
                # keys.append(obj['Key'])
        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break

    return objects

def get_all_s3_keys(bucket):
    """Get a list of all keys in an S3 bucket."""
    keys = []

    kwargs = {'Bucket': bucket}
    while True:
        resp = s3.list_objects_v2(**kwargs)
        for obj in resp['Contents']:
            keys.append(obj['Key'])

        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break

    return keys


################################################################################################################
#   enqueue a message onto the SQS queue
################################################################################################################
def enqueue_object(s3_object, S3_BUCKET_NAME, S3_BUCKET_PREFIX, S3_BUCKET_SUFFIX):
    # payload ={
#     "ETag": "\"51ec3079c61643d5e3e1b0b390141caf\"",
#     "Key": "images/000030_22:21:09.000030_diagram.png",
#     "LastModified": "2021-03-07 06:34:53+00:00",
#     "Size": 98831,
#     "StorageClass": "STANDARD",
#     "bucket_name": "s3workflow-imagesbucketd1ef9a17-16f4rwi6wz5w7",
#     "bucket_prefix": "images/",
#     "bucket_suffix": ""
# }
    

    s3_object['bucket_name'] = S3_BUCKET_NAME
    s3_object['bucket_prefix'] = S3_BUCKET_PREFIX
    s3_object['bucket_suffix'] = S3_BUCKET_SUFFIX
    str_payload = json.dumps(s3_object, indent=4, sort_keys=True, default=str)
    
    response = sqs_client.send_message(
        QueueUrl=QUEUEURL,
        DelaySeconds=1,
        # MessageAttributes=,
        MessageBody=str_payload        
        # MessageBody=(
        #     'Information about current NY Times fiction bestseller for '
        #     'week of 12/11/2016.'
        # )
    )
    logger.debug("sqs_client.send_message() to SQS Successful\n\n response={0}".format(response))
    logger.info("Sent MessageBody={0}".format(str_payload))
    # print(response['MessageId'])


################################################################################################################
################################################################################################################
#   LAMBDA HANDLER 
################################################################################################################
################################################################################################################
def lambda_handler(event, context):
    logger.info("\n Lambda event={0}\n".format(json.dumps(event)))
    # logger.info("hello0")

    if context == "-": #RUNNING A LOCAL EXECUTION
        object_list = get_matching_s3_keys(S3_BUCKET_NAME, S3_BUCKET_PREFIX, S3_BUCKET_SUFFIX)
        for s3_object in object_list:
            logger.debug(s3_object)
            # time.sleep(3)
            enqueue_object(s3_object, S3_BUCKET_NAME, S3_BUCKET_PREFIX, S3_BUCKET_SUFFIX)
            # continue
            # send_object_to_elasticsearch('')

    else:   #RUNNING A LAMBDA INVOCATION
        logger.info("RUNNING A LAMBDA INVOCATION")

################################################################################################################
# LOCAL TESTING and DEBUGGING  
################################################################################################################
if __name__ == "__main__":
    context = "-"
    # for x in range(0, 300):
    # while True:
    event = {}
    logger.info("\n event={0}\n".format(json.dumps(event)))
    lambda_handler(event, context)

