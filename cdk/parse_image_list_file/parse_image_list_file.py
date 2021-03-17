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
# https://stackoverflow.com/questions/37703634/how-to-import-a-text-file-on-aws-s3-into-pandas-without-writing-to-disk
# https://docs.aws.amazon.com/AmazonS3/latest/dev/notification-content-structure.html
# https://elasticsearch-py.readthedocs.io/en/7.10.0/
# https://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-log-file-validation-digest-file-structure.html
# https://stackoverflow.com/questions/15924632/empty-string-in-elasticsearch-date-field
# https://www.geeksforgeeks.org/python-ways-to-remove-a-key-from-dictionary/
# https://www.geeksforgeeks.org/python-check-if-a-given-object-is-list-or-not/


################################################################################################################
#   Config
################################################################################################################
region = os.environ['AWS_REGION']
QUEUEURL = os.environ['QUEUEURL']
ELASTICSEARCH_HOST = os.environ['ELASTICSEARCH_HOST']
logging_level_name = os.getenv('LOGGING_LEVEL', default = 'INFO')
logging_level = logging._nameToLevel[logging_level_name]
logging.basicConfig(stream=sys.stdout, level=logging_level)
logger = logging.getLogger()
logger.setLevel(logging_level)
sqs_client = boto3.client('sqs')
# s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')
file_name = "image_list_file.txt"
s3_file_key = "images/" + file_name
local_file_path = "/tmp/" + file_name

# Connect to Elasticsearch Service Domain
service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)
elasticsearchclient = Elasticsearch(
    hosts = [{'host': ELASTICSEARCH_HOST, 'port': 443}],
    http_auth = awsauth,
    use_ssl = True,
    verify_certs = True,
    connection_class = RequestsHttpConnection
)
################################################################################################################
################################################################################################################



def confirm_image_file_name(lambda_event):
    event_s3_file_key = lambda_event['Records'][0]['s3']['object']['key']
    if event_s3_file_key == s3_file_key:
        return True
    else:
        return False


def retrieve_s3_file(message):
    ################################################################################################################
    #   Unpack the message from SQS and get bucket name and object name
    ################################################################################################################

    job_data = {}
    try:
        s3_notification_records = message['Records']
    except:
        logger.error("Failed to retrieve \'Records\' from message! ")
        raise Exception("....Skipping message")

    s3_bucket_name = s3_notification_records[0]['s3']['bucket']['name']
    s3_object_key = s3_notification_records[0]['s3']['object']['key']
    s3_object_eTag = s3_notification_records[0]['s3']['object']['eTag']
    s3_object_eventTime = s3_notification_records[0]['eventTime']
    logger.info(s3_bucket_name + "=" + s3_object_key)
    logger.info(s3_object_key + "=" + s3_object_key)
    logger.info(s3_object_eTag + "=" + s3_object_eTag)
    logger.info(s3_object_eventTime + "=" + s3_object_eventTime)

    ################################################################################################################
    #   Get the data from S3  
    ################################################################################################################
    try:
        s3_resource.Bucket(s3_bucket_name).download_file(s3_object_key, local_file_path)
        logger.info("\n S3 File Download: COMPLETE\n")
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            logger.error("The object does not exist.")
        else:
            raise
    
    job_data["S3_BUCKET_NAME"] = s3_bucket_name
    job_data["S3_BUCKET_KEY"] = s3_object_key
    job_data["S3_BUCKET_ETAG"] = s3_object_eTag
    job_data["file_name"] = file_name
    return job_data 


def retrieve_s3_file_list():
    ################################################################################################################
    #   Get the data from S3 in JSON format
    ################################################################################################################
    with open(local_file_path, 'r') as f:
        json_data = json.load(f)

    logger.debug("\n\nDISPLAY JSON FILE CONTENTS")
    logger.debug(json_data)

    return json_data
        

################################################################################################################
#   enqueue a message onto the SQS queue
################################################################################################################
def enqueue_object(job_data):
    str_payload = json.dumps(job_data, indent=4, sort_keys=True, default=str)    
    response = sqs_client.send_message(
        QueueUrl=QUEUEURL,
        DelaySeconds=0,
        # MessageAttributes=,
        MessageBody=str_payload        
        # MessageBody=(
        #     'Information about current NY Times fiction bestseller for '
        #     'week of 12/11/2016.'
        # )
    )
    logger.debug("sqs_client.send_message() to SQS Successful\n\n response={0}".format(response))
    logger.info("Sent MessageBody={0}".format(str_payload))


def send_object_to_elasticsearch(job_data):
    str_payload = json.dumps(job_data, indent=4, sort_keys=True, default=str)
    logger.debug("\n\n\n\n  str_payload = {0}\n\n\n".format(str_payload))
    json_data = job_data
    for key in json_data:
        if json_data[key] == None:
            pass
        elif json_data[key] == "":
            json_data[key] = None
        else:
            json_data[key] = str(json_data[key])

    index_name_prefix = "s3workflow-image-processing"
    unique_job_data = job_data['file_name']
    index_name_caps = index_name_prefix + "-" + unique_job_data 
    index_name = index_name_caps.lower()

    ################################################################################################################
    # Put the record into the Elasticsearch Service Domain
    ################################################################################################################
    try:
        res = elasticsearchclient.index(index=index_name, body=json_data)
        logger.info('res[\'result\']=')
        print(res['result'])
        logger.info('\nSUCCESS: SENDING into the Elasticsearch Service Domain one at a time')
    except Exception as e:
        logger.error('\nFAILED: SENDING into the Elasticsearch Service Domain one at a time\n')
        logger.error(e)
        exit(1)


################################################################################################################
################################################################################################################
#   LAMBDA HANDLER 
################################################################################################################
################################################################################################################
def lambda_handler(event, context):
    logger.info("\n Lambda event={0}\n".format(json.dumps(event)))
    is_image_file = confirm_image_file_name(event)
    if is_image_file:
        job_data = retrieve_s3_file(event)
    else:
        logger.error("The input event does not contain image_list_file.txt")
        exit()
    
    file_list = retrieve_s3_file_list()
    for file_key in file_list: 
        job_data['filelist_key'] = file_key
        enqueue_object(job_data)
        send_object_to_elasticsearch(job_data)

################################################################################################################
################################################################################################################
#   LAMBDA HANDLER 
################################################################################################################
################################################################################################################


################################################################################################################
# LOCAL TESTING and DEBUGGING  
################################################################################################################
if __name__ == "__main__":
    context = "-"
    event = {
    "Records": [
        {
        "eventVersion": "2.0",
        "eventSource": "aws:s3",
        "awsRegion": "us-west-2",
        "eventTime": "2021-03-016T09:00:00.000Z",
        "eventName": "ObjectCreated:Put",
        "userIdentity": {
            "principalId": "EXAMPLE"
        },
        "requestParameters": {
            "sourceIPAddress": "127.0.0.1"
        },
        "responseElements": {
            "x-amz-request-id": "EXAMPLE123456789",
            "x-amz-id-2": "EXAMPLE123/5678abcdefghijklambdaisawesome/mnopqrstuvwxyzABCDEFGH"
        },
        "s3": {
            "s3SchemaVersion": "1.0",
            "configurationId": "testConfigRule",
            "bucket": {
            "name": "s3workflow-imagesbucketd1ef9a17-16f4rwi6wz5w7",
            "ownerIdentity": {
                "principalId": "EXAMPLE"
            },
            "arn": "arn:aws:s3:::example-bucket"
            },
            "object": {
            "key": "images/image_list_file.txt",
            "size": 1024,
            "eTag": "0123456789abcdef0123456789abcdef",
            "sequencer": "0A1B2C3D4E5F678901"
            }
        }
        }
    ]
    }
    lambda_handler(event, context)


            # "key": "images/000000_04:16:32.000000_diagram.png",


