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
import sys
import io
import csv
import os
import gzip


################################################################################################################
#   References
################################################################################################################
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
S3_BUCKET_NAME = os.environ['S3_BUCKET_NAME']
S3_BUCKET_PREFIX = os.environ['S3_BUCKET_PREFIX']
S3_BUCKET_SUFFIX = os.environ['S3_BUCKET_SUFFIX']
ELASTICSEARCH_HOST = os.environ['ELASTICSEARCH_HOST']
ELASTICSEARCH_HOST = os.environ['ELASTICSEARCH_HOST']
logging_level_name = os.getenv('LOGGING_LEVEL', default = 'INFO')
logging_level = logging._nameToLevel[logging_level_name]
logging.basicConfig(stream=sys.stdout, level=logging_level)
logger = logging.getLogger()
logger.setLevel(logging_level)
# SQS_ReceiptHandle = {}

sqs_client = boto3.client('sqs')
s3_client = boto3.client('s3')

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


def get_sqs_message(QUEUEURL, sqs_client):
    ###### Example of string data that was sent:#########
    # MessageBody={
    #     "ETag": "\"51ec3079c61643d5e3e1b0b390141caf\"",
    #     "Key": "images/000030_21:02:25.000030_diagram.png",
    #     "LastModified": "2021-03-07 06:34:53+00:00",
    #     "Size": 98831,
    #     "StorageClass": "STANDARD"
    # }
    ################################################

    receive_message_response = dict()
    receive_message_response = sqs_client.receive_message(
        QueueUrl=QUEUEURL,
        # AttributeNames=[
        #     'All'|'Policy'|'VisibilityTimeout'|'MaximumMessageSize'|'MessageRetentionPeriod'|'ApproximateNumberOfMessages'|'ApproximateNumberOfMessagesNotVisible'|'CreatedTimestamp'|'LastModifiedTimestamp'|'QueueArn'|'ApproximateNumberOfMessagesDelayed'|'DelaySeconds'|'ReceiveMessageWaitTimeSeconds'|'RedrivePolicy'|'FifoQueue'|'ContentBasedDeduplication'|'KmsMasterKeyId'|'KmsDataKeyReusePeriodSeconds',
        # ],
        # MessageAttributeNames=[
        #     'string',
        # ],
        MaxNumberOfMessages=10
        # VisibilityTimeout=123,
        # WaitTimeSeconds=123,
        # ReceiveRequestAttemptId='string'
    )
    if 'Messages' in receive_message_response:
        number_of_messages = len(receive_message_response['Messages'])
        logger.info("\n received {0} messages!! ....Processing messages \n".format(number_of_messages))
        SQS_ReceiptHandle = receive_message_response['Messages'][0]['ReceiptHandle']
        return receive_message_response['Messages']
    else:
        exit()

# def delete_messages():
#         # ReceiptHandle = receive_message_response['Messages'][0]['ReceiptHandle']
#         delete_message_response = sqs_client.delete_message(QueueUrl=QUEUEURL, ReceiptHandle=SQS_ReceiptHandle)
#         logger.info("delete_message_response = {0}".format(delete_message_response))
#         # return receive_message_response
#         return delete_message_response



def convert_to_TimeForElasticsearch(time_from_S3):
    # Have: 2021-03-07 06:34:46+00:00
    # Need: 2018-04-23T10:45:13.899Z
    # Got:
    # 2020-Nov-23T07:43:07Z
    # 2021-Jan-1T06:23:08Z


    # 2021-03-07 06:34:46+00:00
    date_and_time_list = time_from_S3.split(' ')# [2021-03-07, 06:34:46+00:00]
    date = date_and_time_list[0]                # 2021-03-07
    date_list = date.split('-')                 # [2021, 03, 07]
    
    time = date_and_time_list[1]               # 06:34:46+00:00

    hms_and_offset_list = time.split('+')       # [06:34:46, 00:00]
    hms = hms_and_offset_list[0]                # 06:34:46  
    hms_list = hms.split(':')                   # [06, 34, 46]  
    offset = hms_and_offset_list[1]             # 00:00


    # Assign varibles to create TimeForElasticsearch
    year = date_list[0]
    month = date_list[1]
    day = date_list[2]

    hour = hms_list[0]
    minutes = hms_list[1]
    seconds = hms_list[2]


    # # convert month name to month number
    # datetime_object = datetime.datetime.strptime(month, "%b")
    # month_number = datetime_object.month
    # month_number_string = str(month_number)

    # Checking for single digit month or day or month
    if len(day) == 1:
        day = "0" + day
    if len(month) == 1:
        month = "0" + month

    logger.info(year)
    logger.info(month)
    logger.info(day)
    logger.info(hour)
    logger.info(minutes)
    logger.info(seconds)

    newtime = str( year + '-' + month + '-' + day + 'T' + hour + ':' + minutes + ':' + seconds + 'Z' )
    logger.info('newtime=' + newtime)

    return newtime



    ################################################################################################################
    # Python program to convert a list to string 
    ################################################################################################################
def listToString(s):  
    # initialize an empty string 
    str1 = ""  
    
    # traverse in the string   
    for ele in s:  
        str1 += ele   
        str1 += ", "   
    
    # return string   
    return str1  
        


def send_object_to_elasticsearch(Message):
    ################################################################################################################
    #   for each object, Put records into the Elasticsearch cluster
    ################################################################################################################    
    ###### Example of string data from SQS:#########
    # Message['Body']='{\n    
    # "ETag": "\\"51ec3079c61643d5e3e1b0b390141caf\\"",\n    
    # "Key": "images/000001_05:08:44.000001_diagram.png",\n    
    # "LastModified": "2021-03-07 06:34:46+00:00",\n    
    # "Size": 98831,\n    
    # "StorageClass": "STANDARD",\n    
    # "bucket_name": "s3workflow-imagesbucketd1ef9a17-16f4rwi6wz5w7",\n    
    # "bucket_prefix": "images/",\n    
    # "bucket_suffix": ""
    # \n}'
    ################################################
    Body = Message['Body']
    json_Body = json.loads(Body)
    bucket_name = json_Body['bucket_name']
    bucket_prefix = json_Body['bucket_prefix']
    key = json_Body['Key']
    # index_name = bucket_name
    json_data = json_Body

    # Have: 2021-03-07 06:34:46+00:00
    # Need: 2018-04-23T10:45:13.899Z
    # Converting Time for Elasticsearch
    json_data["TimeForElasticsearch"] = convert_to_TimeForElasticsearch(json_Body['LastModified'])

    bucket_prefix_clean = bucket_prefix.replace('/', '')
    workflow = "list-objects"
    index_name_caps = bucket_name + "-" + workflow + "-" + bucket_prefix_clean
    index_name = index_name_caps.lower()

    logger.debug("\n\n\n\n  json_data = {0}\n\n\n".format(json_data))

    ################################################################################################################
    #   for each object, set the correct data type in the dictionary 
    ################################################################################################################
    for key in json_data:
        logger.debug("\n(Starting) key = {0}".format(key))
        logger.debug("\n(Starting) value = {0}".format(json_data[key]))
        logger.debug("\n(Starting) type(value) = {0}\n".format( type(json_data[key]) ))

        if json_data[key] == None:
            pass
        elif json_data[key] == "":
            json_data[key] = None
        else:
            json_data[key] = str(json_data[key])

        if key == "Size":
            json_data[key] = int(json_data[key])

        logger.debug("\n(Final) key = {0}".format(key))
        logger.debug("\n(Final) value = {0}".format(json_data[key]))
        logger.debug("\n(Final) type(value) = {0}\n".format( type(json_data[key]) ))



    ################################################################################################################
    # Put the record into the Elasticsearch Service Domain
    ################################################################################################################
    try:
        res = elasticsearchclient.index(index=index_name, body=json_data)
        logger.debug('res[\'result\']=')
        logger.debug(res['result'])
        logger.debug('\nSUCCESS: SENDING into the Elasticsearch Service Domain one at a time')
    except Exception as e:
        logger.debug('\nFAILED: SENDING into the Elasticsearch Service Domain one at a time\n')
        logger.debug(e)
        exit(1)

    logger.info('COMPLETED: Putting 1 record into the Elasticsearch Service Domain one at a time' )

################################################################################################################
################################################################################################################
#   LAMBDA HANDLER 
################################################################################################################
################################################################################################################
def lambda_handler(event, context):
    logger.debug("\n Lambda event={0}\n".format(json.dumps(event)))

    if context == "-": #RUNNING A LOCAL EXECUTION 
        number_of_messages_in_event = len(event)
        message_number = 1
        for Message in event:
            logger.info("processing message {} of {}".format(message_number, number_of_messages_in_event))
            send_object_to_elasticsearch(Message)
            message_number += 1
        # delete_messages()

    else:   #RUNNING A LAMBDA INVOCATION
        number_of_records_in_event = len(event['Records'])
        record_number = 1            
        for Record in event['Records']:
            logger.info("processing record {} of {}".format(record_number, number_of_records_in_event))
            send_object_to_elasticsearch(Record)
            record_number += 1


################################################################################################################
# LOCAL TESTING and DEBUGGING  
################################################################################################################
if __name__ == "__main__":
    context = "-"
    event = get_sqs_message(QUEUEURL, sqs_client)
    logger.debug("\n event={0}\n".format(json.dumps(event)))
    lambda_handler(event,context)

