import boto3
from elasticsearch import Elasticsearch, RequestsHttpConnection
from botocore.exceptions import NoCredentialsError
from botocore.exceptions import ClientError
from requests_aws4auth import AWS4Auth
from datetime import datetime
import os 
import pprint
import json 
import time

import oneagent # SDK initialization functions
import oneagent.sdk as onesdk # All other SDK functions.
from oneagent.common import MessagingDestinationType
from oneagent.common import AgentState


################################################################################################################
#   Get messages from queue 
################################################################################################################
def get_message(sqs_client, QUEUEURL):
    no_messages_received = True
    while no_messages_received:
        receive_message_response = sqs_client.receive_message(
            QueueUrl=QUEUEURL,
            MaxNumberOfMessages=1
        )
        if 'Messages' in receive_message_response:
            number_of_messages = len(receive_message_response['Messages'])
            print("\n received {0} messages!! ....Processing message \n".format(number_of_messages))
            no_messages_received = False
        else:
            print("\n received 0 messages!! waiting.....2 seconds before retrying \n")
            time.sleep(2)

    message_body=json.loads(receive_message_response['Messages'][0]['Body'])
    print("message_body = {0} \n".format(message_body))
    # bucketname = message_body['bucketname']
    # objectkey = message_body['s3_file_name']

    ReceiptHandle = receive_message_response['Messages'][0]['ReceiptHandle']
    delete_message_response = sqs_client.delete_message(
    QueueUrl=QUEUEURL,
    ReceiptHandle=ReceiptHandle
    )
    print("delete_message_response = {0}".format(delete_message_response))

    return message_body


def detect_text(rekognition_client, message):
    response = rekognition_client.detect_text(
        Image={
            # 'Bytes': b'bytes',
            'S3Object': {
                'Bucket': message['S3_BUCKET_NAME'],
                'Name': message['filelist_key']
                # 'Version': 'string'
            }
        }
        # Filters={
        #     'WordFilter': {
        #         'MinConfidence': ...,
        #         'MinBoundingBoxHeight': ...,
        #         'MinBoundingBoxWidth': ...
        #     },
        #     'RegionsOfInterest': [
        #         {
        #             'BoundingBox': {
        #                 'Width': ...,
        #                 'Height': ...,
        #                 'Left': ...,
        #                 'Top': ...
        #             }
        #         },
        #     ]
        # }
    )
    print("response=" + str(response))
    return response

def send_message(sqs_client, message, COMPREHEND_QUEUE):
    str_payload = json.dumps(message, indent=4, sort_keys=True, default=str)    
    response = sqs_client.send_message(
        QueueUrl=COMPREHEND_QUEUE,
        DelaySeconds=0,
        # MessageAttributes=,
        MessageBody=str_payload        
        # MessageBody=(
        #     'Information about current NY Times fiction bestseller for '
        #     'week of 12/11/2016.'
        # )
    )
    print("sqs_client.send_message() to SQS Successful\n\n response={0}".format(response))
    print("Sent MessageBody={0}".format(str_payload))


def put_document(elasticsearchclient, job_data):
    str_payload = json.dumps(job_data, indent=4, sort_keys=True, default=str)
    print("\n\n\n\n  str_payload = {0}\n\n\n".format(str_payload))
    json_data = job_data
    for key in json_data:
        if json_data[key] == None:
            pass
        elif json_data[key] == "":
            json_data[key] = None
        else:
            json_data[key] = str(json_data[key])
    index_name_prefix = "s3workflow-rekognition"
    unique_job_data = job_data['s3_file_key_for_es'] + "-" + job_data['S3_BUCKET_ETAG']
    index_name_caps = index_name_prefix + "-" + unique_job_data 
    index_name = index_name_caps.lower()

    ################################################################################################################
    # Put the record into the Elasticsearch Service Domain
    ################################################################################################################
    try:
        res = elasticsearchclient.index(index=index_name, body=json_data)
        print('res[\'result\']=')
        print(res['result'])
        print('\nSUCCESS: SENDING into the Elasticsearch Service Domain one at a time')
    except Exception as e:
        print('\nFAILED: SENDING into the Elasticsearch Service Domain one at a time\n')
        print(e)
        exit(1)


################################################################################################################
#   Debug Dynatrace SDK errors
################################################################################################################
def _diag_callback(unicode_message):
	print(unicode_message)


################################################################################################################
#   Main function 
################################################################################################################
if __name__ == '__main__':

    try:
        ################################################################################################################
        #   Setup Dynatrace Tracing
        ################################################################################################################
        init_result = oneagent.initialize()
        # if not oneagent.initialize():
        if not init_result:
            print('Error initializing OneAgent SDK.')
        if init_result:
            print('SDK should work (but agent might be inactive).')
            print('OneAgent SDK initialization result: ' + repr(init_result))
        else:
            print('SDK will definitely not work (i.e. functions will be no-ops):', init_result)
        sdk = oneagent.get_sdk()
        if sdk.agent_state not in (AgentState.ACTIVE, AgentState.TEMPORARILY_INACTIVE):
            print('Dynatrace SDK agent is NOT Active, you will not see data from this process.')
        sdk.set_diagnostic_callback(_diag_callback)
        print('It may take a few moments before the path appears in the UI.')

        ################################################################################################################
        # Get and Print the list of user's environment variables 
        env_var = os.environ 
        print("\n User's Environment variables:") 
        pprint.pprint(dict(env_var), width = 1) 
        ################################################################################################################

        ################################################################################################################
        sqs_client = boto3.client('sqs')
        region = os.environ['AWS_REGION']
        rekognition_client = boto3.client('rekognition')
        REKOGNITION_QUEUE = env_var['REKOGNITION_QUEUE']
        COMPREHEND_QUEUE = env_var['COMPREHEND_QUEUE']
        ELASTICSEARCH_HOST = env_var['ELASTICSEARCH_HOST']
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
        sdk.add_custom_request_attribute('project', 's3workflow')
        sdk.add_custom_request_attribute('Container', 'rekognitioncontainer')

        while True:
            with sdk.trace_custom_service('get_message()', 'SQS'):
                message = get_message(sqs_client, REKOGNITION_QUEUE)

            with sdk.trace_custom_service('detect_text()', 'REKOGNITION'):
                detect_text_response = detect_text(rekognition_client, message)

            message['detect_text_response'] = detect_text_response
            with sdk.trace_custom_service('send_message()', 'SQS'):
                send_message(sqs_client, message, COMPREHEND_QUEUE)

            with sdk.trace_custom_service('put_document()', 'ELASTICSEARCH'):
                put_document(elasticsearchclient, message)
    finally:
        shutdown_error = oneagent.shutdown()
        if shutdown_error:
            print('Error shutting down SDK:', shutdown_error)

