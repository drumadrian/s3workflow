import aws_cdk.aws_elasticloadbalancingv2 as aws_elasticloadbalancingv2
import aws_cdk.aws_sns_subscriptions as aws_sns_subscriptions
import aws_cdk.aws_s3_notifications as aws_s3_notifications
from aws_cdk.aws_lambda_event_sources import SqsEventSource
import aws_cdk.aws_secretsmanager as aws_secretsmanager
import aws_cdk.aws_elasticsearch as aws_elasticsearch
import aws_cdk.aws_cloudtrail as aws_cloudtrail
import aws_cdk.aws_route53 as aws_route53
import aws_cdk.aws_cognito as aws_cognito
import aws_cdk.aws_lambda as aws_lambda
import aws_cdk.aws_logs as aws_logs
import aws_cdk.aws_sns as aws_sns
import aws_cdk.aws_iam as aws_iam
import aws_cdk.aws_sqs as aws_sqs
import aws_cdk.aws_ec2 as aws_ec2
import aws_cdk.aws_ecr as aws_ecr
import aws_cdk.aws_ecs as aws_ecs
import aws_cdk.aws_s3 as aws_s3
import inspect as inspect
from aws_cdk import core


###########################################################################
# References 
###########################################################################
# https://github.com/drumadrian/s3workflow/


class CdkStack(core.Stack):

    def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)


        ###########################################################################
        # AWS LAMBDA FUNCTIONS 
        ###########################################################################
        parse_image_list_file = aws_lambda.Function(self,'parse_image_list_file',
        handler='parse_image_list_file.lambda_handler',
        runtime=aws_lambda.Runtime.PYTHON_3_7,
        code=aws_lambda.Code.asset('parse_image_list_file'),
        memory_size=4096,
        timeout=core.Duration.seconds(300),
        log_retention=aws_logs.RetentionDays.ONE_DAY
        )

        list_objects = aws_lambda.Function(self,'list_objects',
        handler='list_objects.lambda_handler',
        runtime=aws_lambda.Runtime.PYTHON_3_7,
        code=aws_lambda.Code.asset('list_objects'),
        memory_size=4096,
        timeout=core.Duration.seconds(300),
        log_retention=aws_logs.RetentionDays.ONE_DAY
        )

        get_size_and_store = aws_lambda.Function(self,'get_size_and_store',
        handler='get_size_and_store.lambda_handler',
        runtime=aws_lambda.Runtime.PYTHON_3_7,
        code=aws_lambda.Code.asset('get_size_and_store'),
        memory_size=4096,
        timeout=core.Duration.seconds(300),
        log_retention=aws_logs.RetentionDays.ONE_DAY
        )


        ###########################################################################
        # AMAZON S3 BUCKETS 
        ###########################################################################
        images_bucket = aws_s3.Bucket(self, "images_bucket")


        ###########################################################################
        # LAMBDA SUPPLEMENTAL POLICIES 
        ###########################################################################
        lambda_supplemental_policy_statement = aws_iam.PolicyStatement(
            effect=aws_iam.Effect.ALLOW,
            actions=["s3:Get*","s3:Head*","s3:List*","sqs:*","es:*"],
            resources=["*"]
            )

        list_objects.add_to_role_policy(lambda_supplemental_policy_statement)
        get_size_and_store.add_to_role_policy(lambda_supplemental_policy_statement)


        ###########################################################################
        # AWS SQS QUEUES
        ###########################################################################
        object_queue_iqueue = aws_sqs.Queue(self, "object_queue_iqueue")
        object_queue_dlq = aws_sqs.DeadLetterQueue(max_receive_count=10, queue=object_queue_iqueue)
        object_queue = aws_sqs.Queue(self, "object_queue", visibility_timeout=core.Duration.seconds(301), dead_letter_queue=object_queue_dlq)

        
        ###########################################################################
        # AWS LAMBDA SQS EVENT SOURCE
        ###########################################################################
        get_size_and_store.add_event_source(SqsEventSource(object_queue,batch_size=10))


        ###########################################################################
        # AWS ELASTICSEARCH DOMAIN
        ###########################################################################
        s3workflow_domain = aws_elasticsearch.Domain(self, "s3workflow_domain",
            version=aws_elasticsearch.ElasticsearchVersion.V7_1,
            capacity={
                "master_nodes": 3,
                "data_nodes": 4
            },
            ebs={
                "volume_size": 100
            },
            zone_awareness={
                "availability_zone_count": 2
            },
            logging={
                "slow_search_log_enabled": True,
                "app_log_enabled": True,
                "slow_index_log_enabled": True
            }
        )


        ###########################################################################
        # AMAZON COGNITO USER POOL
        ###########################################################################
        s3workflow_pool = aws_cognito.UserPool(self, "s3workflow-pool",
                                                            account_recovery=None, 
                                                            auto_verify=None, 
                                                            custom_attributes=None, 
                                                            email_settings=None, 
                                                            enable_sms_role=None, 
                                                            lambda_triggers=None, 
                                                            mfa=None, 
                                                            mfa_second_factor=None, 
                                                            password_policy=None, 
                                                            self_sign_up_enabled=None, 
                                                            sign_in_aliases=aws_cognito.SignInAliases(email=True, phone=None, preferred_username=None, username=True), 
                                                            sign_in_case_sensitive=None, 
                                                            sms_role=None, 
                                                            sms_role_external_id=None, 
                                                            standard_attributes=None, 
                                                            user_invitation=None, 
                                                            user_pool_name=None, 
                                                            user_verification=None
                                                            )


        ###########################################################################
        # ENVIRONMENT VARIABLES 
        ###########################################################################
        list_objects.add_environment("QUEUEURL", object_queue.queue_url)
        list_objects.add_environment("ELASTICSEARCH_HOST", s3workflow_domain.domain_endpoint)
        list_objects.add_environment("S3_BUCKET_NAME", images_bucket.bucket_name)
        list_objects.add_environment("S3_BUCKET_PREFIX", "images/")
        list_objects.add_environment("S3_BUCKET_SUFFIX", "")
        list_objects.add_environment("LOGGING_LEVEL", "INFO")

        get_size_and_store.add_environment("QUEUEURL", object_queue.queue_url)
        get_size_and_store.add_environment("ELASTICSEARCH_HOST", s3workflow_domain.domain_endpoint)
        get_size_and_store.add_environment("S3_BUCKET_NAME", images_bucket.bucket_name)
        get_size_and_store.add_environment("S3_BUCKET_PREFIX", "images/")
        get_size_and_store.add_environment("S3_BUCKET_SUFFIX", "")
        get_size_and_store.add_environment("LOGGING_LEVEL", "INFO")

