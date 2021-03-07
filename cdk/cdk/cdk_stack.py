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
# https://?.com


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
            actions=["s3:Get*","s3:Head*","s3:List*","firehose:*","es:*"],
            resources=["*"]
            )

        parse_image_list_file.add_to_role_policy(lambda_supplemental_policy_statement)
        list_objects.add_to_role_policy(lambda_supplemental_policy_statement)


        ###########################################################################
        # AWS SNS TOPICS 
        ###########################################################################
        # notification_topic = aws_sns.Topic(self, "notification_topic")


        ###########################################################################
        # ADD AMAZON S3 BUCKET NOTIFICATIONS
        ###########################################################################
        images_bucket.add_event_notification(aws_s3.EventType.OBJECT_CREATED, aws_s3_notifications.LambdaDestination(parse_image_list_file))


        ###########################################################################
        # AWS SQS QUEUES
        ###########################################################################
        comprehend_queue_iqueue = aws_sqs.Queue(self, "comprehend_queue_iqueue")
        comprehend_queue_iqueue_dlq = aws_sqs.DeadLetterQueue(max_receive_count=10, queue=comprehend_queue_iqueue)
        comprehend_queue = aws_sqs.Queue(self, "comprehend_queue", visibility_timeout=core.Duration.seconds(301), dead_letter_queue=comprehend_queue_iqueue_dlq)

        rekognition_queue_iqueue = aws_sqs.Queue(self, "rekognition_queue_iqueue")
        rekognition_queue_dlq = aws_sqs.DeadLetterQueue(max_receive_count=10, queue=rekognition_queue_iqueue)
        rekognition_queue = aws_sqs.Queue(self, "rekognition_queue", visibility_timeout=core.Duration.seconds(301), dead_letter_queue=rekognition_queue_dlq)

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
        # AMAZON VPC  
        ###########################################################################
        vpc = aws_ec2.Vpc(self, "s3workflowVPC", max_azs=3)     # default is all AZs in region


        ###########################################################################
        # AMAZON ECS CLUSTER 
        ###########################################################################
        cluster = aws_ecs.Cluster(self, "s3", vpc=vpc)


        ###########################################################################
        # AMAZON ECS Repositories  
        ###########################################################################
        rekognition_repository = aws_ecr.Repository(self, "rekognition_repository", image_scan_on_push=True, removal_policy=core.RemovalPolicy("DESTROY") )
        put_repository = aws_ecr.Repository(self, "put_repository", image_scan_on_push=True, removal_policy=core.RemovalPolicy("DESTROY") )


        ###########################################################################
        # AMAZON ECS Roles and Policies
        ###########################################################################        
        task_execution_policy_statement = aws_iam.PolicyStatement(
            effect=aws_iam.Effect.ALLOW,
            actions=["logs:*", "ecs:*", "ec2:*", "elasticloadbalancing:*","ecr:*"],
            resources=["*"]
            )
        task_execution_policy_document = aws_iam.PolicyDocument()
        task_execution_policy_document.add_statements(task_execution_policy_statement)
        task_execution_policy = aws_iam.Policy(self, "task_execution_policy", document=task_execution_policy_document)
        task_execution_role = aws_iam.Role(self, "task_execution_role", assumed_by=aws_iam.ServicePrincipal('ecs-tasks.amazonaws.com') )
        task_execution_role.attach_inline_policy(task_execution_policy)

        task_policy_statement = aws_iam.PolicyStatement(
            effect=aws_iam.Effect.ALLOW,
            actions=["logs:*", "xray:*", "sqs:*", "s3:*", "rekognition:*", "comprehend:*", "es:*"],
            resources=["*"]
            )
        task_policy_document = aws_iam.PolicyDocument()
        task_policy_document.add_statements(task_policy_statement)
        task_policy = aws_iam.Policy(self, "task_policy", document=task_policy_document)
        task_role = aws_iam.Role(self, "task_role", assumed_by=aws_iam.ServicePrincipal('ecs-tasks.amazonaws.com') )
        task_role.attach_inline_policy(task_policy)


        ###########################################################################
        # AMAZON ECS Task definitions
        ###########################################################################
        rekognition_task_definition = aws_ecs.TaskDefinition(self, "rekognition_task_definition",
                                                                        compatibility=aws_ecs.Compatibility("FARGATE"), 
                                                                        cpu="1024", 
                                                                        # ipc_mode=None, 
                                                                        memory_mib="2048", 
                                                                        network_mode=aws_ecs.NetworkMode("AWS_VPC"), 
                                                                        # pid_mode=None,                                      #Not supported in Fargate and Windows containers
                                                                        # placement_constraints=None, 
                                                                        execution_role=task_execution_role, 
                                                                        # family=None, 
                                                                        # proxy_configuration=None, 
                                                                        task_role=task_role
                                                                        # volumes=None
                                                                        )

        comprehend_task_definition = aws_ecs.TaskDefinition(self, "comprehend_task_definition",
                                                                        compatibility=aws_ecs.Compatibility("FARGATE"), 
                                                                        cpu="1024", 
                                                                        # ipc_mode=None, 
                                                                        memory_mib="2048", 
                                                                        network_mode=aws_ecs.NetworkMode("AWS_VPC"), 
                                                                        # pid_mode=None,                                      #Not supported in Fargate and Windows containers
                                                                        # placement_constraints=None, 
                                                                        execution_role=task_execution_role, 
                                                                        # family=None, 
                                                                        # proxy_configuration=None, 
                                                                        task_role=task_role
                                                                        # volumes=None
                                                                        )


        ###########################################################################
        # AMAZON ECS Images 
        ###########################################################################
        rekognition_ecr_image = aws_ecs.EcrImage(repository=rekognition_repository, tag="latest")
        comprehend_ecr_image = aws_ecs.EcrImage(repository=put_repository, tag="latest")


        ###########################################################################
        # ENVIRONMENT VARIABLES 
        ###########################################################################
        environment_variables = {}
        environment_variables["COMPREHEND_QUEUE"] = comprehend_queue.queue_url
        environment_variables["REKOGNITION_QUEUE"] = rekognition_queue.queue_url
        environment_variables["IMAGES_BUCKET"] = images_bucket.bucket_name
        environment_variables["ELASTICSEARCH_HOST"] = s3workflow_domain.domain_endpoint
        
        parse_image_list_file.add_environment("ELASTICSEARCH_HOST", s3workflow_domain.domain_endpoint )
        parse_image_list_file.add_environment("QUEUEURL", rekognition_queue.queue_url )
        parse_image_list_file.add_environment("DEBUG", "False" )
        parse_image_list_file.add_environment("BUCKET", "-" )
        parse_image_list_file.add_environment("KEY", "-" )

        get_size_and_store.add_environment("QUEUEURL", object_queue.queue_url)
        get_size_and_store.add_environment("ELASTICSEARCH_HOST", s3workflow_domain.domain_endpoint)


        ###########################################################################
        # ECS Log Drivers 
        ###########################################################################
        rekognition_task_log_driver = aws_ecs.LogDriver.aws_logs(stream_prefix="s3workflow", log_retention=aws_logs.RetentionDays("ONE_DAY"))
        comprehend_task_log_driver = aws_ecs.LogDriver.aws_logs(stream_prefix="s3workflow", log_retention=aws_logs.RetentionDays("ONE_DAY"))


        ###########################################################################
        # ECS Task Definitions 
        ###########################################################################
        rekognition_task_definition.add_container("rekognition_task_definition", 
                                                    image=rekognition_ecr_image, 
                                                    memory_reservation_mib=1024,
                                                    environment=environment_variables,
                                                    logging=rekognition_task_log_driver
                                                    )

        comprehend_task_definition.add_container("comprehend_task_definition", 
                                                    image=comprehend_ecr_image, 
                                                    memory_reservation_mib=1024,
                                                    environment=environment_variables,
                                                    logging=comprehend_task_log_driver
                                                    )


        ###########################################################################
        # AWS ROUTE53 HOSTED ZONE 
        ###########################################################################
        hosted_zone = aws_route53.HostedZone(self, "hosted_zone", zone_name="s3workflow.com" ,comment="private hosted zone for s3workflow system")
        hosted_zone.add_vpc(vpc)
        # images_bucket_record_values = [images_bucket.bucket_name]
        # queue_record_values = [ecs_task_queue_queue.queue_url]
        # images_bucket_record_name = "imagesbucket." + hosted_zone.zone_name
        # queue_record_name = "filesqueue." + hosted_zone.zone_name
        # hosted_zone_record_bucket = aws_route53.TxtRecord(self, "hosted_zone_record_bucket", record_name=images_bucket_record_name, values=images_bucket_record_values, zone=hosted_zone, comment="dns record for images bucket name")
        # hosted_zone_record_queue = aws_route53.TxtRecord(self, "hosted_zone_record_queue", record_name=queue_record_name, values=queue_record_values, zone=hosted_zone, comment="dns record for queue name")


