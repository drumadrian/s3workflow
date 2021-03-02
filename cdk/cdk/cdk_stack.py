from aws_cdk import core
import aws_cdk.aws_secretsmanager as aws_secretsmanager
# import aws_cdk.aws_cloudformation as aws_cloudformation
import aws_cdk.aws_lambda as aws_lambda
import aws_cdk.aws_iam as aws_iam
import aws_cdk.aws_s3_notifications as aws_s3_notifications
import aws_cdk.aws_s3 as aws_s3
import aws_cdk.aws_sns as aws_sns
import aws_cdk.aws_sns_subscriptions as aws_sns_subscriptions
import aws_cdk.aws_sqs as aws_sqs
from aws_cdk.aws_lambda_event_sources import SqsEventSource
import aws_cdk.aws_elasticsearch as aws_elasticsearch
import aws_cdk.aws_cognito as aws_cognito
import aws_cdk.aws_elasticloadbalancingv2 as aws_elasticloadbalancingv2
import aws_cdk.aws_ec2 as aws_ec2
import aws_cdk.aws_logs as logs
import aws_cdk.aws_cloudtrail as aws_cloudtrail
import inspect as inspect

import aws_cdk.aws_ecr as aws_ecr
import aws_cdk.aws_ecs as aws_ecs
import aws_cdk.aws_route53 as aws_route53


###########################################################################
# References 
###########################################################################
# https://github.com/aws/aws-cdk/issues/7236



class CdkStack(core.Stack):

    def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # The code that defines your stack goes here

        ###########################################################################
        # AWS SECRETS MANAGER - Templated secret 
        ###########################################################################
        # templated_secret = aws_secretsmanager.Secret(self, "TemplatedSecret",
        #     generate_secret_string=aws_secretsmanager.SecretStringGenerator(
        #         secret_string_template= "{\"username\":\"cleanbox\"}",
        #         generate_string_key="password"
        #     )
        # )
        ###########################################################################
        # CUSTOM CLOUDFORMATION RESOURCE 
        ###########################################################################
        # customlambda = aws_lambda.Function(self,'customconfig',
        # handler='customconfig.on_event',
        # runtime=aws_lambda.Runtime.PYTHON_3_7,
        # code=aws_lambda.Code.asset('customconfig'),
        # )

        # customlambda_statement = aws_iam.PolicyStatement(actions=["events:PutRule"], conditions=None, effect=None, not_actions=None, not_principals=None, not_resources=None, principals=None, resources=["*"], sid=None)
        # customlambda.add_to_role_policy(statement=customlambda_statement)

        # my_provider = cr.Provider(self, "MyProvider",
        #     on_event_handler=customlambda,
        #     # is_complete_handler=is_complete, # optional async "waiter"
        #     log_retention=logs.RetentionDays.SIX_MONTHS
        # )

        # CustomResource(self, 'customconfigresource', service_token=my_provider.service_token)


        ###########################################################################
        # AWS LAMBDA FUNCTIONS 
        ###########################################################################
        parse_image_list_file = aws_lambda.Function(self,'parse_image_list_file',
        handler='parse_image_list_file.lambda_handler',
        runtime=aws_lambda.Runtime.PYTHON_3_7,
        code=aws_lambda.Code.asset('parse_image_list_file'),
        memory_size=4096,
        timeout=core.Duration.seconds(300),
        log_retention=logs.RetentionDays.ONE_DAY
        )

        list_objects = aws_lambda.Function(self,'list_objects',
        handler='list_objects.lambda_handler',
        runtime=aws_lambda.Runtime.PYTHON_3_7,
        code=aws_lambda.Code.asset('list_objects'),
        memory_size=4096,
        timeout=core.Duration.seconds(300),
        log_retention=logs.RetentionDays.ONE_DAY
        )

        get_size_and_store = aws_lambda.Function(self,'get_size_and_store',
        handler='get_size_and_store.lambda_handler',
        runtime=aws_lambda.Runtime.PYTHON_3_7,
        code=aws_lambda.Code.asset('get_size_and_store'),
        memory_size=4096,
        timeout=core.Duration.seconds(300),
        log_retention=logs.RetentionDays.ONE_DAY
        )


        ###########################################################################
        # AMAZON S3 BUCKETS 
        ###########################################################################
        image_bucket = aws_s3.Bucket(self, "image_bucket")


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
        # cloudtrail_log_topic = aws_sns.Topic(self, "cloudtrail_log_topic")

        ###########################################################################
        # ADD AMAZON S3 BUCKET NOTIFICATIONS
        ###########################################################################
        image_bucket.add_event_notification(aws_s3.EventType.OBJECT_CREATED, aws_s3_notifications.LambdaDestination(parse_image_list_file))


        ###########################################################################
        # AWS SQS QUEUES
        ###########################################################################
        images_queue_iqueue = aws_sqs.Queue(self, "images_queue_iqueue")
        images_queue_dlq = aws_sqs.DeadLetterQueue(max_receive_count=10, queue=images_queue_iqueue)
        images_queue = aws_sqs.Queue(self, "images_queue", visibility_timeout=core.Duration.seconds(301), dead_letter_queue=images_queue_dlq)


        recognition_queue_iqueue = aws_sqs.Queue(self, "recognition_queue_iqueue")
        recognition_queue_dlq = aws_sqs.DeadLetterQueue(max_receive_count=10, queue=recognition_queue_iqueue)
        recognition_queue = aws_sqs.Queue(self, "recognition_queue", visibility_timeout=core.Duration.seconds(301), dead_letter_queue=recognition_queue_dlq)


        object_queue_iqueue = aws_sqs.Queue(self, "object_queue_iqueue")
        object_queue_dlq = aws_sqs.DeadLetterQueue(max_receive_count=10, queue=object_queue_iqueue)
        object_queue = aws_sqs.Queue(self, "object_queue", visibility_timeout=core.Duration.seconds(301), dead_letter_queue=object_queue_dlq)


        # sqs_to_elastic_cloud_queue_iqueue = aws_sqs.Queue(self, "sqs_to_elastic_cloud_queue_dlq")
        # sqs_to_elastic_cloud_queue_dlq = aws_sqs.DeadLetterQueue(max_receive_count=10, queue=sqs_to_elastic_cloud_queue_iqueue)
        # sqs_to_elastic_cloud_queue = aws_sqs.Queue(self, "sqs_to_elastic_cloud_queue", visibility_timeout=core.Duration.seconds(300), dead_letter_queue=sqs_to_elastic_cloud_queue_dlq)


        ###########################################################################
        # AWS SNS TOPIC SUBSCRIPTIONS
        ###########################################################################
        # cloudtrail_log_topic.add_subscription(aws_sns_subscriptions.SqsSubscription(sqs_to_elastic_cloud_queue))
        # cloudtrail_log_topic.add_subscription(aws_sns_subscriptions.SqsSubscription(sqs_to_elasticsearch_service_queue))

        
        ###########################################################################
        # AWS LAMBDA SQS EVENT SOURCE
        ###########################################################################
        get_size_and_store.add_event_source(SqsEventSource(object_queue,batch_size=10))
        # sqs_to_elasticsearch_service.add_event_source(SqsEventSource(sqs_to_elasticsearch_service_queue,batch_size=10))


        ###########################################################################
        # AWS ELASTICSEARCH DOMAIN
        ###########################################################################

        ###########################################################################
        # AWS ELASTICSEARCH DOMAIN ACCESS POLICY 
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


        parse_image_list_file.add_environment("ELASTICSEARCH_HOST", s3workflow_domain.domain_endpoint )
        parse_image_list_file.add_environment("QUEUEURL", images_queue.queue_url )
        parse_image_list_file.add_environment("DEBUG", "False" )
        parse_image_list_file.add_environment("BUCKET", "-" )
        parse_image_list_file.add_environment("KEY", "-" )

        get_size_and_store.add_environment("QUEUEURL", object_queue.queue_url)
        get_size_and_store.add_environment("ELASTICSEARCH_HOST", s3workflow_domain.domain_endpoint)
        # sqs_to_elastic_cloud.add_environment("ELASTIC_CLOUD_PASSWORD", "-")
        # sqs_to_elastic_cloud.add_environment("ELASTIC_CLOUD_USERNAME", "-")
        # sqs_to_elastic_cloud.add_environment("QUEUEURL", sqs_to_elastic_cloud_queue.queue_url )
        # sqs_to_elastic_cloud.add_environment("DEBUG", "False" )



        ######################################################################################################################################################



        ###########################################################################
        # AMAZON VPC  
        ###########################################################################
        vpc = aws_ec2.Vpc(self, "LoadTestVPC", max_azs=3)     # default is all AZs in region


        ###########################################################################
        # AMAZON ECS Repositories  
        ###########################################################################
        # get_repository = aws_ecs.IRepository(self, "get_repository", image_scan_on_push=True, removal_policy=aws_cdk.core.RemovalPolicy("DESTROY") )
        # put_repository = aws_ecs.IRepository(self, "put_repository", image_scan_on_push=True, removal_policy=aws_cdk.core.RemovalPolicy("DESTROY") )
        get_repository = aws_ecr.Repository(self, "get_repository", image_scan_on_push=True, removal_policy=core.RemovalPolicy("DESTROY") )
        put_repository = aws_ecr.Repository(self, "put_repository", image_scan_on_push=True, removal_policy=core.RemovalPolicy("DESTROY") )
        xray_repository = aws_ecr.Repository(self, "xray_repository", image_scan_on_push=True, removal_policy=core.RemovalPolicy("DESTROY") )


        ###########################################################################
        # AMAZON ECS Roles and Policies
        ###########################################################################        
        # task_execution_policy_statement = aws_iam.PolicyStatement(
        #     effect=aws_iam.Effect.ALLOW,
        #     actions=["logs:*", "ecs:*", "ec2:*", "elasticloadbalancing:*","ecr:*"],
        #     resources=["*"]
        #     )
        # task_execution_policy_document = aws_iam.PolicyDocument()
        # task_execution_policy_document.add_statements(task_execution_policy_statement)
        # task_execution_policy = aws_iam.Policy(self, "task_execution_policy", document=task_execution_policy_document)
        # task_execution_role = aws_iam.Role(self, "task_execution_role", assumed_by=aws_iam.ServicePrincipal('ecs-tasks.amazonaws.com') )
        # task_execution_role.attach_inline_policy(task_execution_policy)

        # task_policy_statement = aws_iam.PolicyStatement(
        #     effect=aws_iam.Effect.ALLOW,
        #     actions=["logs:*", "xray:*", "sqs:*", "s3:*"],
        #     resources=["*"]
        #     )
        # task_policy_document = aws_iam.PolicyDocument()
        # task_policy_document.add_statements(task_policy_statement)
        # task_policy = aws_iam.Policy(self, "task_policy", document=task_policy_document)
        # task_role = aws_iam.Role(self, "task_role", assumed_by=aws_iam.ServicePrincipal('ecs-tasks.amazonaws.com') )
        # task_role.attach_inline_policy(task_policy)


        ###########################################################################
        # AMAZON ECS Task definitions
        ###########################################################################
        # get_task_definition = aws_ecs.TaskDefinition(self, "gettaskdefinition",
        #                                                                 compatibility=aws_ecs.Compatibility("FARGATE"), 
        #                                                                 cpu="1024", 
        #                                                                 # ipc_mode=None, 
        #                                                                 memory_mib="2048", 
        #                                                                 network_mode=aws_ecs.NetworkMode("AWS_VPC"), 
        #                                                                 # pid_mode=None,                                      #Not supported in Fargate and Windows containers
        #                                                                 # placement_constraints=None, 
        #                                                                 execution_role=task_execution_role, 
        #                                                                 # family=None, 
        #                                                                 # proxy_configuration=None, 
        #                                                                 task_role=task_role
        #                                                                 # volumes=None
        #                                                                 )

        # put_task_definition = aws_ecs.TaskDefinition(self, "puttaskdefinition",
        #                                                                 compatibility=aws_ecs.Compatibility("FARGATE"), 
        #                                                                 cpu="1024", 
        #                                                                 # ipc_mode=None, 
        #                                                                 memory_mib="2048", 
        #                                                                 network_mode=aws_ecs.NetworkMode("AWS_VPC"), 
        #                                                                 # pid_mode=None,                                      #Not supported in Fargate and Windows containers
        #                                                                 # placement_constraints=None, 
        #                                                                 execution_role=task_execution_role, 
        #                                                                 # family=None, 
        #                                                                 # proxy_configuration=None, 
        #                                                                 task_role=task_role
        #                                                                 # volumes=None
        #                                                                 )




        ###########################################################################
        # AMAZON ECS Images 
        ###########################################################################
        # get_repository_ecr_image = aws_ecs.EcrImage(repository=get_repository, tag="latest")
        # put_repository_ecr_image = aws_ecs.EcrImage(repository=put_repository, tag="latest")
        # xray_repository_ecr_image = aws_ecs.EcrImage(repository=xray_repository, tag="latest")
        # environment_variables = {}
        # environment_variables["SQS_QUEUE"] = ecs_task_queue_queue.queue_url
        # environment_variables["S3_BUCKET"] = storage_bucket.bucket_name
        
        # get_task_log_driver = aws_ecs.LogDriver.aws_logs(stream_prefix="S3LoadTest", log_retention=aws_logs.RetentionDays("ONE_WEEK"))
        # put_task_log_driver = aws_ecs.LogDriver.aws_logs(stream_prefix="S3LoadTest", log_retention=aws_logs.RetentionDays("ONE_WEEK"))
        # xray_task_log_driver = aws_ecs.LogDriver.aws_logs(stream_prefix="S3LoadTest", log_retention=aws_logs.RetentionDays("ONE_WEEK"))


        # get_task_definition.add_container("get_task_definition_get", 
        #                                             image=get_repository_ecr_image, 
        #                                             memory_reservation_mib=1024,
        #                                             environment=environment_variables,
        #                                             logging=get_task_log_driver
        #                                             )
        # get_task_definition.add_container("get_task_definition_xray", 
        #                                             image=xray_repository_ecr_image, 
        #                                             memory_reservation_mib=1024,
        #                                             environment=environment_variables,
        #                                             logging=xray_task_log_driver
        #                                             )

        # put_task_definition.add_container("put_task_definition_put", 
        #                                             image=put_repository_ecr_image, 
        #                                             memory_reservation_mib=1024,
        #                                             environment=environment_variables,
        #                                             logging=put_task_log_driver
        #                                             )
        # put_task_definition.add_container("put_task_definition_xray", 
        #                                             image=xray_repository_ecr_image, 
        #                                             memory_reservation_mib=1024,
        #                                             environment=environment_variables,
        #                                             logging=xray_task_log_driver
        #                                             )


        ###########################################################################
        # AMAZON ECS CLUSTER 
        ###########################################################################
        cluster = aws_ecs.Cluster(self, "LoadTestCluster", vpc=vpc)


        ###########################################################################
        # AWS ROUTE53 HOSTED ZONE 
        ###########################################################################
        hosted_zone = aws_route53.HostedZone(self, "hosted_zone", zone_name="s3workload.com" ,comment="private hosted zone for s3workload system")
        hosted_zone.add_vpc(vpc)
        # bucket_record_values = [storage_bucket.bucket_name]
        # queue_record_values = [ecs_task_queue_queue.queue_url]
        # bucket_record_name = "bucket." + hosted_zone.zone_name
        # queue_record_name = "filesqueue." + hosted_zone.zone_name
        # hosted_zone_record_bucket = aws_route53.TxtRecord(self, "hosted_zone_record_bucket", record_name=bucket_record_name, values=bucket_record_values, zone=hosted_zone, comment="dns record for bucket name")
        # hosted_zone_record_queue = aws_route53.TxtRecord(self, "hosted_zone_record_queue", record_name=queue_record_name, values=queue_record_values, zone=hosted_zone, comment="dns record for queue name")


