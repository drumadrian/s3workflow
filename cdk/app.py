from aws_cdk import core
from cdk.cdk_stack import CdkStack
from aws_cdk.core import App, Stack, Tags


app = core.App()
# app.build()
s3workflowstack = CdkStack(app, "s3workflow")

Tags.of(s3workflowstack).add("auto-delete","no")
Tags.of(s3workflowstack).add("project","s3workflow")

app.synth()
#!/usr/bin/env python3
