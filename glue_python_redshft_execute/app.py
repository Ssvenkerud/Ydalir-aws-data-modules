#!/usr/bin/env python3


import aws_cdk as cdk

from glue_python_redshft_execute.Redshift import RedshiftStack
from glue_python_redshft_execute.vpc import VpcStack

app = cdk.App()

vpc_stack = VpcStack(
    app,
    f"{app.node.try_get_context('project')}-VpcStack",
    stack_log_level="INFO",
    description="",
)

# Deploy Redshift cluster and load data"

redshift_demo = RedshiftStack(
    app,
    f"{app.node.try_get_context('project')}-redshiftStack",
    vpc=vpc_stack,
    ec2_instance_type="dc2.large",
    stack_log_level="INFO",
    description="",
)

app.synth()
