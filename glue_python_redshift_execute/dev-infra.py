
from aws_cdk import App

from "Ydalir-aws-data-modules".cdk.redshift_stack import RedshiftStack
from Ydalir-aws-data-modules.cdk.vpc_stack import VpcStack
from Ydalir-aws-data-modules.cdk.glue_raw_to_redshift import GluePipelineStack
from Ydalir-aws-data-modules.cdk.s3_bucket_stack import S3BucketStack

app = App()

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
s3_storage = S3BucketStack(app, f"{app.node.try_get_context('project')}-StorageBuckets")
glue_ingestion_ssb_deaths = GluePipelineStack(app,
                                       f"{app.node.try_get_context('project')}-GluePipelineStack", 
                                       "ssb_death_copy")
app.synth()
