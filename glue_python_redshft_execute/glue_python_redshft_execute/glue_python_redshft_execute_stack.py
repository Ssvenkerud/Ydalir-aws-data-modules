from aws_cdk import Stack  # Duration,; aws_sqs as sqs,
from constructs import Construct


class GluePythonRedshftExecuteStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # The code that defines your stack goes here

        # example resource
        # queue = sqs.Queue(
        #     self, "GluePythonRedshftExecuteQueue",
        #     visibility_timeout=Duration.seconds(300),
        # )
