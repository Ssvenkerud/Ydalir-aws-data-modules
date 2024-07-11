import aws_cdk as core
import aws_cdk.assertions as assertions

from glue_python_redshft_execute.glue_python_redshft_execute_stack import GluePythonRedshftExecuteStack

# example tests. To run these tests, uncomment this file along with the example
# resource in glue_python_redshft_execute/glue_python_redshft_execute_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = GluePythonRedshftExecuteStack(app, "glue-python-redshft-execute")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
