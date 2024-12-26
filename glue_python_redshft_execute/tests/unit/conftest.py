import pytest
import boto3
import os

from glue_python_redshft_execute.glue_python_redshft_execute import RedshiftExecutor



@pytest.fixture(scope="session")
def aws_creds():
    return {
        "aws_access_key_id": os.environ["AWS_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["AWS_SECRET_ACCESS_KEY"],
        # "aws_session_token":  os.environ["SESSION_TOKEN"],
        "aws_region": os.environ["AWS_DEFAULT_REGION"],
    }


@pytest.fixture(scope="session")
def redshift_creds():
    return {
        "Database": "test_db",
        "DbUser": "dwh_user",
        "SecretArn": "arn:aws:secretsmanager:eu-north-1:851725430422:secret:RedshiftClusterSecret-RAPGwX",
        "ClusterIdentifier": "redshiftcluster-tysluuzgx1l5",
    }


@pytest.fixture(scope="session")
def test_client(aws_creds, redshift_creds):

    client = boto3.client(
        "redshift-data",
        aws_access_key_id=aws_creds["aws_access_key_id"],
        aws_secret_access_key=aws_creds["aws_secret_access_key"],
    )

@pytest.fixture(scope="session")
def RS_executor(test_client, redshift_creds):
    return RedshiftExecutor(redshift_creds, redshift_client=test_client)
