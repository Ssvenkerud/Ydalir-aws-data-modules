import pytest
import sys


print(%pwd)
def test_dummy():
    assert True


@pytest.skip(reason="temp disables")
def test_Redshift_executor_init():
    test_result = ReshiftExecutor(name="testJob")

    assert type(test_result.redshift_conn) == ""


def test_adhock():
    import boto3
    client = boto3.client('redshift-data')
    print(type(client))



