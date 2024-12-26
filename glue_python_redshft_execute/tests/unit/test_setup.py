import boto3


def test_boto_conn(aws_creds):
    try:
        client = boto3.client(
            "s3",
            aws_access_key_id=aws_creds["aws_access_key_id"],
            aws_secret_access_key=aws_creds["aws_secret_access_key"],
        )
        response = client.list_buckets()
        return True
    except Exception as e:
        if (
            str(e)
            != "An error occurred (InvalidAccessKeyId) when calling the ListBuckets operation: The AWS Access Key Id you provided does not exist in our records."
        ):
            return True
        return False


def test_redshift_conn(aws_creds, redshift_creds):
    try:
        client = boto3.client(
            "redshift-data",
            aws_access_key_id=aws_creds["aws_access_key_id"],
            aws_secret_access_key=aws_creds["aws_secret_access_key"],
        )
        responses = client.list_databases(
            Database=redshift_creds["Database"],
            DbUser=redshift_creds["DbUser"],
            ClusterIdentifier=redshift_creds["ClusterIdentifier"],
        )
        assert True

    except:
        assert False
