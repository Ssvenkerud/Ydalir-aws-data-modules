from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from moto import mock_aws
import boto3
import pytest
import sys
import os


TestSpark = SparkSession.builder.getOrCreate()


@pytest.fixture(scope="session")
def cdc_log_valid_schema():
    return StructType(
        [
            StructField("Identificator", StringType(), False),
            StructField("stringvar", StringType(), True),
            StructField("numvar", LongType(), True),
            StructField("Boolvar", BooleanType(), True),
            StructField("timevar", StringType(), True),
            StructField("datevar", StringType(), True),
            StructField("op", StringType(), True),
            StructField("transact_id", StringType(), False),
        ]
    )


@pytest.fixture(scope="session")
def cdc_log_valid_raw_data():
    return [
        ("id1", "monkey", 1, True, "2021-01-01 01:01:01.000", "2021-01-01", "", ""),
        ("id2", "tiger", 1345, False, "2021-02-02 02:02:02.000", "2022-02-02", "", ""),
        ("id3", "Elefant", 256, False, "2021-03-03 03:03:03.000", "2023-03-03", "", ""),
        (
            "id4",
            "snake",
            10854751,
            True,
            "2021-04-04-04:04:04.000",
            "2021-04-04",
            "",
            "",
        ),
        (
            "id5",
            "moose",
            17438531,
            True,
            "2021-05-05-05:05:05.000",
            "2021-05-05",
            "",
            "",
        ),
        (
            "id5",
            "moose",
            17438531,
            True,
            "2021-06-06-06:06:06.000",
            "2026-06-06",
            "U",
            "1",
        ),
        (
            "id5",
            "moose",
            17465731,
            True,
            "2021-07-07-07:07:07.000",
            "2027-07-07",
            "U",
            "2",
        ),
        (
            "id1",
            "monkey",
            17438531,
            True,
            "2021-08-08-08:08:08.000",
            "2028-08-08",
            "U",
            "3",
        ),
        (
            "id5",
            "moose",
            17438531,
            True,
            "2021-09-09-09:09:09.000",
            "2029-09-09",
            "U",
            "4",
        ),
        (
            "id6",
            "Squirrel",
            17438531,
            True,
            "2021-10-10-10:10:10.000",
            "2030-10-10",
            "I",
            "5",
        ),
        ("id2", None, None, None, None, None, "D", "6"),
    ]


@pytest.fixture(scope="session")
def cdc_conformed_data():
    return [
        ("id3", "Elefant", 256, False, "2021-03-03 03:03:03.000", "2023-03-03", "", ""),
        (
            "id4",
            "snake",
            10854751,
            True,
            "2021-04-04-04:04:04.000",
            "2021-04-04",
            "",
            "",
        ),
        (
            "id1",
            "monkey",
            17438531,
            True,
            "2021-08-08-08:08:08.000",
            "2028-08-08",
            "U",
            "3",
        ),
        (
            "id5",
            "moose",
            17438531,
            True,
            "2021-09-09-09:09:09.000",
            "2029-09-09",
            "U",
            "4",
        ),
        (
            "id6",
            "Squirrel",
            17438531,
            True,
            "2021-10-10-10:10:10.000",
            "2030-10-10",
            "I",
            "5",
        ),
    ]

@pytest.fixture(scope="session")
def cdc_update_data():
    return [(
            "id5",
            "moose",
            17438531,
            True,
            "2021-06-06-06:06:06.000",
            "2026-06-06",
            "U",
            "1",
        ),
        (
            "id5",
            "moose",
            17465731,
            True,
            "2021-07-07-07:07:07.000",
            "2027-07-07",
            "U",
            "2",
        ),
        (
            "id1",
            "monkey",
            17438531,
            True,
            "2021-08-08-08:08:08.000",
            "2028-08-08",
            "U",
            "3",
        ),
        (
            "id5",
            "moose",
            17438531,
            True,
            "2021-09-09-09:09:09.000",
            "2029-09-09",
            "U",
            "4",
        ),
        (
            "id6",
            "Squirrel",
            17438531,
            True,
            "2021-10-10-10:10:10.000",
            "2030-10-10",
            "I",
            "5",
        ),
        ("id2", None, None, None, None, None, "D", "6"),
        ("id8", "Crow", 97342, False, "2045-04-12:12:13:34.343",
         "2045-04-12","I", "7"),
        ("id8", None, None, None, None, None, "D", "8"),
        ("id8", None, None, None, None, None, "D", "8"),
            ]

@pytest.fixture(scope="session")
def cdc_data_multiple_updates():
    return [
    ("id3", "Elefant",1 , False, "2021-03-03 03:03:03.000", "2023-03-03", "", ""),
    ("id3", "Elefant",2 , False, "2022-03-03 03:03:03.000", "2023-03-03", "U","1"),
    ("id3", "Elefant", 3, False, "2023-03-03 03:03:03.000", "2023-03-03", "U","2"),
    ("id3", "Elefant", 4, False, "2024-03-03 03:03:03.000", "2023-03-03", "U","3"),
    ("id3", "Elefant", 5, False, "2025-03-03 03:03:03.000", "2023-03-03", "U","4"),
    ]
@pytest.fixture(scope="session")
def cdc_data_update_with_delete():
    return [
    ("id3", "Elefant", 4, False, "2024-03-03 03:03:03.000", "2023-03-03", "U","3"),
    ("id3", None, None, None, None, None, "D","4"),
    ]


@pytest.fixture(scope="session")
def cdc_data_update_with_duplicates():
    return [
    ("id3", "Elefant",1 , False, "2021-03-03 03:03:03.000", "2023-03-03", "", ""),
    ("id3", "Elefant", 3, False, "2023-03-03 03:03:03.000", "2023-03-03", "U","2"),
    ("id3", "Elefant", 3, False, "2023-03-03 03:03:03.000", "2023-03-03", "U","2"),
    ("id4", "Mouse", 5, False, "2025-03-03 03:03:03.000", "2023-03-03", "U","4"),
    ("id5", None, None, None, None, None, "D","6"),
    ("id5", None, None, None, None, None, "D","6"),
    ]
@pytest.fixture(scope="session")
def cdc_upsert_data():
    return [
        (
            "id1",
            "monkey",
            17438531,
            True,
            "2021-08-08-08:08:08.000",
            "2028-08-08",
            "U",
            "3",
        ),
        (
            "id5",
            "moose",
            17438531,
            True,
            "2021-09-09-09:09:09.000",
            "2029-09-09",
            "U",
            "4",
        ),
        (
            "id6",
            "Squirrel",
            17438531,
            True,
            "2021-10-10-10:10:10.000",
            "2030-10-10",
            "I",
            "5",
        ),


            ]
@pytest.fixture(scope="session")
def cdc_deletes_data():
    return [
            ("id3", None, None, None, None, None, "D","4"),

            ]
@pytest.fixture(scope="session")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"

@mock_aws
@pytest.fixture(scope="session")
def mock_redshift(aws_credentials):
    redshift_client =  boto3.client("redshift-data", region_name="eu-north-1")
    yield redshift_client

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("Test_engine").getOrCreate()


