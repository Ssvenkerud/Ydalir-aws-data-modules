from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
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
def spark():
    return SparkSession.builder.appName("Test_engine").getOrCreate()
