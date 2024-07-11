from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
import boto3
from moto import mock_aws
from moto.core import DEFAULT_ACCOUNT_ID
from moto.redshiftdata import redshiftdata_backends
import pytest
import sys
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))
from src.cdcIngestion import cdcIngestion


def test_engine_test(spark):
    assert spark.active()
    spark_conf = spark.sparkContext._conf.getAll()
    print(spark_conf)


def test_simple_cdc():
    pass

