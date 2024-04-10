from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
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


def test_get_source_high_watermark(spark,
                                   cdc_log_valid_raw_data,
                                   cdc_log_valid_schema
                                   ):
    input_data = spark.createDataFrame(cdc_log_valid_raw_data,
                                       cdc_log_valid_schema
                                       )
    cdc1 = cdcIngestion(spark)
    source_high_watermark = cdc1.get_source_high_watermark(
        source_data=input_data,
        high_water_column="Identificator",
    )
    assert source_high_watermark == "id6"


def test_get_target_high_watermark(spark,
                                   cdc_log_valid_schema,
                                   cdc_conformed_data
                                   ):
    input_data = spark.createDataFrame(cdc_conformed_data,
                                       cdc_log_valid_schema
                                       )
    cdc = cdcIngestion(spark)
    target_high_watermark = cdc.get_target_high_watermark(
        spark,
        high_water_column="identificator",
        target_data=input_data,
    )
    assert target_high_watermark == "id6"

def test_get_high_watermark(
        spark,
        cdc_log_valid_schema,
        cdc_log_valid_raw_data,
        cdc_conformed_data
        ):
    source_data = spark.createDataFrame(cdc_log_valid_raw_data,
                                       cdc_log_valid_schema
                                       )

    target_data = spark.createDataFrame(cdc_conformed_data,
                                       cdc_log_valid_schema
                                       )

    cdc = cdcIngestion(spark)
    source_high_watermark, target_high_watermark = (
    cdc.get_high_watermark(high_water_column ='identificator',
                           source_data=source_data, 
                           target_data=target_data
                           ))
    assert source_high_watermark == 'id6'
    assert target_high_watermark == 'id6'


def test_get_latest_updates(
        spark,
        cdc_log_valid_raw_data,
        cdc_log_valid_schema
        ):
    source_data = spark.createDataFrame(
            cdc_log_valid_raw_data, 
            cdc_log_valid_schema
            )
    cdc = cdcIngestion(spark)
    update_data = cdc.get_updates(
            source_data=source_data,
            target_high_watermark='1',
            high_water_column = 'transact_id'
            )
    assert len(update_data.filter(update_data.op.isNull()).collect()) ==0
    assert len(update_data.filter(update_data.transact_id.isNull()).collect())==0
    assert(update_data.select('transact_id')
           .agg(F.min('transact_id'))
           .collect()[0][0]) == '2'
