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


@mock_aws
def test_redshift_config(mock_redshift):
    response = mock_redshift.execute_statement(
        ClusterIdentifier='test-cluster',
        Database='dwh',
        DbUser='test-user',
        Sql="select * from dummy")
    backend = redshiftdata_backends[DEFAULT_ACCOUNT_ID]["us-east-1"]  # Use the appropriate account/region
    print("dummy")
    for statement in backend.statements.values():
        print("itter")
        print(statement.query_string)

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


def test_process_updates(
        spark,
        cdc_log_valid_schema,
        cdc_update_data,
        ):
    update_data = spark.createDataFrame(cdc_update_data, cdc_log_valid_schema)

    cdc = cdcIngestion(spark)
    deletes, upsert_data  = cdc.process_updates(
            update_data=update_data,
            unique_key=["identificator"],
            high_water_column="transact_id"
            )
    assert len(deletes.filter(deletes.op != 'D').collect()) == 0
    assert deletes.dropDuplicates(["identificator"]).count() == deletes.count()
    assert len(upsert_data.filter(upsert_data.op == 'D').collect()) == 0
    assert upsert_data.dropDuplicates(["identificator"]).count() == upsert_data.count()


def test_process_multiple_updates(
        spark,
        cdc_data_multiple_updates,
        cdc_log_valid_schema
        ):
    update_data = spark.createDataFrame(cdc_data_multiple_updates, cdc_log_valid_schema)
    cdc = cdcIngestion(spark)
    delets, upsert_data = cdc.process_updates(
            update_data= update_data,
            unique_key=["identificator"],
            high_water_column="transact_id"
            )
    assert len(delets.collect())==0
    assert len(upsert_data.collect())>0
    assert upsert_data.dropDuplicates(["identificator"]).count() == upsert_data.count()
    assert (upsert_data.filter(F.col("identificator")=="id3")
            .select(upsert_data.transact_id)
            .collect()[0][0]) == "4"

def test_process_updates_with_delete(spark,
                                     cdc_data_update_with_delete,
                                     cdc_log_valid_schema
                                     ):
    update_data = spark.createDataFrame(cdc_data_update_with_delete, cdc_log_valid_schema)

    cdc = cdcIngestion(spark)
    deletes, upsert_data = cdc.process_updates(
            update_data= update_data,
            unique_key=["identificator"],
            high_water_column="transact_id"
             )
    assert len(upsert_data.collect())==0
    assert len(deletes.collect())>0
    assert deletes.columns == update_data.columns
    assert upsert_data.columns == update_data.columns

def test_process_updates_with_duplicates(spark,
                                         cdc_data_update_with_duplicates,
                                         cdc_log_valid_schema):
    update_data = spark.createDataFrame(cdc_data_update_with_duplicates,
                                        cdc_log_valid_schema)

    cdc = cdcIngestion(spark)
    deletes, upsert_data = cdc.process_updates(
            update_data= update_data,
            unique_key=["identificator"],
            high_water_column="transact_id"
            )
    assert len(upsert_data.collect())==2
    assert len(deletes.collect())==1
@mock_aws
def test_redshift_write(spark,
                        cdc_upsert_data,
                        cdc_deletes_data,
                        cdc_log_valid_schema
                        ):
 
    deletes = spark.createDataFrame(cdc_deletes_data, cdc_log_valid_schema)
    upsert_data = spark.createDataFrame(cdc_upsert_data, cdc_log_valid_schema)

    cdc = cdcIngestion(spark)
#    reports = cdc.redshift_writer(
        #          target_table= "",
        #         deleted=deletes,
        #         upsert_data=upsert_data,
        #          )
