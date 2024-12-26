import pytest
import boto3
import time

from glue_python_redshft_execute.glue_python_redshft_execute import RedshiftExecutor


def test_simple_execution(test_client, redshift_creds):
    redex = RedshiftExecutor(redshift_creds, redshift_client=test_client)
    query = """CREATE OR REPLACE TABLE testing AS ID as int"""

    redex.execute_statement("Test_case", query)
    assert redex.execution_status == 200


def test_check_status(RS_executor, redshift_creds):
    query = """create table if not exists T1 (
                col1 Varchar(20) collate case_insensitive
                 );
                insert into T1 values ('bob'), ('john'), ('Tom'), ('JOHN'), ('Bob');
                """
    setup = boto3.client("redshift-data")
    setup_response = setup.execute_statement(
            ClusterIdentifier=redshift_creds["ClusterIdentifier"],
            Database=redshift_creds["Database"],
            DbUser=redshift_creds["DbUser"],
            Sql=query,
            StatementName="statustest",
            )

    redex = RS_executor
    redex.check_status(setup_response["Id"])

    assert redex.status == "FINISHED"
    assert redex.status_message == "Statement finished sucsessfully"


def test_failing_table_exist_query(RS_executor, redshift_creds):
    query1 = """DROP TABLE public.failing1;
                create table public.failing1(
                col1 Varchar(20) collate case_insensitive
                );
            """
    query2 = """create table public.failing1(
                col1 Varchar(20) collate case_insensitive
                );
                """

    setup = boto3.client("redshift-data")
    setup.execute_statement(
            ClusterIdentifier=redshift_creds["ClusterIdentifier"],
            Database=redshift_creds["Database"],
            DbUser=redshift_creds["DbUser"],
            Sql=query1,
            StatementName="statustest",
            )
    setup_response2 = setup.execute_statement(
            ClusterIdentifier=redshift_creds["ClusterIdentifier"],
            Database=redshift_creds["Database"],
            DbUser=redshift_creds["DbUser"],
            Sql=query2,
            StatementName="statustest",
            )

    redex = RS_executor
    redex.check_status(setup_response2["Id"])

    assert redex.status == "FAILED"
    assert redex.status_message == """ERROR: Relation "failing1" already exists"""


def test_error_query(RS_executor, redshift_creds):
    query = """fail"""
    setup = boto3.client("redshift-data")
    setup_response1 = setup.execute_statement(
            ClusterIdentifier=redshift_creds["ClusterIdentifier"],
            Database=redshift_creds["Database"],
            DbUser=redshift_creds["DbUser"],
            Sql=query,
            StatementName="statustest",
            )
    redex = RS_executor
    redex.check_status(setup_response1["Id"])

    assert redex.status == "FAILED"


def test_abort_query(RS_executor, redshift_creds):
    query1 = """DROP TABLE public.failing1;
                create table public.failing1(
                col1 Varchar(20) collate case_insensitive
                );
            """

    setup = boto3.client("redshift-data")
    setup_response1 = setup.execute_statement(
            ClusterIdentifier=redshift_creds["ClusterIdentifier"],
            Database=redshift_creds["Database"],
            DbUser=redshift_creds["DbUser"],
            Sql=query1,
            StatementName="statustest",
            )
    setup.cancel_statement(
                    Id=setup_response1["Id"]
                    )
    redex = RS_executor
    redex.check_status(setup_response1["Id"])

    assert redex.status == "ABORTED"
    assert redex.status_message == "Query was Canceled"
