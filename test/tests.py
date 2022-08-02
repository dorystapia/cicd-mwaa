# Purpose: Pytest test for MWAA Dags
#          Airflow Demonstration project DAGs
# Author: Gary A. Stafford
# Modified: 2021-12-10

from asyncio import tasks
from cgi import test
from ctypes import pythonapi
from datetime import datetime
from multiprocessing import context
import os
from platform import python_build
import sys

import pytest
from airflow.models import DagBag 
# from airflow.operators.python_operator import PythonOperator 
# from airflow.operators.bash_operator import BashOperator

sys.path.append(os.path.join(os.path.dirname(__file__), "../dags"))
sys.path.append(os.path.join(os.path.dirname(__file__), "../dags/plugins"))

# Airflow variables called from DAGs under test are stubbed out
os.environ["AIRFLOW_VAR_BUCKET"] = "test_bucket"
os.environ["AIRFLOW_VAR_ATHENA_QUERY_RESULTS"] = "SELECT 1;"
os.environ["AIRFLOW_VAR_SNS_TOPIC"] = "test_topic"
os.environ["AIRFLOW_VAR_REDSHIFT_UNLOAD_IAM_ROLE"] = "test_role_1"
os.environ["AIRFLOW_VAR_GLUE_CRAWLER_IAM_ROLE"] = "test_role_2"
#os.environ["more variables"] = ""


@pytest.fixture(params=["../dags/"])
def dag_bag(request):
    return DagBag(dag_folder=request.param, include_examples=False)

def test_no_import_errors(dag_bag):
    assert not dag_bag.import_errors

def test_requires_tags(dag_bag):
    for dag_id, dag in dag_bag.dags.items():
        assert dag.tags

def test_requires_specific_tag(dag_bag):
    for dag_id, dag in dag_bag.dags.items():
        try:
            assert dag.tags.index("") >= 0
        except ValueError:
            assert dag.tags.index("") >= 0

def test_desc_len_greater_than_fifteen(dag_bag):
    for dag_id, dag in dag_bag.dags.items():
        assert len(dag.description) > 15

def test_owner_len_greater_than_five(dag_bag):
    for dag_id, dag in dag_bag.dags.items():
        assert len(dag.owner) > 5

def test_airflow_owner(dag_bag):
    for dag_id, dag in dag_bag.dags.items():
        assert str.lower(dag.owner) != "airflow"

def test_no_emails_on_retry(dag_bag):
    for dag_id, dag in dag_bag.dags.items():
        assert not dag.default_args["email_on_retry"]

def test_no_emails_on_failure(dag_bag):
    for dag_id, dag in dag_bag.dags.items():
        assert not dag.default_args["email_on_failure"]

def test_retries(dag_bag):
    for dag_id, dag in dag_bag.dags.items():
        retries = dag_bag.dags[dag].default_args.get('retries', [])
        error_msg = 'Retries not set to 2 for DAG {id}'.format(id=dag)
        assert retries == 2, error_msg
# def test_python():
#         test = PythonOperator(task_id="Test", python_callable=lambda: "testme")
#         result = test.execute(context=[])
#         assert result == "testme PythonOperator"
# def execute():
#         test = BashOperator(task_id="test", bash_command="echo testme", xcom_push=True )
#         result = test.execute(context={})
#         assert result == "testme BashOperator"

# class BaseOperator():
#     def execute(self, context):
#         raise NotImplementedError()

# def next_week(**context):
#     return context["execution_date"] + datetime.timedelta(days=7)

# def test_python_operator():
#     test = PythonOperator(tasks_id="test", python_callable=next_week, provide_context=True)
#     testdate = datetime.datetime(2022, 8, 8)
#     result = test.execute(context={"execution_date": testdate})
#     assert result == testdate + datetime.timedelta(days=7)
