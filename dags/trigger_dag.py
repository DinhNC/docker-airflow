import airflow
import airflow.utils.helpers
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.models import DAG

from datetime import datetime, timedelta


# args and params
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(year=2018, month=7, day=12),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
    "provide_context": False,
    "catchup": False,
    "trigger_rule": 'all_done'
}
did_parent = 'importer_parent_v1'
did_tid_list = [('importer_child_v1_db_1', 'importer_v1_db_1'),
                ('importer_child_v1_db_2', 'importer_v1_db_2'),
                ('importer_child_v1_db_3', 'importer_v1_db_3')]

def create_trigger_dag_operator(dag, trigger_dag_id, task_id):
    # Define the single task in this controller example DAG
    trigger_op = TriggerDagRunOperator(task_id=task_id,
                                       trigger_dag_id=trigger_dag_id,
                                       python_callable=lambda context, dag_run_obj : dag_run_obj,
                                       dag=dag)
    return trigger_op

def create_trigger_dag_operators(dag, did_tid_list):
    trigger_ops = [create_trigger_dag_operator(dag, did, tid) for did, tid in did_tid_list]
    # chain triggerdagrun-operators together
    airflow.utils.helpers.chain(*trigger_ops)
    return trigger_ops

dag = DAG(dag_id=did_parent,
          default_args=default_args,
          schedule_interval=None)

trigger_ops = create_trigger_dag_operators(dag, did_tid_list)