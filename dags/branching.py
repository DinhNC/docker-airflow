from airflow import DAG
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

import time
import datetime
from datetime import datetime, timedelta
import calendar

# args
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(year=2018, month=7, day=9),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
    "provide_context": True
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}
dag_id = 'branch-v1'
tid_branch = 'tid_branch'
tid_stat_pipeline = 'tid_stat_pipeline'
tid_dummy_start = 'tid_dummy_start'
tid_dummy_branch = 'tid_dummy_branch'
tid_dummy_join = 'tid_dummy_join'

# methods
def branch_decider(**kwargs):
    execution_date = kwargs['execution_date']
    if calendar.day_name[execution_date.weekday()] == 'Saturday':
        return tid_stat_pipeline
    else:
        return tid_dummy_branch

def stat_pipeline(**kwargs):
	print('stat_pipeline started')
	time.sleep(5)
	print('stat_pipeline completed')

# dag & tasks
dag = DAG(dag_id=dag_id, default_args=default_args,
          schedule_interval=None, catchup=True)

dummy_op_start = DummyOperator(task_id=tid_dummy_start, dag=dag)

branch_op = BranchPythonOperator(task_id=tid_branch, dag=dag,
                                 python_callable=branch_decider)
py_op_stat_pipeline = PythonOperator(task_id=tid_stat_pipeline, dag=dag,
	                                 python_callable=stat_pipeline)
dummy_op_branch = DummyOperator(task_id=tid_dummy_branch, dag=dag)

dummy_op_join = DummyOperator(task_id=tid_dummy_join, dag=dag,
	                          trigger_rule='one_success')

# order of execution
branch_op.set_upstream(dummy_op_start)

py_op_stat_pipeline.set_upstream(branch_op)
dummy_op_branch.set_upstream(branch_op)

dummy_op_join.set_upstream([py_op_stat_pipeline, dummy_op_branch])
