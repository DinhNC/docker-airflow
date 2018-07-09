import airflow
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG

from datetime import datetime, timedelta
import time
import random

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
    "provide_context": True,
    "catchup": False
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}
dag_id = 'short_cirtuit_v1'
tid_check_sync_enabled = 'check_sync_enabled'
tid_spark_submit = 'spark_submit'
tid_distcp = 'distcp'
db_name = 'zomato4'
op_args = [db_name]


# helper methods
def mimic_task(task_name, success_percent=100, sleep_duration=0):
	time.sleep(sleep_duration)
	if (random.randint(1, 101) <= success_percent):
		print('%s succeeded' % (task_name))
		return True
	else:
		print('%s failed' % (task_name))
		return False


# callable methods
def check_sync_enabled(db_name, **kwargs):
	return mimic_task('check_sync_enabled for %s' % db_name, 60, 1)

def spark_submit(db_name, **kwargs):
	return mimic_task('spark_submit for %s' % db_name, 50, 5)

def distcp(db_name, **kwargs):
	mimic_task('distcp for %s' % db_name, 60, 2)


# DAG & operators
dag = DAG(dag_id=dag_id, default_args=default_args,
	      schedule_interval=None, catchup=True)

sc_op_check = ShortCircuitOperator(task_id=tid_check_sync_enabled, dag=dag,
	                               python_callable=check_sync_enabled,
	                               op_args=op_args)

sc_op_spark = ShortCircuitOperator(task_id=tid_spark_submit, dag=dag,
	                               python_callable=spark_submit,
	                               op_args=op_args)

py_op_disctp = PythonOperator(task_id=tid_distcp, dag=dag,
	                          python_callable=distcp,
	                          op_args=op_args)

# link
sc_op_check >> sc_op_spark >> py_op_disctp