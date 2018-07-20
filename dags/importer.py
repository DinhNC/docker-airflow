import airflow
import airflow.utils.helpers
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import DAG

from datetime import datetime, timedelta
import time
import random


# args
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(year=2018, month=7, day=10),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
    "provide_context": False,
    "catchup": False
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}
did_importer_prefix = 'importer_child_v1_'
tid_prefix_check = 'check_'
tid_prefix_spark = 'spark_submit_'
tid_prefix_distcp = 'distcp_'
db_names = ['db_1', 'db_2', 'db_3']


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
def check_sync_enabled(db_name, exception_percent=0, **kwargs):
    if (random.randint(1, 101) <= exception_percent):
        raise Exception('Exception in check_sync_enabled for %s' % db_name)
    else:
        return mimic_task('check_sync_enabled for %s' % db_name, 70, 2)

def spark_submit(db_name, **kwargs):
    if mimic_task('spark_submit for %s' % db_name, 60, 10) == False:
        raise Exception('Exception in spark_submit for %s' % db_name)
    else:
        return True

def distcp(db_name, **kwargs):
    if mimic_task('distcp for %s' % db_name, 50, 5) == False:
        raise Exception('Exception in dictcp for %s' % db_name)


# db_importer dags
def create_dag(did_prefix, db_name):
    did_importer = did_prefix + db_name
    dag = DAG(dag_id=did_importer,
              default_args=default_args,
              schedule_interval=None)
    op_args=[db_name]

    tid_check = tid_prefix_check + db_name
    op_args_copy = list(op_args)
    op_args_copy.append(30)
    sc_op_check = ShortCircuitOperator(task_id=tid_check, dag=dag,
                                       python_callable=check_sync_enabled,
                                       op_args=op_args_copy)

    tid_spark = tid_prefix_spark + db_name
    sc_op_spark = ShortCircuitOperator(task_id=tid_spark, dag=dag,
                                       python_callable=spark_submit,
                                       op_args=op_args)

    tid_distcp = tid_prefix_distcp + db_name
    py_op_disctp = PythonOperator(task_id=tid_distcp, dag=dag,
                                  python_callable=distcp,
                                  op_args=op_args)

    sc_op_check >> sc_op_spark >> py_op_disctp
    return dag

for db_name in db_names:
    dag_id = did_importer_prefix + db_name
    globals()[dag_id] = create_dag(did_importer_prefix, db_name)