from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

from datetime import datetime, timedelta


# args
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(year=2018, month=7, day=6),
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
dag_id = 'xcom-1-v7'
xcom_list_int = [1, 2, 3]
xcom_list_str = ['one', 'two', 'three']
xcom_key = 'xcom_key_'


# methods
def push_by_return(**kwargs):
    return (xcom_list_int, xcom_list_str)

def push_by_key(key=None, **kwargs):
    kwargs['ti'].xcom_push(key=key+'int_str', value=(xcom_list_int, xcom_list_str))
    kwargs['ti'].xcom_push(key=key+'str', value=xcom_list_str)
    kwargs['ti'].xcom_push(key=key+'int', value=xcom_list_int)

def pull(task_ids, dag_id=None, key=None, **kwargs):
    print('pulled_msg: ', end='')
    print(kwargs['ti'].xcom_pull(task_ids=task_ids, dag_id=dag_id, key=key))


# dag & tasks
dag = DAG(dag_id=dag_id, default_args=default_args,
          schedule_interval=None, catchup=True)

py_op_push_by_return = PythonOperator(task_id='py_op_push_by_return',
                                      dag=dag, python_callable=push_by_return)
py_op_push_by_key = PythonOperator(task_id='py_op_push_by_key',
                                   dag=dag, python_callable=push_by_key,
                                   op_args=[xcom_key])

py_op_pull_by_task_ids = PythonOperator(task_id='py_op_pull_by_task_ids',
                                        dag=dag, python_callable=pull,
                                        op_args=['py_op_push_by_key'])
py_op_pull_by_dag_id = PythonOperator(task_id='py_op_pull_by_dag_id',
                                      dag=dag, python_callable=pull,
                                      op_args=[['py_op_push_by_key'], dag_id])
py_op_pull_by_key = PythonOperator(task_id='py_op_pull_by_key',
                                   dag=dag, python_callable=pull,
                                   op_args=['py_op_push_by_key', None, xcom_key+'int_str'])


# relationships
sleep_op_1 = BashOperator(task_id='sleep_op_1', dag=dag,
                          bash_command='sleep 5')
sleep_op_1.set_downstream([py_op_push_by_return, py_op_push_by_key])

sleep_op_2 = BashOperator(task_id='sleep_op_2', dag=dag,
                          bash_command='sleep 5')
sleep_op_2.set_upstream([py_op_push_by_return, py_op_push_by_key])
sleep_op_2.set_downstream([py_op_pull_by_task_ids, py_op_pull_by_dag_id,
                           py_op_pull_by_key])

sleep_op_3 = BashOperator(task_id='sleep_op_3', dag=dag,
                          bash_command='sleep 5')
sleep_op_3.set_upstream([py_op_pull_by_task_ids, py_op_pull_by_dag_id,
                         py_op_pull_by_key])
