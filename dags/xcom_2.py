from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2018, 7, 1),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}
in_file_path = 'in_files/'
out_file_path = 'out_files/'


def read_list(file_path):
    with open(file_path) as file:
        lines = file.read().splitlines()

    return [int(line.strip()) for line in lines]

def write_list(file_path, lines):
    with open(file_path, mode='wt') as file:
        file.write('\n'.join([str(line) for line in lines]))

def add_lists(list_1_name, list_2_name):
    sum_list = [(ele_1 + ele_2) for ele_1, ele_2 in list(zip(list_1, list_2))]
    return sum_list


dag = DAG(dag_id="xcom-2-v1", default_args=default_args,
          schedule_interval=timedelta(minutes=1), catchup=False)

py_op_read_1 = PythonOperator(task_id='py_op_read_1', python_callable=read_list,
                              dag=dag, op_args=[in_file_path + 'f1.txt'])
py_op_read_2 = PythonOperator(task_id='py_op_read_2', python_callable=read_list,
                              dag=dag, op_args=[in_file_path + 'f2.txt'])
py_op_read_3 = PythonOperator(task_id='py_op_read_3', python_callable=read_list,
                              dag=dag, op_args=[in_file_path + 'f3.txt'])
py_op_read_4 = PythonOperator(task_id='py_op_read_4', python_callable=read_list,
                              dag=dag, op_args=[in_file_path + 'f4.txt'])

py_op_add_12 = PythonOperator(task_id='py_op_add_1', python_callable=read_list,
                              dag=dag, op_args=[in_file_path + 'f1.txt'])