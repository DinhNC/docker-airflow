from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2018, 7, 5),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
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


def add_lists(list_1, list_2):
    sum_list = [(ele_1 + ele_2) for ele_1, ele_2 in list(zip(list_1, list_2))]
    return sum_list


def process_files(in_file_path_1, in_file_path_2, out_file_path):
    list_1 = read_list(in_file_path_1)
    list_2 = read_list(in_file_path_2)
    list_sum = add_lists(list_1, list_2)
    write_list(out_file_path, list_sum)




dag = DAG(dag_id="file-sum-1-v6", default_args=default_args,
          schedule_interval='@once', catchup=True)

py_op_12_args = [in_file_path + 'f1.txt', in_file_path + 'f2.txt', out_file_path + 'f12.txt']
py_op_12 = PythonOperator(task_id='s12', python_callable=process_files,
                                dag=dag, op_args=py_op_12_args)

py_op_34_args = [in_file_path + 'f3.txt', in_file_path + 'f4.txt', out_file_path + 'f34.txt']
py_op_34 = PythonOperator(task_id='s34', python_callable=process_files,
                                dag=dag, op_args=py_op_34_args)

py_op_1234_args = [out_file_path + 'f12.txt', out_file_path + 'f34.txt', out_file_path + 'f1234.txt']
py_op_1234 = PythonOperator(task_id='s1234', python_callable=process_files,
                                dag=dag, op_args=py_op_1234_args)

py_op_1234.set_upstream([py_op_12, py_op_34])

