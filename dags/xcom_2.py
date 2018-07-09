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
task_id_push_file_to_xcom = 'push_file_to_xcom_'
task_id_push_sum_to_xcom = 'push_sum_to_xcom_'

# xcom methods
def push_list(key_name, list_to_push, **kwargs):
  kwargs['ti'].xcom_push(key=key_name, value=list_to_push)

def pull_list(task_ids, key_name, **kwargs):
  return kwargs['ti'].xcom_pull(task_ids=task_ids, key=key_name)

# file read-write methods
def read_list(file_path):
    with open(file_path) as file:
        lines = file.read().splitlines()

    return [int(line.strip()) for line in lines]

def write_list(file_path, lines):
    with open(file_path, mode='wt') as file:
        file.write('\n'.join([str(line) for line in lines]))

# utility methods
def add_lists(list_1_name, list_2_name):
    sum_list = [(ele_1 + ele_2) for ele_1, ele_2 in list(zip(list_1, list_2))]
    return sum_list

def extract_file_name(file_path):
  file_name = file_path[file_path.rfind('/'):][1:]
  return file_name

# complete tasks
def push_file_to_xcom(file_path, **kwargs):
  file_name = extract_file_name(file_path)
  list_to_push = read_list(file_path)
  push_list(file_name, list_to_push, kwargs)

def push_sum_to_xcom(file_path_1, file_path_2, **kwargs):
  file_name_1 = extract_file_name(file_path_1)
  file_name_2 = extract_file_name(file_path_2)
  file_name_12 = file_name_1 + file_name_2

  task_ids_1 = [task_id_push_file_to_xcom + file_name_1]
  task_ids_2 = [task_id_push_file_to_xcom + file_name_2]

  list_1 = pull_list(task_ids_1, file_name_1, kwargs)
  list_2 = pull_list(task_ids_2, file_name_2, kwargs)
  list_sum = add_lists(list_1, list_2)

  push_list(file_name_12, list_sum, kwargs)

def write_xcom_to_file(task_ids, file_path, **kwargs):
  file_name = extract_file_name(file_path)
  list_to_write = pull_list(task_ids, file_name, kwargs)
  write_list(file_path, list_to_write)


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