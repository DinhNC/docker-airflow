import airflow
import airflow.utils.helpers
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from custom_task_sensor import CustomTaskSensor
from airflow.models import DAG

from datetime import datetime, timedelta


# args and params
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
    "catchup": False,
    "trigger_rule": 'all_done'
}
did_parent = 'importer_parent_blocking_v2'
did_tid_list = [('importer_child_v1_db_1', 'importer_v1_db_1'),
                ('importer_child_v1_db_2', 'importer_v1_db_2'),
                ('importer_child_v1_db_3', 'importer_v1_db_3')]
did_last_tid_sid_list = [('importer_child_v1_db_1', 'distcp_db_1', 'sensor_v1_db_1'),
                         ('importer_child_v1_db_2', 'distcp_db_2', 'sensor_v1_db_2'),
                         ('importer_child_v1_db_3', 'distcp_db_3', 'sensor_v1_db_3')]


def create_trigger_dag_operator(dag, trigger_dag_id, task_id):
    trigger_op = TriggerDagRunOperator(task_id=task_id,
                                       trigger_dag_id=trigger_dag_id,
                                       python_callable=lambda context, dag_run_obj : dag_run_obj,
                                       dag=dag)
    return trigger_op

def create_task_sensor_operator(dag, external_dag_id, external_task_id, sensor_id):
    from airflow.utils.state import State
    task_sensor_op = CustomTaskSensor(task_id=sensor_id,
                                      external_dag_id=external_dag_id,
                                      external_task_id=external_task_id,
                                      allowed_states=[State.SUCCESS, State.SHUTDOWN, State.FAILED, State.UPSTREAM_FAILED, State.SKIPPED],
                                      execution_delta=timedelta(minutes=10),
                                      retries=3,
                                      poke_interval=5,
                                      dag=dag)
    return task_sensor_op

def create_operators_chain(dag, did_tid_list, did_last_tid_sid_list):
    trigger_ops = [create_trigger_dag_operator(dag, did, tid) for did, tid in did_tid_list]
    sensor_ops = [create_task_sensor_operator(dag, did, last_tid, sid) for did, last_tid, sid in did_last_tid_sid_list]

    zip_ops = list(zip(trigger_ops, sensor_ops))
    # flat_ops = list(sum(zip_ops, ()))
    flat_ops = [op for tuple in zip_ops for op in tuple]

    # chain triggerdagrun-operators & external-task-sensors together
    airflow.utils.helpers.chain(*flat_ops)
    return flat_ops


dag = DAG(dag_id=did_parent,
          default_args=default_args,
          schedule_interval=None)

trigger_ops = create_operators_chain(dag, did_tid_list, did_last_tid_sid_list)
