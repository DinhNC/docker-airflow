from airflow.models import TaskInstance
from airflow.operators.sensors import ExternalTaskSensor
from airflow.utils.db import provide_session

class CustomTaskSensor(ExternalTaskSensor):

    @provide_session
    def poke(self, context, session=None):
        if self.execution_delta:
            dttm = context['execution_date'] - self.execution_delta
            serialized_dttm_filter = dttm.isoformat()

            self.log.info(
                'Poking for '
                '{self.external_dag_id}.'
                '{self.external_task_id} on '
                '{} ... '.format(serialized_dttm_filter, **locals()))
            TI = TaskInstance

            count_all = session.query(TI).filter(
                TI.dag_id == self.external_dag_id,
                TI.task_id == self.external_task_id,
                TI.execution_date >= dttm,
            ).count()
            count_filtered = session.query(TI).filter(
                TI.dag_id == self.external_dag_id,
                TI.task_id == self.external_task_id,
                TI.state.in_(self.allowed_states),
                TI.execution_date >= dttm,
            ).count()
            session.commit()

            if (count_all > 0):
                return count_all == count_filtered
            else:
                return True
        else:
            return super(CustomTaskSensor, self).poke(context)
