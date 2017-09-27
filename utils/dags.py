from datetime import datetime

from airflow import DAG


class QuarterlySubDAG(DAG):
    def __init__(self, main_dag_name, sub_dag_name, *args, **kwargs):
        if main_dag_name is None:
            full_dag_name = sub_dag_name
        else:
            full_dag_name = '{}.{}'.format(main_dag_name, sub_dag_name)
        default_args = {
            'depends_on_past': False,
            'start_date': datetime(2011, 1, 1),
        }
        super(QuarterlySubDAG, self).__init__(
            full_dag_name,
            schedule_interval='0 0 1 */3 *',
            default_args=default_args,
            max_active_runs=1
            *args, **kwargs
        )
