"""Ingest job listing data from partners, converting into common schema"""
from datetime import datetime

from airflow import DAG
from airflow.operators import BaseOperator

default_args = {
    'depends_on_past': False,
    'start_date': datetime(2011, 1, 1),
}


def define_partner_etl(main_dag_name):
    dag = DAG(
        '{}.partner_etl'.format(main_dag_name),
        schedule_interval='0 0 1 */3 *',
        default_args=default_args
    )

    class Virginia(BaseOperator):
        def execute(self, context):
            pass

    class CareerBuilder(BaseOperator):
        def execute(self, context):
            pass

    va = Virginia(task_id='va', dag=dag)
    cb = CareerBuilder(task_id='cb', dag=dag)

    return dag
