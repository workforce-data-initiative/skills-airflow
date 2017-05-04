from airflow import DAG
from operators.partner_snapshot import PartnerSnapshotOperator
from datetime import datetime
from config import config

from skills_ml.datasets.partner_updaters import USAJobsUpdater


default_args = {
    'depends_on_past': False,
    'start_date': datetime(2017, 5, 1),
}


def define_partner_nightly(main_dag_name):
    dag = DAG(
        dag_id='{}.partner_nightly'.format(main_dag_name),
        default_args=default_args,
        schedule_interval='@daily'
    )

    raw_jobs = config.get('raw_jobs_s3_paths', {})
    if not raw_jobs:
        return dag

    usa_jobs_credentials = config.get('usa_jobs_credentials', {})
    if not usa_jobs_credentials:
        return dag
    PartnerSnapshotOperator(
        task_id='usa_jobs_update',
        dag=dag,
        s3_prefix=raw_jobs['US'],
        updater_class=USAJobsUpdater,
        passthrough_kwargs=usa_jobs_credentials
    )

    return dag
