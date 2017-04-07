"""Open Skills Project data flow"""
from datetime import datetime

from airflow import DAG
from airflow.operators.subdag_operator import SubDagOperator
from dags.api_sync_v1 import define_api_sync
from dags.partner_etl import define_partner_etl
from dags.partner_update import define_partner_update
from dags.onet_extract import define_onet_extract
from dags.elasticsearch_normalizer import define_normalizer_index
from dags.title_count import define_title_counts
from dags.job_label import define_job_label
from dags.job_vectorize import define_job_vectorize
from dags.skill_tag import define_skill_tag
from dags.job_title_sample import define_job_title_sample
from dags.tabular_upload import define_tabular_upload

from config import config

default_args = {
    'depends_on_past': False,
    'start_date': datetime(2011, 1, 1),
    'email': config.get('airflow_contacts', [])
}


MAIN_DAG_NAME = 'open_skills_master'

api_sync_dag = define_api_sync(MAIN_DAG_NAME)
partner_etl_dag = define_partner_etl(MAIN_DAG_NAME)
partner_update_dag = define_partner_update(MAIN_DAG_NAME)
onet_extract_dag = define_onet_extract(MAIN_DAG_NAME)
normalizer_index_dag = define_normalizer_index(MAIN_DAG_NAME)
title_count_dag = define_title_counts(MAIN_DAG_NAME)
job_label_dag = define_job_label(MAIN_DAG_NAME)
job_vectorize_dag = define_job_vectorize(MAIN_DAG_NAME)
skill_tag_dag = define_skill_tag(MAIN_DAG_NAME)
job_title_sample_dag = define_job_title_sample(MAIN_DAG_NAME)
tabular_upload_dag = define_tabular_upload(MAIN_DAG_NAME)

dag = DAG(
    dag_id=MAIN_DAG_NAME,
    schedule_interval='0 0 1 */3 *',
    default_args=default_args
)

api_sync = SubDagOperator(
    subdag=api_sync_dag,
    task_id='api_v1_sync',
    dag=dag,
)

partner_etl = SubDagOperator(
    subdag=partner_etl_dag,
    task_id='partner_etl',
    dag=dag,
)

partner_update = SubDagOperator(
    subdag=partner_update_dag,
    task_id='partner_update',
    dag=dag,
)

onet_extract = SubDagOperator(
    subdag=onet_extract_dag,
    task_id='onet_extract',
    dag=dag,
)

normalizer_index = SubDagOperator(
    subdag=normalizer_index_dag,
    task_id='normalize_elasticsearch',
    dag=dag,
)

title_count = SubDagOperator(
    subdag=title_count_dag,
    task_id='title_count',
    dag=dag,
)

job_label = SubDagOperator(
    subdag=job_label_dag,
    task_id='job_label',
    dag=dag,
)

job_vectorize = SubDagOperator(
    subdag=job_vectorize_dag,
    task_id='job_vectorize',
    dag=dag,
)

skill_tag = SubDagOperator(
    subdag=skill_tag_dag,
    task_id='skill_tag',
    dag=dag,
)

job_title_sample = SubDagOperator(
    subdag=job_title_sample_dag,
    task_id='job_title_sample',
    dag=dag,
)

tabular_upload = SubDagOperator(
    subdag=tabular_upload_dag,
    task_id='tabular_upload',
    dag=dag
)

partner_etl.set_upstream(partner_update)
api_sync.set_upstream(title_count)
api_sync.set_upstream(onet_extract)
api_sync.set_upstream(normalizer_index)
normalizer_index.set_upstream(partner_etl)
normalizer_index.set_upstream(onet_extract)
title_count.set_upstream(partner_etl)
job_label.set_upstream(partner_etl)
job_vectorize.set_upstream(partner_etl)
skill_tag.set_upstream(partner_etl)
skill_tag.set_upstream(onet_extract)
job_title_sample.set_upstream(title_count)
tabular_upload.set_upstream(title_count)
