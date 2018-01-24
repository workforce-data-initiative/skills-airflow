"""Open Skills Project data flow"""
from datetime import datetime

from airflow import DAG
from airflow.operators.subdag_operator import SubDagOperator
from dags.api_sync_v1 import define_api_sync
from dags.partner_etl import define_partner_etl
from dags.partner_quarterly import define_partner_quarterly
from dags.onet_extract import define_onet_extract
from dags.elasticsearch_normalizer import define_normalizer_index
from dags.title_count import define_title_counts
from dags.soc_count import define_soc_counts
from dags.job_label import define_job_label
from dags.job_vectorize import define_job_vectorize
from dags.skill_tag import define_skill_tag
from dags.tabular_upload import define_tabular_upload
from dags.geocode import define_geocode
from dags.test_process import define_test

from config import config

default_args = {
    'depends_on_past': False,
    'start_date': datetime(2011, 1, 1),
    'email': config.get('airflow_contacts', [])
}


MAIN_DAG_NAME = 'open_skills_master'

api_sync_dag = define_api_sync(MAIN_DAG_NAME)
partner_etl_dag = define_partner_etl(MAIN_DAG_NAME)
partner_quarterly_dag = define_partner_quarterly(MAIN_DAG_NAME)
onet_extract_dag = define_onet_extract(MAIN_DAG_NAME)
normalizer_index_dag = define_normalizer_index(MAIN_DAG_NAME)
geocode_dag = define_geocode(MAIN_DAG_NAME)
title_count_dag = define_title_counts(MAIN_DAG_NAME)
soc_count_dag = define_soc_counts(MAIN_DAG_NAME)
job_label_dag = define_job_label(MAIN_DAG_NAME)
job_vectorize_dag = define_job_vectorize(MAIN_DAG_NAME)
skill_tag_dag = define_skill_tag(MAIN_DAG_NAME)
tabular_upload_dag = define_tabular_upload(MAIN_DAG_NAME)
test_dag = define_test(MAIN_DAG_NAME)

dag = DAG(
    dag_id=MAIN_DAG_NAME,
    schedule_interval='0 0 1 */3 *',
    default_args=default_args
)

api_sync = SubDagOperator(
    subdag=api_sync_dag,
    task_id='api_v1_sync',
    priority_weight=1,
    queue='subdag',
    dag=dag,
)

partner_etl = SubDagOperator(
    subdag=partner_etl_dag,
    task_id='partner_etl',
    priority_weight=1,
    queue='subdag',
    dag=dag,
)

partner_quarterly = SubDagOperator(
    subdag=partner_quarterly_dag,
    task_id='partner_quarterly',
    priority_weight=1,
    queue='subdag',
    dag=dag,
)

onet_extract = SubDagOperator(
    subdag=onet_extract_dag,
    task_id='onet_extract',
    priority_weight=1,
    queue='subdag',
    dag=dag,
)

normalizer_index = SubDagOperator(
    subdag=normalizer_index_dag,
    task_id='normalize_elasticsearch',
    priority_weight=1,
    queue='subdag',
    dag=dag,
)

geocode = SubDagOperator(
    subdag=geocode_dag,
    task_id='geocode',
    priority_weight=1,
    queue='subdag',
    dag=dag,
)

title_count = SubDagOperator(
    subdag=title_count_dag,
    task_id='title_count',
    priority_weight=1,
    queue='subdag',
    dag=dag,
)

soc_count = SubDagOperator(
    subdag=soc_count_dag,
    task_id='soc_count',
    priority_weight=1,
    queue='subdag',
    dag=dag
)

job_label = SubDagOperator(
    subdag=job_label_dag,
    task_id='job_label',
    priority_weight=1,
    queue='subdag',
    dag=dag,
)

job_vectorize = SubDagOperator(
    subdag=job_vectorize_dag,
    task_id='job_vectorize',
    priority_weight=1,
    queue='subdag',
    dag=dag,
)

skill_tag = SubDagOperator(
    subdag=skill_tag_dag,
    task_id='skill_tag',
    priority_weight=1,
    queue='subdag',
    dag=dag,
)

tabular_upload = SubDagOperator(
    subdag=tabular_upload_dag,
    task_id='tabular_upload',
    priority_weight=1,
    queue='subdag',
    dag=dag
)

test_process = SubDagOperator(
    subdag=test_dag,
    task_id='test',
    priority_weight=1,
    queue='subdag',
    dag=dag
)

partner_etl.set_upstream(partner_quarterly)
api_sync.set_upstream(title_count)
api_sync.set_upstream(onet_extract)
api_sync.set_upstream(normalizer_index)
normalizer_index.set_upstream(partner_etl)
normalizer_index.set_upstream(onet_extract)
geocode.set_upstream(partner_etl)
title_count.set_upstream(geocode)
soc_count.set_upstream(geocode)
job_label.set_upstream(partner_etl)
job_vectorize.set_upstream(partner_etl)
skill_tag.set_upstream(partner_etl)
skill_tag.set_upstream(onet_extract)
tabular_upload.set_upstream(title_count)
test_process.set_upstream(partner_etl)
