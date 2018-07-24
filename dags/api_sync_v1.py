"""Workflow to sync saved CSV output data
to version 1 of the open skills API database
"""
import os
from datetime import datetime
import alembic.config

from airflow import DAG
from airflow.operators import BaseOperator

from config import config
from utils.db import get_apiv1_dbengine as get_db
from skills_ml.storage import S3Store

from api_sync.v1 import \
    load_jobs_master, \
    load_alternate_titles, \
    load_jobs_unusual_titles, \
    load_skills_master, \
    load_skills_importance

default_args = {
    'depends_on_past': False,
    'start_date': datetime(2011, 1, 1),
}


dag = DAG('api_v1_sync', default_args=default_args)

output_folder = config.get('output_folder', 'output')

table_files = {
    'jobs_master': 'job_titles_master_table.tsv',
    'skills_master': 'skills_master_table.tsv',
    'interesting_jobs': 'interesting_job_titles.tsv',
    'skill_importance': 'ksas_importance.tsv',
}


storage = S3Store('open-skills-public/pipeline/tables')


def full_path(filename):
    output_folder = os.environ.get('OUTPUT_FOLDER', None)
    if not output_folder:
        output_folder = config.get('output_folder', 'output')
    return os.path.join(output_folder, filename)


class JobMaster(BaseOperator):
    def execute(self, context):
        engine = get_db()
        load_jobs_master(storage, table_files['jobs_master'], engine)


class SkillMaster(BaseOperator):
    def execute(self, context):
        engine = get_db()
        load_skills_master(storage, table_files['skills_master'], engine)


class JobAlternateTitles(BaseOperator):
    def execute(self, context):
        engine = get_db()
        load_alternate_titles(storage, table_files['jobs_master'], engine)


class JobUnusualTitles(BaseOperator):
    def execute(self, context):
        engine = get_db()
        load_jobs_unusual_titles(storage, table_files['interesting_jobs'], engine)


class SkillImportance(BaseOperator):
    def execute(self, context):
        engine = get_db()
        load_skills_importance(storage, table_files['skill_importance'], engine)


class SchemaUpgrade(BaseOperator):
    def execute(self, context):
        alembic.config.main(argv=['--raiseerr', 'upgrade', 'head'])


schema_upgrade = SchemaUpgrade(task_id='schema_upgrade', dag=dag)
job_master = JobMaster(task_id='job_master', dag=dag)
skill_master = SkillMaster(task_id='skill_master', dag=dag)
alternate_titles = JobAlternateTitles(task_id='alternate_titles', dag=dag)
unusual_titles = JobUnusualTitles(task_id='unusual_titles', dag=dag)
skill_importance = SkillImportance(task_id='skill_importance', dag=dag)

alternate_titles.set_upstream(job_master)
unusual_titles.set_upstream(job_master)
skill_importance.set_upstream(job_master)
skill_importance.set_upstream(skill_master)

all_tasks = [
    job_master,
    skill_master,
    alternate_titles,
    unusual_titles,
    skill_importance
]
for task in all_tasks:
    task.set_upstream(schema_upgrade)
