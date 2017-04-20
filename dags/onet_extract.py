"""Workflow to create skills, job titles, and importances based on ONET data"""
import csv
import io
import os

from airflow.hooks import S3Hook
from airflow.operators import BaseOperator

from config import config
from skills_ml.datasets import OnetCache
from skills_utils.es import basic_client
from skills_utils.hash import md5
from skills_utils.s3 import split_s3_path, upload

from skills_ml.algorithms.skill_extractors.onet_ksas import OnetSkillExtractor
from skills_ml.algorithms.skill_importance_extractors.onet import OnetSkillImportanceExtractor
from skills_ml.algorithms.title_extractors.onet import OnetTitleExtractor
from skills_ml.algorithms.elasticsearch_indexers.job_titles_master import JobTitlesMasterIndexer

from utils.dags import QuarterlySubDAG


def define_onet_extract(main_dag_name):
    dag = QuarterlySubDAG(main_dag_name, 'onet_extract')

    output_folder = config.get('output_folder', 'output')
    if not os.path.isdir(output_folder):
        os.mkdir(output_folder)
    skills_filename = '{}/skills_master_table.tsv'.format(output_folder)
    titles_filename = '{}/job_titles_master_table.tsv'.format(output_folder)
    skill_importance_filename = '{}/ksas_importance.tsv'.format(output_folder)

    class SkillExtractOperator(BaseOperator):
        def execute(self, context):
            conn = S3Hook().get_conn()
            skill_extractor = OnetSkillExtractor(
                onet_source=OnetCache(
                    conn,
                    cache_dir=config['onet']['cache_dir'],
                    s3_path=config['onet']['s3_path'],
                ),
                output_filename=skills_filename,
                hash_function=md5
            )
            skill_extractor.run()
            upload(conn, skills_filename, config['output_tables']['s3_path'])

    class TitleExtractOperator(BaseOperator):
        def execute(self, context):
            conn = S3Hook().get_conn()
            title_extractor = OnetTitleExtractor(
                onet_source=OnetCache(
                    conn,
                    cache_dir=config['onet']['cache_dir'],
                    s3_path=config['onet']['s3_path'],
                ),
                output_filename=titles_filename,
                hash_function=md5
            )
            title_extractor.run()
            upload(conn, titles_filename, config['output_tables']['s3_path'])

    class SkillImportanceOperator(BaseOperator):
        def execute(self, context):
            conn = S3Hook().get_conn()
            skill_extractor = OnetSkillImportanceExtractor(
                onet_source=OnetCache(
                    conn,
                    cache_dir=config['onet']['cache_dir'],
                    s3_path=config['onet']['s3_path'],
                ),
                output_filename=skill_importance_filename,
                hash_function=md5
            )
            skill_extractor.run()
            upload(conn, skill_importance_filename, config['output_tables']['s3_path'])

    class IndexJobTitlesMasterOperator(BaseOperator):
        def execute(self, context):
            conn = S3Hook()
            input_bucket, input_prefix = split_s3_path(config['output_tables']['s3_path'])
            key = conn.get_key(
                '{}/{}'.format(input_prefix, titles_filename),
                bucket_name=input_bucket
            )
            text = key.get_contents_as_string().decode('utf-8')
            reader = csv.DictReader(io.StringIO(text), delimiter='\t')
            JobTitlesMasterIndexer(
                s3_conn=conn.get_conn(),
                es_client=basic_client(),
                job_title_generator=reader,
                alias_name=config['normalizer']['titles_master_index_name']
            ).replace()

    skills = SkillExtractOperator(task_id='skill_extract', dag=dag)
    titles = TitleExtractOperator(task_id='title_extract', dag=dag)
    skill_importance = SkillImportanceOperator(task_id='skill_importance', dag=dag)
    index_titles = IndexJobTitlesMasterOperator(task_id='job_titles_master', dag=dag)

    index_titles.set_upstream(titles)

    return dag
