"""Uploads tabular datasets to a public bucket, along with a README"""
from airflow.operators import BaseOperator
from utils.dags import QuarterlySubDAG
from config import config
from skills_utils.s3 import download, upload
from skills_utils.time import datetime_to_quarter
from airflow.hooks import S3Hook
import os

table_config = config['output_tables']

COMMON_TITLE_AGG_INFO = """Includes:
- The top ONET skills (KSATs) extracted from the job postings
    of the given job title
- The top predicted ONET SOC codes from two different in-development
    versions of our classifier based on job posting content
    of the given job title
- The top ONET SOC codes given to us by the data partner
    for job postings of the given job title
"""

folder_readmes = {}
folder_readmes[table_config['cleaned_geo_title_count_dir']] = """
Counts of job posting title occurrences by CBSA.

{agg_info}

Job titles are cleaned by lowercasing, removing punctuation, and removing city and state names."""\
    .format(agg_info=COMMON_TITLE_AGG_INFO)

folder_readmes[table_config['cleaned_title_count_dir']] = """
Counts of job posting title occurrences.

{agg_info}

Job titles are cleaned by lowercasing, removing punctuation, and removing city and state names."""\
    .format(agg_info=COMMON_TITLE_AGG_INFO)

folder_readmes[table_config['geo_title_count_dir']] = """
Counts of job posting title occurrences by CBSA.

{agg_info}

Job titles are cleaned by lowercasing and removing punctuation."""\
    .format(agg_info=COMMON_TITLE_AGG_INFO)

folder_readmes[table_config['title_count_dir']] = """
Counts of job posting title occurrences.

{agg_info}

Job titles are cleaned by lowercasing and removing punctuation."""\
    .format(agg_info=COMMON_TITLE_AGG_INFO)

folder_readmes[table_config['geo_soc_common_count_dir']] = """
Job postings per SOC code, by CBSA.

SOC code inferred by 'common match' method
"""

folder_readmes[table_config['soc_common_count_dir']] = """
Job postings per SOC code

SOC code inferred by 'common match' method
"""

folder_readmes[table_config['geo_soc_top_count_dir']] = """
Job postings per SOC code, by CBSA.

SOC code inferred by 'top match' method
"""

folder_readmes[table_config['soc_top_count_dir']] = """
Job postings per SOC code

SOC code inferred by 'top match' method
"""

folder_readmes[table_config['geo_soc_given_count_dir']] = """
Job postings per SOC code, by CBSA.

SOC code given by data source
"""

folder_readmes[table_config['soc_given_count_dir']] = """
Job postings per SOC code

SOC code given by data source
"""

QUARTERLY_NOTE = """Each file contains the data for job postings active in one quarter.
If a job posting was active in two quarters,
it will be present in the counts of both quarters."""


def define_tabular_upload(main_dag_name):
    dag = QuarterlySubDAG(main_dag_name, 'tabular_upload')

    class TabularUploadOperator(BaseOperator):
        def execute(self, context):
            local_folder = config.get('output_folder', 'output')
            if not os.path.isdir(local_folder):
                os.mkdir(local_folder)
            source_s3_path = config['output_tables']['s3_path']
            upload_s3_path = config['tabular_uploads']['s3_path']

            s3_conn = S3Hook().get_conn()
            quarter = datetime_to_quarter(context['execution_date'])

            for folder_name, readme_string in folder_readmes.items():
                full_folder = '{}/{}'.format(local_folder, folder_name)
                if not os.path.isdir(full_folder):
                    os.mkdir(full_folder)
                data_filename = '{}.csv'.format(quarter)
                data_filepath = os.path.join(full_folder, data_filename)
                readme_filepath = os.path.join(full_folder, 'README.txt')
                with open(readme_filepath, 'w') as readme_file:
                    readme_file.write(readme_string + "\n" + QUARTERLY_NOTE)
                download(
                    s3_conn,
                    data_filepath,
                    os.path.join(source_s3_path, folder_name, data_filename)
                )
                upload_s3_folder = os.path.join(upload_s3_path, folder_name)
                upload(s3_conn, readme_filepath, upload_s3_folder)
                upload(s3_conn, data_filepath, upload_s3_folder)

            base_readme_filepath = os.path.join(local_folder, 'README.txt')
            with open(base_readme_filepath, 'w') as readme_file:
                readme_file.write("Open Skills Datasets\n\n")
                for folder_name, readme_string in folder_readmes.items():
                    readme_file.write("###" + folder_name + "###\n\n")
                    readme_file.write(readme_string + "\n\n\n")
            upload(s3_conn, base_readme_filepath, upload_s3_path)

    TabularUploadOperator(task_id='tabular_upload', dag=dag)

    return dag
