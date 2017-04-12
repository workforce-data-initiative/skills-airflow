"""Uploads tabular datasets to a public bucket, along with a README"""
from airflow.operators import BaseOperator
from utils.dags import QuarterlySubDAG
from config import config
from skills_utils.s3 import download, upload
from skills_utils.time import datetime_to_quarter
from airflow.hooks import S3Hook
import os

FOLDER_READMES = {
    'cleaned_geo_title_count': """Counts of job posting title occurrences by CBSA.

Job titles are cleaned by lowercasing, removing punctuation, and removing city and state names.""",
    'cleaned_title_count': """Counts of job posting title occurrences.

Job titles are cleaned by lowercasing, removing punctuation, and removing city and state names.""",
    'geo_title_count': """Counts of job posting title occurrences by CBSA.

Job titles are cleaned by lowercasing and removing punctuation.""",
    'title_count': """Counts of job posting title occurrences.

Job titles are cleaned by lowercasing and removing punctuation.""",
}

QUARTERLY_NOTE = """Each file contains the data for job postings active in one quarter. If a job posting was active in two quarters, it will be present in the counts of both quarters."""


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

            for folder_name, readme_string in FOLDER_READMES.items():
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
                for folder_name, readme_string in FOLDER_READMES.items():
                    readme_file.write("###" + folder_name + "###\n\n")
                    readme_file.write(readme_string + "\n\n\n")
            upload(s3_conn, base_readme_filepath, upload_s3_path)

    TabularUploadOperator(task_id='tabular_upload', dag=dag)

    return dag
