"""Take random sample of job titles from list of aggregated counts"""
import logging
import os

from airflow.operators import BaseOperator

from skills_utils.time import datetime_to_quarter
from skills_utils.fs import check_create_folder
from skills_ml.algorithms.file_sampler import sampler
from config import config

from utils.dags import QuarterlySubDAG


def define_job_title_sample(main_dag_name):
    dag = QuarterlySubDAG(main_dag_name, 'job_title_sample')

    output_folder = config.get('output_folder', 'output')
    if not os.path.isdir(output_folder):
        os.mkdir(output_folder)

    SAMPLENUM = 100

    class JobTitleSampleOperator(BaseOperator):
        def execute(self, context):
            quarter = datetime_to_quarter(context['execution_date'])

            cleaned_count_filename = '{}/{}/{}.csv'.format(
                output_folder,
                config.get('cleaned_geo_title_count'),
                quarter
            )

            cleaned_rollup_filename = '{}/{}/{}.csv'.format(
                output_folder,
                config.get('cleaned_title_count'),
                quarter
            )

            sampled_count_filename = '{}/sampled_geo_title_count/{}.csv'.format(
                output_folder,
                quarter
            )

            sampled_rollup_filename = '{}/sampled_title_count/{}.csv'.format(
                output_folder,
                quarter
            )

            geo_sample = sampler.reservoir_sample(SAMPLENUM, cleaned_count_filename, 12)
            count_sample = sampler.reservoir_sample(SAMPLENUM, cleaned_rollup_filename, 12)

            check_create_folder(sampled_count_filename)
            with open(sampled_count_filename, 'w') as sample_file:
                for line in geo_sample:
                    sample_file.write(line)

            check_create_folder(sampled_rollup_filename)
            with open(sampled_rollup_filename, 'w') as sample_file:
                for line in count_sample:
                    sample_file.write(line)

            logging.info(
                'Sampled %s job title rows for %s',
                SAMPLENUM,
                quarter,
            )

    JobTitleSampleOperator(task_id='jobtitle_sample', dag=dag)

    return dag
