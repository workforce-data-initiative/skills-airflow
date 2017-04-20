"""Label occupational category of job listings"""
import csv
import logging

from airflow.hooks import S3Hook
from airflow.operators import BaseOperator

from utils.dags import QuarterlySubDAG

from skills_utils.time import datetime_to_quarter
from skills_ml.datasets import job_postings
from skills_ml.algorithms.corpus_creators.basic import JobCategoryCorpusCreator


def define_job_label(main_dag_name):
    dag = QuarterlySubDAG(main_dag_name, 'job_label')

    class JobLabelOperator(BaseOperator):
        def execute(self, context):
            s3_conn = S3Hook().get_conn()
            quarter = datetime_to_quarter(context['execution_date'])
            job_label_filename = 'tmp/job_label_train_'+quarter+'.csv'
            with open(job_label_filename, 'w') as outfile:
                writer = csv.writer(outfile, delimiter=',')
                job_postings_generator = job_postings(s3_conn, quarter)
                corpus_generator = JobCategoryCorpusCreator().label_corpora(job_postings_generator)
                for label in corpus_generator:
                    writer.writerow([label])
            logging.info('Done labeling job categories to %s', job_label_filename)

    JobLabelOperator(task_id='job_labeling', dag=dag)

    return dag
