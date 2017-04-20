"""Compute a vectorized representation of each job listing"""
import csv
import logging

from airflow.hooks import S3Hook
from airflow.operators import BaseOperator

from skills_utils.time import datetime_to_quarter
from skills_ml.datasets import job_postings

from skills_ml.algorithms.corpus_creators.basic import GensimCorpusCreator
from skills_ml.algorithms.job_vectorizers.doc2vec_vectorizer import Doc2Vectorizer

from utils.dags import QuarterlySubDAG


def define_job_vectorize(main_dag_name):
    dag = QuarterlySubDAG(main_dag_name, 'job_vectorize')

    class JobVectorizeOperator(BaseOperator):
        def execute(self, context):
            s3_conn = S3Hook().get_conn()
            quarter = datetime_to_quarter(context['execution_date'])
            job_vector_filename = 'tmp/job_features_train_' + quarter + '.csv'
            with open(job_vector_filename, 'w') as outfile:
                writer = csv.writer(outfile, delimiter=',')
                job_postings_generator = job_postings(s3_conn, quarter)
                corpus_generator = GensimCorpusCreator().array_corpora(job_postings_generator)
                vectorized_job_generator = Doc2Vectorizer(model_name='gensim_doc2vec',
                                                          path='skills-private/model_cache/',
                                                          s3_conn=s3_conn).vectorize(corpus_generator)
                for vector in vectorized_job_generator:
                    writer.writerow(vector)
            logging.info('Done vecotrizing job postings to %s', job_vector_filename)

    JobVectorizeOperator(task_id='job_vectorize', dag=dag)

    return dag
