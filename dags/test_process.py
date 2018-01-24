from collections.abc import MutableMapping
import s3fs
import json
import logging
from config import config
from skills_utils.time import datetime_to_quarter
from airflow.hooks import S3Hook
from skills_ml.datasets.job_postings import job_postings_highmem
from airflow.operators import BaseOperator
from skills_ml.algorithms.string_cleaners import NLPTransforms
from skills_ml.algorithms.jobtitle_cleaner.clean import JobTitleStringClean
from skills_ml.algorithms.occupation_classifiers.classifiers import \
    Classifier, download_ann_classifier_files
from skills_ml.algorithms.corpus_creators.basic import SimpleCorpusCreator
from utils.dags import QuarterlySubDAG


class S3BackedJsonDict(MutableMapping):
    def __init__(self, *args, **kw):
        self.path = kw.pop('path')
        self.fs = s3fs.S3FileSystem()

        if self.fs.exists(self.path):
            with self.fs.open(self.path, 'rb') as f:
                self._storage = json.load(f)
        else:
            self._storage = dict()

    def __getitem__(self, key):
        return self._storage[key]

    def __iter__(self):
        return iter(self._storage)

    def __len__(self):
        return len(self._storage)

    def __del__(self):
        logging.info('Saving on del')
        with self.fs.open(self.path, 'wb') as f:
            json.dump(self._storage, f)


def compute_job_posting_properties(s3_conn, job_postings_generator, property_name, property_computer):
    caches = {}
    for job_posting in job_postings_generator:
        job_obj = json.loads(job_posting)
        date_posted = job_obj.get('datePosted', 'unknown')
        if not caches[date_posted]:
            caches[date_posted] = S3BackedJsonDict(
                path='/'.join([
                    config['job_posting_computed_properties']['s3_path'],
                    property_name,
                    date_posted
                ])
            )
        if job_obj['id'] not in caches[date_posted]:
            caches[date_posted][job_obj['id']] = property_computer(job_obj['title'])


class QuarterlyJobPostingMixin(object):
    def job_postings_generator(self, context):
        quarter = datetime_to_quarter(context['execution_date'])
        s3_conn = S3Hook().get_conn()

        return job_postings_highmem(
            s3_conn,
            quarter,
            config['job_postings']['s3_path']
        )


class TitleCleanPhaseOne(BaseOperator, QuarterlyJobPostingMixin):
    def execute(self, context):
        s3_conn = S3Hook().get_conn()
        title_func = NLPTransforms().title_phase_one

        def func(job_posting):
            return title_func(job_posting['title'])

        compute_job_posting_properties(
            s3_conn=s3_conn,
            job_postings_generator=self.job_postings_generator(context),
            property_name='title_clean_phase_one',
            property_computer=func
        )


class TitleCleanPhaseTwo(BaseOperator, QuarterlyJobPostingMixin):
    def execute(self, context):
        s3_conn = S3Hook().get_conn()
        title_func = JobTitleStringClean().clean_title

        def func(job_posting):
            return title_func(job_posting['title'])

        compute_job_posting_properties(
            s3_conn=s3_conn,
            job_postings_generator=self.job_postings_generator(context),
            property_name='title_clean_phase_two',
            property_computer=func
        )


class ClassifyCommon(BaseOperator, QuarterlyJobPostingMixin):
    def execute(self, context):
        s3_conn = S3Hook().get_conn()
        common_classifier = Classifier(
            s3_conn=s3_conn,
            classifier_id='ann_0614',
            classify_kwargs={'mode': 'common'},
        )
        corpus_creator = SimpleCorpusCreator()

        def func(job_posting):
            return common_classifier.classify(corpus_creator._transform(job_posting))

        compute_job_posting_properties(
            s3_conn=s3_conn,
            job_postings_generator=self.job_postings_generator(context),
            property_name='soc_common',
            property_computer=func
        )


def define_test(main_dag_name):
    dag = QuarterlySubDAG(main_dag_name, 'test_process')
    TitleCleanPhaseOne(task_id='title_clean_phase_one', dag=dag)
    return dag
