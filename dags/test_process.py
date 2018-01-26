from collections.abc import MutableMapping
from datetime import timedelta
import s3fs
import json
import logging
import os
import tempfile
from skills_utils.s3 import download
from skills_utils.time import quarter_to_daterange
from config import config
from skills_utils.time import datetime_to_quarter
from airflow.hooks import S3Hook
from skills_ml.datasets.job_postings import job_postings_highmem
from airflow.operators import BaseOperator
from skills_ml.algorithms.string_cleaners import NLPTransforms
from skills_ml.algorithms.jobtitle_cleaner.clean import JobTitleStringClean
from skills_ml.algorithms.occupation_classifiers.classifiers import \
    Classifier
from skills_ml.algorithms.corpus_creators.basic import SimpleCorpusCreator
from skills_ml.algorithms.job_geography_queriers import JobCBSAFromGeocodeQuerier
from utils.dags import QuarterlySubDAG
from skills_ml.algorithms.skill_extractors.freetext\
    import OccupationScopedSkillExtractor, ExactMatchSkillExtractor
from skills_ml.algorithms.geocoders.cbsa import S3CachedCBSAFinder
import pandas


class S3BackedJsonDict(MutableMapping):
    def __init__(self, *args, **kw):
        self.path = kw.pop('path') + '.json'
        self.fs = s3fs.S3FileSystem()

        if self.fs.exists(self.path):
            with self.fs.open(self.path, 'rb') as f:
                data = f.read().decode('utf-8') or '{}'
                self._storage = json.loads(data)
        else:
            self._storage = dict()

        logging.info('Loaded storage with %s keys', len(self))

    def __getitem__(self, key):
        return self._storage[key]

    def __iter__(self):
        return iter(self._storage)

    def __len__(self):
        return len(self._storage)

    def __delitem__(self, key):
        del self._storage[key]

    def __setitem__(self, key, value):
        self._storage[key] = value

    def __keytransform__(self, key):
        return key

    def __contains__(self, key):
        return key in self._storage

    def save(self):
        logging.info('Saving storage of length %s to %s', len(self), self.path)
        with self.fs.open(self.path, 'wb') as f:
            f.write(json.dumps(self._storage).encode('utf-8'))


def compute_job_posting_properties(job_postings_generator, property_name, property_computer):
    caches = {}
    misses = 0
    hits = 0
    for number, job_posting in enumerate(job_postings_generator):
        if number % 1000 == 0:
            logging.info(
                'Computation of job posting properties for %s on posting %s',
                property_name,
                number
            )
        date_posted = job_posting.get('datePosted', 'unknown')
        if date_posted not in caches:
            caches[date_posted] = S3BackedJsonDict(
                path='/'.join([
                    config['job_posting_computed_properties']['s3_path'],
                    property_name,
                    date_posted
                ])
            )
        if job_posting['id'] not in caches[date_posted]:
            caches[date_posted][job_posting['id']] = property_computer(job_posting)
            misses += 1
        else:
            hits += 1
    for cache in caches.values():
        cache.save()
    logging.info(
        'Computation of job posting properties for %s complete. %s hits, %s misses',
        property_name,
        hits,
        misses
    )


class QuarterlyJobPostingMixin(object):
    def job_postings_generator(self, context):
        quarter = datetime_to_quarter(context['execution_date'])
        s3_conn = S3Hook().get_conn()

        for job_posting_string in job_postings_highmem(
            s3_conn,
            quarter,
            config['job_postings']['s3_path']
        ):
            yield json.loads(job_posting_string)


class TitleCleanPhaseOne(BaseOperator, QuarterlyJobPostingMixin):
    def execute(self, context):
        title_func = NLPTransforms().title_phase_one

        def func(job_posting):
            return title_func(job_posting['title'])

        compute_job_posting_properties(
            job_postings_generator=self.job_postings_generator(context),
            property_name='title_clean_phase_one',
            property_computer=func
        )


class TitleCleanPhaseTwo(BaseOperator, QuarterlyJobPostingMixin):
    def execute(self, context):
        title_func = JobTitleStringClean().clean_title

        def func(job_posting):
            return title_func(job_posting['title'])

        compute_job_posting_properties(
            job_postings_generator=self.job_postings_generator(context),
            property_name='title_clean_phase_two',
            property_computer=func
        )


class ClassifyCommon(BaseOperator, QuarterlyJobPostingMixin):
    def execute(self, context):
        s3_conn = S3Hook().get_conn()
        with tempfile.TemporaryDirectory() as temp_dir:
            common_classifier = Classifier(
                s3_conn=s3_conn,
                classifier_id='ann_0614',
                classify_kwargs={'mode': 'common'},
                temporary_directory=temp_dir
            )
            corpus_creator = SimpleCorpusCreator()

            def func(job_posting):
                return common_classifier.classify(corpus_creator._transform(job_posting))

            compute_job_posting_properties(
                job_postings_generator=self.job_postings_generator(context),
                property_name='soc_common',
                property_computer=func
            )


class ClassifyTop(BaseOperator, QuarterlyJobPostingMixin):
    def execute(self, context):
        s3_conn = S3Hook().get_conn()
        with tempfile.TemporaryDirectory() as temp_dir:
            common_classifier = Classifier(
                s3_conn=s3_conn,
                classifier_id='ann_0614',
                classify_kwargs={'mode': 'top'},
                temporary_directory=temp_dir
            )
            corpus_creator = SimpleCorpusCreator()

            def func(job_posting):
                return common_classifier.classify(corpus_creator._transform(job_posting))

            compute_job_posting_properties(
                job_postings_generator=self.job_postings_generator(context),
                property_name='soc_top',
                property_computer=func
            )


class ClassifyGiven(BaseOperator, QuarterlyJobPostingMixin):
    def execute(self, context):
        def func(job_posting):
            return job_posting.get('onet_soc_code', '99-9999.00')

        compute_job_posting_properties(
            job_postings_generator=self.job_postings_generator(context),
            property_name='soc_given',
            property_computer=func
        )


class SocScopedExactMatchSkillCounts(BaseOperator, QuarterlyJobPostingMixin):
    def execute(self, context):
        s3_conn = S3Hook().get_conn()
        skills_file = 'skills_master_table.tsv'
        with tempfile.TemporaryDirectory() as temp_dir:
            skills_filename = '{}/{}'.format(temp_dir, skills_file)

            if not os.path.isfile(skills_filename):
                download(
                  s3_conn=s3_conn,
                  out_filename=skills_filename,
                  s3_path=config['output_tables']['s3_path'] + skills_file
                )
            corpus_creator = SimpleCorpusCreator()
            skill_extractor = OccupationScopedSkillExtractor(skill_lookup_path=skills_filename)

            def func(job_posting):
                return skill_extractor.document_skill_counts(
                  soc_code=job_posting.get('onet_soc_code', '99-9999.00'),
                  document=corpus_creator._transform(job_posting)
                )
            compute_job_posting_properties(
                job_postings_generator=self.job_postings_generator(context),
                property_name='skill_counts_soc_scoped',
                property_computer=func
            )


class ExactMatchSkillCounts(BaseOperator, QuarterlyJobPostingMixin):
    def execute(self, context):
        s3_conn = S3Hook().get_conn()
        skills_file = 'skills_master_table.tsv'
        with tempfile.TemporaryDirectory() as temp_dir:
            skills_filename = '{}/{}'.format(temp_dir, skills_file)

            if not os.path.isfile(skills_filename):
                download(
                  s3_conn=s3_conn,
                  out_filename=skills_filename,
                  s3_path=config['output_tables']['s3_path'] + skills_file
                )
            corpus_creator = SimpleCorpusCreator()
            skill_extractor = ExactMatchSkillExtractor(skill_lookup_path=skills_filename)

            def func(job_posting):
                return skill_extractor.document_skill_counts(
                  document=corpus_creator._transform(job_posting)
                )
            compute_job_posting_properties(
                job_postings_generator=self.job_postings_generator(context),
                property_name='skill_counts_exact_match',
                property_computer=func
            )


class PostingIdPresent(BaseOperator, QuarterlyJobPostingMixin):
    def execute(self, context):
        def func(job_posting):
            return 1

        compute_job_posting_properties(
            job_postings_generator=self.job_postings_generator(context),
            property_name='posting_id_present',
            property_computer=func
        )


class CBSAandStateFromGeocode(BaseOperator, QuarterlyJobPostingMixin):
    def execute(self, context):
        s3_conn = S3Hook().get_conn()
        geo_querier = JobCBSAFromGeocodeQuerier(
            cbsa_results=S3CachedCBSAFinder(
                s3_conn=s3_conn,
                cache_s3_path=config['cbsa_lookup']['s3_path']
            ).all_cached_cbsa_results
        )

        def func(job_posting):
            return geo_querier.query(json.dumps(job_posting))

        compute_job_posting_properties(
            job_postings_generator=self.job_postings_generator(context),
            property_name='cbsa_and_state_from_geocode',
            property_computer=func
        )


def define_test(main_dag_name):
    dag = QuarterlySubDAG(main_dag_name, 'test_process')
    TitleCleanPhaseOne(task_id='title_clean_phase_one', dag=dag)
    TitleCleanPhaseTwo(task_id='title_clean_phase_two', dag=dag)
    ClassifyCommon(task_id='soc_common', dag=dag)
    ClassifyTop(task_id='soc_top', dag=dag)
    ClassifyGiven(task_id='soc_given', dag=dag)
    SocScopedExactMatchSkillCounts(task_id='skill_counts_exact_match_soc_scoped', dag=dag)
    ExactMatchSkillCounts(task_id='skill_counts_exact_match', dag=dag)
    PostingIdPresent(task_id='posting_id_present', dag=dag)
    CBSAandStateFromGeocode(task_id='cbsa_and_state_from_geocode', dag=dag)
    return dag


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


def job_posting_computed_properties(s3_conn, quarter, property_names):
    start_date, end_date = quarter_to_daterange(quarter)
    for included_date in daterange(start_date, end_date):
        datestring = included_date.strftime("%Y-%m-%d")
        cache = S3BackedJsonDict(
            path='/'.join([
                config['job_posting_computed_properties']['s3_path'],
                property_names[0],
                datestring
            ])
        )
        import pdb
        pdb.set_trace()

class TitleCountAggregate(BaseOperator):
    left = ['title_clean_phase_one']
    right = ['posting_id_present']
    def execute(self, context):
        s3_conn = S3Hook().get_conn()
        quarter = datetime_to_quarter(context['execution_date'])
        job_posting_computed_properties(s3_conn, quarter, self.left + self.right)
