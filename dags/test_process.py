from collections.abc import MutableMapping
from datetime import timedelta
from ABC import ABCMeta, abstractmethod
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
import numpy


class S3BackedJsonDict(MutableMapping):
    """A JSON-serializable dictionary that is backed by S3.

    Not guaranteed to be thread or multiprocess-safe - An attempt is made before saving to merge
    local changes with others that may have happened to the S3 file since this object was loaded,
    but this is not atomic.
    It is recommended that only one version of a given file be modified at a time.

    Will periodically save, but users must call .save() before closing to save all changes.

    Keyword Args:
        path (string): A full s3 path, including bucket (but without the .json suffix),
            used for saving the dictionary.
    """
    SAVE_EVERY_N_UPDATES = 1000

    def __init__(self, *args, **kw):
        self.path = kw.pop('path') + '.json'
        self.fs = s3fs.S3FileSystem()

        if self.fs.exists(self.path):
            with self.fs.open(self.path, 'rb') as f:
                data = f.read().decode('utf-8') or '{}'
                self._storage = json.loads(data)
        else:
            self._storage = dict()

        self.num_updates = 0
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
        self.num_updates += 1
        if self.num_updates % self.SAVE_EVERY_N_UPDATES == 0:
            logging.info('Auto-saving after %s updates', self.num_updates)
            self.save()

    def __keytransform__(self, key):
        return key

    def __contains__(self, key):
        return key in self._storage

    def save(self):
        logging.info('Attempting to save storage of length %s to %s', len(self), self.path)
        with self.fs.open(self.path, 'rb') as f:
            saved_string = f.read().decode('utf-8') or '{}'
            saved_data = json.loads(saved_string)
            logging.info(
                'Merging %s in-memory keys with %s stored keys. In-memory data takes priority',
                len(self),
                len(saved_data)
            )
            saved_data.update(self._storage)
            self._storage = saved_data
        with self.fs.open(self.path, 'wb') as f:
            f.write(json.dumps(self._storage).encode('utf-8'))


class JobPostingComputedProperty(metaclass=ABCMeta):
    """Encapsulates the computation of some piece of data for job postings

    Subclasses must implement _compute_func_on_one to produce a callable that takes in a single
    job posting and returns JSON-serializable output representing the computation target
    """
    def compute_on_collection(self, job_postings_generator):
        """Compute and save to s3 a property for every job posting in a collection.

        Will save data keyed on the job posting ID in JSON format to S3,
        so the output of the property must be JSON-serializable.

        Partitions data daily, according to the datePosted of the job posting.

        Args:
            job_postings_generator (iterable of dicts) Any number of job postings,
                each in dict format
        """
        caches = {}
        misses = 0
        hits = 0
        property_computer = self._compute_func_on_one()
        for number, job_posting in enumerate(job_postings_generator):
            if number % 1000 == 0:
                logging.info(
                    'Computation of job posting properties for %s on posting %s',
                    self.property_name,
                    number
                )
            date_posted = job_posting.get('datePosted', 'unknown')
            if date_posted not in caches:
                caches[date_posted] = S3BackedJsonDict(
                    path='/'.join([
                        config['job_posting_computed_properties']['s3_path'],
                        self.property_name,
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
            self.property_name,
            hits,
            misses
        )

    @abstractmethod
    def _compute_func_on_one(self):
        pass


class TitleCleanPhaseOne(JobPostingComputedProperty):
    property_name = 'title_clean_phase_one'
    property_columns = ['title_clean_phase_one']

    def _compute_func_on_one(self):
        title_func = NLPTransforms().title_phase_one
        return lambda job_posting: title_func(job_posting['title'])


class TitleCleanPhaseTwo(JobPostingComputedProperty):
    property_name = 'title_clean_phase_two'
    property_columns = ['title_clean_phase_two']

    def _compute_func_on_one(self):
        title_func = JobTitleStringClean().clean_title
        return lambda job_posting: title_func(job_posting['title'])


class CBSAandStateFromGeocode(JobPostingComputedProperty):
    def __init__(self, s3_conn, cache_s3_path):
        self.s3_conn = s3_conn
        self.cache_s3_path = cache_s3_path

    property_name = 'cbsa_and_state_from_geocode'
    property_columns = ['cbsa_fips', 'cbsa_name', 'state_code']

    def _compute_func_on_one(self):
        geo_querier = JobCBSAFromGeocodeQuerier(
            cbsa_results=S3CachedCBSAFinder(
                s3_conn=self.s3_conn,
                cache_s3_path=self.cache_s3_path
            ).all_cached_cbsa_results
        )
        return lambda job_posting: geo_querier.query(json.dumps(job_posting))


class ClassifyCommon(JobPostingComputedProperty):
    def __init__(self, s3_conn):
        self.s3_conn = s3_conn
        self.temp_dir = tempfile.TemporaryDirectory()

    property_name = 'soc_common'
    property_columns = ['soc_common']
    aggregate_function = numpy.sum

    def _compute_func_on_one(self):
        common_classifier = Classifier(
            s3_conn=self.s3_conn,
            classifier_id='ann_0614',
            classify_kwargs={'mode': 'common'},
            temporary_directory=self.temp_dir
        )
        corpus_creator = SimpleCorpusCreator()

        def func(job_posting):
            return common_classifier.classify(corpus_creator._transform(job_posting))

        return func


class ClassifyTop(JobPostingComputedProperty):
    def __init__(self, s3_conn):
        self.s3_conn = s3_conn
        self.temp_dir = tempfile.TemporaryDirectory()
    property_name = 'soc_top'
    property_columns = ['soc_top']
    aggregate_function = numpy.sum

    def _compute_func_on_one(self):
        top_classifier = Classifier(
            s3_conn=self.s3_conn,
            classifier_id='ann_0614',
            classify_kwargs={'mode': 'top'},
            temporary_directory=self.temp_dir
        )
        corpus_creator = SimpleCorpusCreator()

        def func(job_posting):
            return top_classifier.classify(corpus_creator._transform(job_posting))

        return func


class ClassifyGiven(JobPostingComputedProperty):
    property_name = 'soc_given'
    property_columns = ['soc_given']
    aggregate_function = numpy.sum

    def _compute_func_on_one(self):
        def func(job_posting):
            return job_posting.get('onet_soc_code', '99-9999.00')
        return func


class SocScopedExactMatchSkillCounts(JobPostingComputedProperty):
    def __init__(self, s3_conn, output_tables_path, skills_file='skills_master_table.tsv'):
        self.s3_conn = s3_conn
        self.output_tables_path = output_tables_path
        self.skills_file = skills_file
        self.temp_dir = tempfile.TemporaryDirectory()

    property_name = 'skill_counts_soc_scoped'
    property_columns = []

    def _compute_func_on_one(self):
        skills_filename = '{}/{}'.format(self.temp_dir, self.skills_file)

        if not os.path.isfile(skills_filename):
            download(
              s3_conn=self.s3_conn,
              out_filename=skills_filename,
              s3_path=self.output_tables_path + self.skills_file
            )
        corpus_creator = SimpleCorpusCreator()
        skill_extractor = OccupationScopedSkillExtractor(skill_lookup_path=skills_filename)

        def func(job_posting):
            count_dict = skill_extractor.document_skill_counts(
              soc_code=job_posting.get('onet_soc_code', '99-9999.00'),
              document=corpus_creator._transform(job_posting)
            )
            count_lists = [[k] * v for k, v in count_dict.items()]
            flattened = [count for countlist in count_lists for count in countlist]
            return {self.property_name: flattened}
        return func


class ExactMatchSkillCounts(JobPostingComputedProperty):
    def __init__(self, s3_conn, output_tables_path, skills_file='skills_master_table.tsv'):
        self.s3_conn = s3_conn
        self.output_tables_path = output_tables_path
        self.skills_file = skills_file
        self.temp_dir = tempfile.TemporaryDirectory()

    property_name = 'skill_counts_exact_match'
    property_columns = []

    def _compute_func_on_one(self):
        skills_filename = '{}/{}'.format(self.temp_dir, self.skills_file)

        if not os.path.isfile(skills_filename):
            download(
              s3_conn=self.s3_conn,
              out_filename=skills_filename,
              s3_path=self.output_tables_path + self.skills_file
            )
        corpus_creator = SimpleCorpusCreator()
        skill_extractor = ExactMatchSkillExtractor(skill_lookup_path=skills_filename)

        def func(job_posting):
            count_dict = skill_extractor.document_skill_counts(
              document=corpus_creator._transform(job_posting)
            )
            count_lists = [[k] * v for k, v in count_dict.items()]
            flattened = [count for countlist in count_lists for count in countlist]
            return {self.property_name: flattened}
        return func


class PostingIdPresent(JobPostingComputedProperty):
    property_name = 'posting_id_present'
    property_columns = ['posting_id_present']
    aggregate_function = numpy.sum  # provides a count

    def _compute_func_on_one(self):
        return lambda job_posting: 1


class QuarterlyJobPostingOperatorMixin(object):
    """Operate on quarterly job postings

    Provides a self.job_posting_generator method to provide job postings
    for the quarter a context resides in
    """
    def job_postings_generator(self, context):
        quarter = datetime_to_quarter(context['execution_date'])
        s3_conn = S3Hook().get_conn()

        for job_posting_string in job_postings_highmem(
            s3_conn,
            quarter,
            config['job_postings']['s3_path']
        ):
            yield json.loads(job_posting_string)


class JobPostingComputedPropertyOperator(BaseOperator, QuarterlyJobPostingOperatorMixin):
    def execute(self, context):
        self.computed_property().compute_on_collection(self.job_postings_generator(context))

class TitleCleanPhaseOne(JobPostingComputedPropertyOperator):
    def computed_property(self):
        return TitleCleanPhaseOne()

class TitleCleanPhaseTwo(JobPostingComputedPropertyOperator):
    def computed_property(self):
        return TitleCleanPhaseTwo()

class ClassifyCommon(JobPostingComputedPropertyOperator):
    def computed_property(self):
        return ClassifyCommon(S3Hook().get_conn())

class ClassifyTop(JobPostingComputedPropertyOperator):
    def computed_property(self):
        return ClassifyTop(S3Hook().get_conn())

class ClassifyGiven(JobPostingComputedPropertyOperator):
    def computed_property(self):
        return ClassifyGiven()

class SocScopedExactMatchSkillCounts(JobPostingComputedPropertyOperator):
    def computed_property(self):
        return SocScopedExactMatchSkillCounts(S3Hook().get_conn(), config['output_tables']['s3_path'])

class ExactMatchSkillCounts(JobPostingComputedPropertyOperator):
    def computed_property(self):
        return ExactMatchSkillCounts(S3Hook().get_conn(), config['output_tables']['s3_path'])

class PostingIdPresent(JobPostingComputedPropertyOperator):
    def computed_property(self):
        return PostingIdPresent()

class CBSAandStateFromGeocode(JobPostingComputedPropertyOperator):
    def computed_property(self):
        return CBSAandStateFromGeocode(S3Hook().get_conn(), config['cbsa_lookup']['s3_path'])


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
    TitleCountAggregate(task_id='title_count_aggregate', dag=dag)
    return dag


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


def job_posting_computed_properties(s3_conn, quarter, grouping_properties, aggregate_properties, aggregate_functions, aggregation_name):
    """Aggregate computed properties

    Args:
        s3_conn
        quarter
        grouping_properties (list of JobPostingComputedProperty)
        aggregate_properties (list of JobPostingComputedProperty)
        aggregate_functions (dict)
    """
    start_date, end_date = quarter_to_daterange(quarter)
    for included_date in daterange(start_date, end_date):
        datestring = included_date.strftime("%Y-%m-%d")
        dataframes = []
        for computed_property in grouping_properties + aggregate_properties:
            cache = S3BackedJsonDict(
                path='/'.join([
                    config['job_posting_computed_properties']['s3_path'],
                    computed_property.property_name,
                    datestring
                ])
            )
            if len(cache) == 0:
                continue
            df = pandas.DataFrame.from_dict(cache, orient='index')
            df.columns = computed_property.property_columns
            dataframes.append(df)
        big_df = dataframes[0].join(dataframes[1:])
        column_lists = [p.property_columns for p in grouping_properties]
        final_columns = [col for collist in column_lists for col in collist]
        aggregates = big_df.groupby(final_columns).agg(aggregate_functions)
        aggregates.columns = ['_'.join(t) for t in aggregates.columns]
        out_path = '/'.join([
            config['job_posting_aggregations']['s3_path'],
            aggregation_name,
            datestring + '.csv'
        ])
        fs = s3fs.S3FileSystem()
        with fs.open(out_path, 'wb') as f:
            f.write(aggregates.to_csv(None).encode())


class TitleCountAggregate(BaseOperator):
    def execute(self, context):
        s3_conn = S3Hook().get_conn()
        left = [
            TitleCleanPhaseOne(),
            CBSAandStateFromGeocode(s3_conn, config['cbsa_lookup']['s3_path'])
        ]
        right = [
            PostingIdPresent(),
            ClassifyCommon(s3_conn),
            ClassifyTop(s3_conn)
        ]
        # aggregate functions to apply to specific aggregate field
        # the aggregate function,
        # when applied to the data in that field over a collection of job postings,
        # *must* reduce or else pandas won't be able to work with it.
        aggregate_functions = {
            'posting_id_present': numpy.sum,
            'soc_common': numpy.sum
        }
        quarter = datetime_to_quarter(context['execution_date'])
        job_posting_computed_properties(
            s3_conn,
            quarter,
            left,
            right,
            aggregate_functions
        )
