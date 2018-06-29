from config import config
from skills_utils.time import datetime_to_year_quarter
from airflow.hooks import S3Hook
from datetime import datetime, timedelta
from skills_ml.job_postings.common_schema import JobPostingCollectionFromS3
from skills_ml.job_postings.computed_properties.computers import (
    TitleCleanPhaseOne,
    TitleCleanPhaseTwo,
    CBSAandStateFromGeocode,
    GivenSOC,
    SkillCounts,
    PostingIdPresent
)
from skills_ml.algorithms.skill_extractors import (
    ExactMatchSkillExtractor,
    FuzzyMatchSkillExtractor,
    SkillEndingPatternExtractor,
    AbilityEndingPatternExtractor,
)
from airflow import DAG
from skills_ml.storage import S3Store
from airflow.operators import BaseOperator
from utils.dags import QuarterlySubDAG
from functools import partial
import logging


def partition_key(transformed_document):
    try:
        partition_key = transformed_document['id'][-4:]
    except Exception as e:
        logging.warning('No partition key available! Choosing fallback')
        partition_key = '-1'
    return partition_key


class YearlyJobPostingOperatorMixin(object):
    """Operate on quarterly job postings

    Provides a self.job_posting_generator method to provide job postings
    for the year a context resides in
    """
    def job_postings_generator(self, context):
        year, _ = datetime_to_year_quarter(context['execution_date'])
        s3_conn = S3Hook().get_conn()
        paths = [f"{base_path}/{year}" for base_path in config['job_postings']['s3_path']]
        return JobPostingCollectionFromS3(s3_conn=s3_conn, s3_paths=paths)

    def storage(self, context):
        year, _ = datetime_to_year_quarter(context['execution_date'])
        computed_properties_base_path = config['job_posting_computed_properties']['s3_path']

        return S3Store(f'{computed_properties_base_path}/{year}')


class JobPostingComputedPropertyOperator(BaseOperator, YearlyJobPostingOperatorMixin):
    def execute(self, context):
        common_kwargs = {
            'storage': self.storage(context),
            'partition_func': partition_key,
        }
        self.computed_property(common_kwargs).compute_on_collection(self.job_postings_generator(context))


class TitleCleanPhaseOneOp(JobPostingComputedPropertyOperator):
    def computed_property(self, common_kwargs):
        return TitleCleanPhaseOne(**common_kwargs)


class TitleCleanPhaseTwoOp(JobPostingComputedPropertyOperator):
    def computed_property(self, common_kwargs):
        return TitleCleanPhaseTwo(**common_kwargs)


class ExactMatchONETSkillCountsOp(JobPostingComputedPropertyOperator):
    def computed_property(self, common_kwargs):
        skill_extractor = ExactMatchSkillExtractor(
            skill_lookup_path=config['skill_sources']['onet_ksat'],
            skill_lookup_name='onet_ksat',
            skill_lookup_description='ONET Knowledge, Skills, Abilities, Tools, and Technology'
        )
        return SkillCounts(skill_extractor, **common_kwargs)


class FuzzyMatchONETSkillCountsOp(JobPostingComputedPropertyOperator):
    def computed_property(self, common_kwargs):
        skill_extractor = FuzzyMatchSkillExtractor(
            skill_lookup_path=config['skill_sources']['onet_ksat'],
            skill_lookup_name='onet_ksat',
            skill_lookup_description='ONET Knowledge, Skills, Abilities, Tools, and Technology'
        )
        return SkillCounts(skill_extractor, **common_kwargs)


class SkillEndingSkillCountsOp(JobPostingComputedPropertyOperator):
    def computed_property(self, common_kwargs):
        skill_extractor = SkillEndingPatternExtractor()
        return SkillCounts(skill_extractor, **common_kwargs)


class AbilityEndingSkillCountsOp(JobPostingComputedPropertyOperator):
    def computed_property(self, common_kwargs):
        skill_extractor = AbilityEndingPatternExtractor()
        return SkillCounts(skill_extractor, **common_kwargs)


class PostingIdPresentOp(JobPostingComputedPropertyOperator):
    def computed_property(self, common_kwargs):
        return PostingIdPresent(**common_kwargs)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2015, 6, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('test_process', default_args=default_args)
TitleCleanPhaseOneOp(task_id='title_clean_phase_one', dag=dag)
TitleCleanPhaseTwoOp(task_id='title_clean_phase_two', dag=dag)
#ClassifyCommon(task_id='soc_common', dag=dag)
#ClassifyTop(task_id='soc_top', dag=dag)
#ClassifyGiven(task_id='soc_given', dag=dag)
#SocScopedExactMatchSkillCountsOp(task_id='skill_counts_exact_match_soc_scoped', dag=dag)
ExactMatchONETSkillCountsOp(task_id='skill_counts_exact_match_onet', dag=dag)
FuzzyMatchONETSkillCountsOp(task_id='skill_counts_fuzzy_match_onet', dag=dag)
SkillEndingSkillCountsOp(task_id='skill_counts_skill_ending', dag=dag)
AbilityEndingSkillCountsOp(task_id='skill_counts_ability_ending', dag=dag)
PostingIdPresentOp(task_id='posting_id_present', dag=dag)
#CBSAandStateFromGeocodeOp(task_id='cbsa_and_state_from_geocode', dag=dag)
