from config import config
from skills_utils.time import datetime_to_year_quarter
from airflow.hooks import S3Hook
from datetime import datetime, timedelta
from skills_ml.job_postings.common_schema import JobPostingCollectionFromS3
from skills_ml.job_postings.computed_properties.computers import (
    TitleCleanPhaseOne,
    TitleCleanPhaseTwo,
    Geography,
    GivenSOC,
    SOCClassifyProperty,
    SkillCounts,
    PostingIdPresent,
    HourlyPay,
    YearlyPay
)
from skills_ml.job_postings.computed_properties.aggregators import (
    aggregate_properties,
    base_func
)
from skills_ml.algorithms.skill_extractors import (
    ExactMatchSkillExtractor,
    FuzzyMatchSkillExtractor,
    SkillEndingPatternExtractor,
    AbilityEndingPatternExtractor,
    SocScopedExactMatchSkillExtractor
)
from skills_ml.ontologies.onet import Onet
import numpy
from skills_ml.job_postings.aggregate.pandas import listy_n_most_common
from functools import partial

from skills_ml.algorithms.geocoders import CachedGeocoder
from skills_ml.algorithms.geocoders.cbsa import CachedCBSAFinder
from skills_ml.job_postings.geography_queriers.cbsa import JobCBSAFromGeocodeQuerier
from skills_ml.job_postings.geography_queriers.state import JobStateQuerier

from skills_ml.algorithms.embedding.models import Doc2VecModel
from skills_ml.algorithms.occupation_classifiers.classifiers import KNNDoc2VecClassifier
from airflow import DAG
from skills_ml.storage import S3Store
from airflow.operators import BaseOperator
import logging


def partition_key(transformed_document):
    try:
        partition_key = transformed_document['id'][-4:]
    except Exception as e:
        logging.warning('No partition key available! Choosing fallback')
        partition_key = '-1'
    return partition_key


def cbsa_querier_property(common_kwargs):
    geocoding_storage = S3Store(config['geocoding']['s3_path'])
    geocoder = CachedGeocoder(
        cache_storage=geocoding_storage,
        cache_fname=config['geocoding']['raw_filename'],
        autosave=False
    )
    cbsa_finder = CachedCBSAFinder(
        cache_storage=geocoding_storage,
        cache_fname=config['geocoding']['cbsa_filename']
    )
    querier = JobCBSAFromGeocodeQuerier(geocoder=geocoder, cbsa_finder=cbsa_finder)
    return Geography(geo_querier=querier, **common_kwargs)


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

    def common_kwargs(self, context):
        return {
            'storage': self.storage(context),
            'partition_func': partition_key,
        }


class AggregateOperator(BaseOperator, YearlyJobPostingOperatorMixin):
    def __init__(
        self,
        aggregation_name,
        grouping_operators,
        aggregate_operators,
        aggregate_functions,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.aggregation_name = aggregation_name
        self.grouping_operators = grouping_operators
        self.aggregate_operators = aggregate_operators
        self.set_upstream(self.grouping_operators)
        self.set_upstream(self.aggregate_operators)
        self.aggregate_functions = aggregate_functions

    def aggregation_storage(self):
        aggregations_base_path = config['job_posting_aggregations']['s3_path']
        return S3Store(aggregations_base_path)

    def execute(self, context):
        year, _ = datetime_to_year_quarter(context['execution_date'])
        common_kwargs = self.common_kwargs(context)
        grouping_properties = [
            gp.computed_property(common_kwargs)
            for gp in self.grouping_operators
        ]
        aggregated_properties = [
            ap.computed_property(common_kwargs)
            for ap in self.aggregate_operators
        ]
        aggregate_functions = self.aggregate_functions
        aggregate_properties(
            out_filename=str(year),
            grouping_properties=grouping_properties,
            aggregate_properties=aggregated_properties,
            aggregate_functions=aggregate_functions,
            aggregation_name=self.aggregation_name,
            storage=self.aggregation_storage()
        )

        lines = []
        lines.append('Grouping columns')
        for group in grouping_properties:
            for column in group.property_columns:
                lines.append(f'{column.name}: {column.description}')
        lines.append(' ')
        lines.append('Aggregate columns')
        for agg in aggregated_properties:
            for column in agg.property_columns:
                for aggfunc in aggregate_functions.get(column.name, []):
                    funcname = base_func(aggfunc).__qualname__
                    desc = next(
                        desc for path, desc
                        in column.compatible_aggregate_function_paths.items()
                        if funcname in path
                    )
                    full_desc = desc + ' ' + column.description
                    lines.append(f'{column.name}: {full_desc}')
        readme_string = '\r'.join(lines)
        self.aggregation_storage().write(
            readme_string.encode('utf-8'),
            f'{self.aggregation_name}/README.txt'
        )


class JobPostingComputedPropertyOperator(BaseOperator, YearlyJobPostingOperatorMixin):
    def execute(self, context):
        common_kwargs = self.common_kwargs(context)
        self.computed_property_instance = self.computed_property(common_kwargs)
        self.computed_property_instance.compute_on_collection(self.job_postings_generator(context))


class TitleCleanPhaseOneOp(JobPostingComputedPropertyOperator):
    def computed_property(self, common_kwargs):
        return TitleCleanPhaseOne(**common_kwargs)


class TitleCleanPhaseTwoOp(JobPostingComputedPropertyOperator):
    def computed_property(self, common_kwargs):
        return TitleCleanPhaseTwo(**common_kwargs)


class YearlyPayOp(JobPostingComputedPropertyOperator):
    def computed_property(self, common_kwargs):
        return YearlyPay(**common_kwargs)


class HourlyPayOp(JobPostingComputedPropertyOperator):
    def computed_property(self, common_kwargs):
        return HourlyPay(**common_kwargs)


class GivenSocOp(JobPostingComputedPropertyOperator):
    def computed_property(self, common_kwargs):
        return GivenSOC(**common_kwargs)


class ClassifyCommonOp(JobPostingComputedPropertyOperator):
    def computed_property(self, common_kwargs):
        storage = S3Store(config['embedding_models']['s3_path'])
        embedding_model = Doc2VecModel.load(
            storage=storage,
            model_name=config['embedding_models']['gold_standard']
        )
        classifier = KNNDoc2VecClassifier(embedding_model, k=10)
        return SOCClassifyProperty(classifier, **common_kwargs)


class ExactMatchONETSkillCountsOp(JobPostingComputedPropertyOperator):
    def computed_property(self, common_kwargs):
        skill_extractor = ExactMatchSkillExtractor(competency_framework=Onet().competency_framework)
        return SkillCounts(skill_extractor, **common_kwargs)


class FuzzyMatchONETSkillCountsOp(JobPostingComputedPropertyOperator):
    def computed_property(self, common_kwargs):
        skill_extractor = FuzzyMatchSkillExtractor(competency_framework=Onet().competency_framework)
        return SkillCounts(skill_extractor, **common_kwargs)


class SocScopedExactMatchSkillCountsOp(JobPostingComputedPropertyOperator):
    def computed_property(self, common_kwargs):
        skill_extractor = SocScopedExactMatchSkillExtractor(competency_ontology=Onet())
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


class CBSAOp(JobPostingComputedPropertyOperator):
    def computed_property(self, common_kwargs):
        return cbsa_querier_property(common_kwargs)

    def execute(self, *args, **kwargs):
        super().execute(*args, **kwargs)
        self.computed_property_instance.geo_querier.geocoder.save()


class StateOp(JobPostingComputedPropertyOperator):
    def computed_property(self, common_kwargs):
        querier = JobStateQuerier()
        return Geography(geo_querier=querier, **common_kwargs)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2010, 1, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'yearly_aggregation',
    default_args=default_args,
    schedule_interval='0 0 31 12 *'
)
title_p1 = TitleCleanPhaseOneOp(task_id='title_clean_phase_one', dag=dag)
title_p2 = TitleCleanPhaseTwoOp(task_id='title_clean_phase_two', dag=dag)
common_soc = ClassifyCommonOp(task_id='soc_code_common', dag=dag)
given_soc = GivenSocOp(task_id='soc_code_given', dag=dag)
comp_exact_onet = ExactMatchONETSkillCountsOp(task_id='skill_counts_exact_match_onet', dag=dag)
comp_fuzzy_onet = FuzzyMatchONETSkillCountsOp(task_id='skill_counts_fuzzy_match_onet', dag=dag)
comp_soc_onet = SocScopedExactMatchSkillCountsOp(
    task_id='skill_counts_exact_match_onet_soc_scoped',
    dag=dag
)
comp_skill = SkillEndingSkillCountsOp(task_id='skill_counts_skill_ending', dag=dag)
comp_ab = AbilityEndingSkillCountsOp(task_id='skill_counts_ability_ending', dag=dag)
counts = PostingIdPresentOp(task_id='posting_id_present', dag=dag)
cbsa = CBSAOp(task_id='cbsa_from_geocode', dag=dag)
state = StateOp(task_id='state', dag=dag)
yearly_pay = YearlyPayOp(task_id='yearly_pay', dag=dag)
hourly_pay = HourlyPayOp(task_id='hourly_pay', dag=dag)


title_state_counts = AggregateOperator(
    aggregation_name='title_state_counts',
    grouping_operators=[state, title_p1],
    aggregate_operators=[counts],
    aggregate_functions={'posting_id_present': [numpy.sum]},
    task_id='title_state_counts',
    dag=dag
)


title_cbsa_counts = AggregateOperator(
    aggregation_name='title_cbsa_counts',
    grouping_operators=[cbsa, title_p1],
    aggregate_operators=[counts, comp_soc_onet, yearly_pay, hourly_pay],
    aggregate_functions={
        'posting_id_present': [numpy.sum],
        'skill_counts_onet_ksat_occscoped_exact_match': [partial(listy_n_most_common, 10)],
        'yearly_pay': [numpy.mean, numpy.median],
        'hourly_pay': [numpy.mean, numpy.median],
    },
    task_id='title_cbsa_counts',
    dag=dag
)

cleaned_title_cbsa_counts = AggregateOperator(
    aggregation_name='cleaned_title_cbsa_counts',
    grouping_operators=[cbsa, title_p2],
    aggregate_operators=[counts, comp_soc_onet, yearly_pay, hourly_pay],
    aggregate_functions={
        'posting_id_present': [numpy.sum],
        'skill_counts_onet_ksat_occscoped_exact_match': [partial(listy_n_most_common, 10)],
        'yearly_pay': [numpy.mean, numpy.median],
        'hourly_pay': [numpy.mean, numpy.median],
    },
    task_id='cleaned_title_cbsa_counts',
    dag=dag
)

cleaned_title_state_counts = AggregateOperator(
    aggregation_name='cleaned_title_state_counts',
    grouping_operators=[state, title_p2],
    aggregate_operators=[counts],
    aggregate_functions={'posting_id_present': [numpy.sum]},
    task_id='cleaned_title_state_counts',
    dag=dag
)

soc_given_counts = AggregateOperator(
    aggregation_name='given_soc_counts',
    grouping_operators=[given_soc],
    aggregate_operators=[counts],
    aggregate_functions={'posting_id_present': [numpy.sum]},
    task_id='soc_given_counts',
    dag=dag
)

soc_given_cbsa_counts = AggregateOperator(
    aggregation_name='given_soc_cbsa_counts',
    grouping_operators=[given_soc, cbsa],
    aggregate_operators=[counts],
    aggregate_functions={'posting_id_present': [numpy.sum]},
    task_id='soc_given_cbsa_counts',
    dag=dag
)
