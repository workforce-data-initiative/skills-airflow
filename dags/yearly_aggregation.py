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
    PostingIdPresent
)
from skills_ml.job_postings.computed_properties.aggregators import aggregate_properties, base_func

from skills_ml.algorithms.skill_extractors import (
    ExactMatchSkillExtractor,
    FuzzyMatchSkillExtractor,
    SkillEndingPatternExtractor,
    AbilityEndingPatternExtractor,
    SocScopedExactMatchSkillExtractor
)
import numpy

from skills_ml.algorithms.geocoders import CachedGeocoder
from skills_ml.algorithms.geocoders.cbsa import CachedCBSAFinder
from skills_ml.job_postings.geography_queriers.cbsa import JobCBSAFromGeocodeQuerier
from skills_ml.job_postings.geography_queriers.state import JobStateQuerier

from skills_ml.algorithms.embedding.models import Doc2VecModel
from skills_ml.algorithms.occupation_classifiers.classifiers import SocClassifier, KNNDoc2VecClassifier
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

# compute step:
# for path in job posting paths:
#   grab all individual s3 files under path using a prefix search
#   for each file, spawn multiprocessing worker that:
#       create JobPostingCollection for individual file
#       create storage object with computed_properties_base_path/year/filename
#       create computed property with storage object and partition key of static '0' (the partition is the file, no need for further partitioning)
#       compute on job posting collection
#
# aggregation step:
# for each property type:
#   create storage object with computed_properties_base_path/year
#   create computed property with storage object. partition func doesn't matter as we don't do any computing
# call aggregation with all computed property objects


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



class AggregateOperator(BaseOperator, YearlyJobPostingOperatorMixin):
    def aggregation_storage(self):
        aggregations_base_path = config['job_posting_aggregations']['s3_path']
        return S3Store(aggregations_base_path)

    def execute(self, context):
        year, _ = datetime_to_year_quarter(context['execution_date'])
        common_kwargs = {'storage': self.storage(context)}
        grouping_properties = self.grouping_properties(common_kwargs)
        aggregated_properties = self.aggregate_properties(common_kwargs)
        aggregate_functions = self.aggregate_functions()
        aggregate_path = aggregate_properties(
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
                    desc = next(desc for path, desc in column.compatible_aggregate_function_paths.items() if funcname in path)
                    full_desc = desc + ' ' + column.description
                    lines.append(f'{column.name}: {full_desc}')
        readme_string = '\r'.join(lines)
        self.aggregation_storage().write(readme_string.encode('utf-8'), f'{self.aggregation_name}/README.txt')


class GivenSocCounts(AggregateOperator):
    aggregation_name = 'given_soc_counts'

    def grouping_properties(self, common_kwargs):
        return [GivenSoc(**common_kwargs)]

    def aggregate_properties(self, common_kwargs):
        return [PostingIdPresent(**common_kwargs)]

    def aggregate_functions(self):
        return {'posting_id_present': [numpy.sum]}


class GivenSocCBSACounts(AggregateOperator):
    aggregation_name = 'given_soc_cbsa_counts'

    def grouping_properties(self, common_kwargs):
        return [
            GivenSoc(**common_kwargs),
            cbsa_querier_property(common_kwargs)
        ]

    def aggregate_properties(self, common_kwargs):
        return [PostingIdPresent(**common_kwargs)]

    def aggregate_functions(self):
        return {'posting_id_present': [numpy.sum]}


class TitleCountsByState(AggregateOperator):
    aggregation_name = 'title_state_counts'

    def grouping_properties(self, common_kwargs):
        return [
            TitleCleanPhaseOne(**common_kwargs),
            Geography(JobStateQuerier(), **common_kwargs)
        ]

    def aggregate_properties(self, common_kwargs):
        return [PostingIdPresent(**common_kwargs)]

    def aggregate_functions(self):
        return {'posting_id_present': [numpy.sum]}


class CleanedTitleCountsByState(AggregateOperator):
    aggregation_name = 'cleaned_title_state_counts'

    def grouping_properties(self, common_kwargs):
        return [
            TitleCleanPhaseTwo(**common_kwargs),
            Geography(JobStateQuerier(), **common_kwargs)
        ]

    def aggregate_properties(self, common_kwargs):
        return [PostingIdPresent(**common_kwargs)]

    def aggregate_functions(self):
        return {'posting_id_present': [numpy.sum]}


class TitleCountsByCBSA(AggregateOperator):
    aggregation_name = 'title_cbsa_counts'

    def grouping_properties(self, common_kwargs):
        return [
            TitleCleanPhaseOne(**common_kwargs),
            cbsa_querier_property(common_kwargs)
        ]

    def aggregate_properties(self, common_kwargs):
        return [PostingIdPresent(**common_kwargs)]

    def aggregate_functions(self):
        return {'posting_id_present': [numpy.sum]}


class CleanedTitleCountsByCBSA(AggregateOperator):
    aggregation_name = 'cleaned_title_cbsa_counts'

    def grouping_properties(self, common_kwargs):
        return [
            TitleCleanPhaseTwo(**common_kwargs),
            cbsa_querier_property(common_kwargs)
        ]

    def aggregate_properties(self, common_kwargs):
        return [PostingIdPresent(**common_kwargs)]

    def aggregate_functions(self):
        return {'posting_id_present': [numpy.sum]}

class JobPostingComputedPropertyOperator(BaseOperator, YearlyJobPostingOperatorMixin):
    def execute(self, context):
        common_kwargs = {
            'storage': self.storage(context),
            'partition_func': partition_key,
        }
        self.computed_property_instance = self.computed_property(common_kwargs)
        self.computed_property_instance.compute_on_collection(self.job_postings_generator(context))


class TitleCleanPhaseOneOp(JobPostingComputedPropertyOperator):
    def computed_property(self, common_kwargs):
        return TitleCleanPhaseOne(**common_kwargs)


class TitleCleanPhaseTwoOp(JobPostingComputedPropertyOperator):
    def computed_property(self, common_kwargs):
        return TitleCleanPhaseTwo(**common_kwargs)


class GivenSocOp(JobPostingComputedPropertyOperator):
    def computed_property(self, common_kwargs):
        return GivenSOC(**common_kwargs)


class ClassifyCommonOp(JobPostingComputedPropertyOperator):
    def computed_property(self, common_kwargs):
        storage = S3Store(config['embedding_models']['s3_path'])
        embedding_model = Doc2VecModel.load(storage=storage, model_name=config['embedding_models']['gold_standard'])
        classifier = KNNDoc2VecClassifier(embedding_model, k=10)
        return SOCClassifyProperty(classifier, **common_kwargs)

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


class SocScopedExactMatchSkillCountsOp(JobPostingComputedPropertyOperator):
    def computed_property(self, common_kwargs):
        skill_extractor = SocScopedExactMatchSkillExtractor(
            skill_lookup_path=config['skill_sources']['onet_ksat_with_soc'],
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
    #'schedule_interval': '@yearly',
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
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
comp_soc_onet = SocScopedExactMatchSkillCountsOp(task_id='skill_counts_exact_match_onet_soc_scoped', dag=dag)
comp_skill = SkillEndingSkillCountsOp(task_id='skill_counts_skill_ending', dag=dag)
comp_ab = AbilityEndingSkillCountsOp(task_id='skill_counts_ability_ending', dag=dag)
counts = PostingIdPresentOp(task_id='posting_id_present', dag=dag)
cbsa = CBSAOp(task_id='cbsa_from_geocode', dag=dag)
state = StateOp(task_id='state', dag=dag)

title_state_counts = TitleCountsByState(task_id='title_counts_by_state', dag=dag)
title_state_counts.set_upstream(state)
title_state_counts.set_upstream(title_p1)
title_state_counts.set_upstream(counts)

cleaned_title_state_counts = CleanedTitleCountsByState(task_id='cleaned_title_counts_by_state', dag=dag)
cleaned_title_state_counts.set_upstream(state)
cleaned_title_state_counts.set_upstream(title_p2)
cleaned_title_state_counts.set_upstream(counts)


title_cbsa_counts = TitleCountsByCBSA(task_id='title_counts_by_cbsa', dag=dag)
title_cbsa_counts.set_upstream(cbsa)
title_cbsa_counts.set_upstream(title_p1)
title_cbsa_counts.set_upstream(counts)

cleaned_title_cbsa_counts = CleanedTitleCountsByCBSA(task_id='cleaned_title_counts_by_cbsa', dag=dag)
cleaned_title_cbsa_counts.set_upstream(cbsa)
cleaned_title_cbsa_counts.set_upstream(title_p2)
cleaned_title_cbsa_counts.set_upstream(counts)

soc_given_counts = GivenSocCounts(task_id='soc_given_counts', dag=dag)
soc_given_counts.set_upstream(given_soc)
soc_given_counts.set_upstream(counts)

soc_given_cbsa_counts = GivenSocCBSACounts(task_id='soc_given_cbsa_counts', dag=dag)
soc_given_cbsa_counts.set_upstream(given_soc)
soc_given_cbsa_counts.set_upstream(cbsa)
soc_given_cbsa_counts.set_upstream(counts)
