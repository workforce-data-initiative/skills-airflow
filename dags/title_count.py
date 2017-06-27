"""Takes common schema job listings and performs
title extraction, cleaning, and aggregation
"""
import os
from collections import OrderedDict

from airflow.hooks import S3Hook
from skills_ml.algorithms.aggregators import\
    CountAggregator, SkillAggregator, SocCodeAggregator, GivenSocCodeAggregator
from skills_ml.algorithms.aggregators.title import GeoTitleAggregator
from skills_ml.algorithms.corpus_creators.basic import SimpleCorpusCreator
from skills_ml.algorithms.jobtitle_cleaner.clean import JobTitleStringClean
from skills_ml.algorithms.string_cleaners import NLPTransforms
from skills_ml.algorithms.skill_extractors.freetext\
    import FreetextSkillExtractor
from skills_ml.algorithms.occupation_classifiers.classifiers import Classifier, download_ann_classifier_files
from skills_utils.s3 import download

from config import config
from operators.geo_count import GeoCountOperator
from utils.dags import QuarterlySubDAG


def title_aggregate(
        job_postings,
        aggregator_constructor,
        processed_folder,
        phase_indices,
        download_folder,
):
    nlp_transforms = NLPTransforms()
    string_clean = JobTitleStringClean()
    phase_functions = [nlp_transforms.title_phase_one, string_clean.clean_title]

    def title_cleaner(title):
        for phase_index in phase_indices:
            title = phase_functions[phase_index](title)
        return title
    skills_filename = '{}/skills_master_table.tsv'\
        .format(processed_folder)

    download(
        s3_conn=S3Hook().get_conn(),
        out_filename=skills_filename,
        s3_path=config['output_tables']['s3_path'] + '/skills_master_table.tsv'
    )
    s3_conn = S3Hook().get_conn()
    corpus_creator = SimpleCorpusCreator()
    common_classifier = Classifier(
        s3_conn=s3_conn,
        classifier_id='ann_0614',
        classify_kwargs={'mode': 'common'},
        temporary_directory=download_folder,
    )
    top_classifier = Classifier(
        s3_conn=s3_conn,
        classifier=common_classifier.classifier,
        classifier_id='ann_0614',
        classify_kwargs={'mode': 'top'},
        temporary_directory=download_folder,
    )
    job_aggregators = OrderedDict()
    job_aggregators['counts'] = CountAggregator()
    job_aggregators['onet_skills'] = SkillAggregator(
        corpus_creator=corpus_creator,
        skill_extractor=FreetextSkillExtractor(
            skills_filename=skills_filename
        ),
        output_count=10
    )
    job_aggregators['soc_code_common'] = SocCodeAggregator(
        corpus_creator=corpus_creator,
        occupation_classifier=common_classifier,
        output_count=2,
        output_total=True
    )
    job_aggregators['soc_code_top'] = SocCodeAggregator(
        corpus_creator=corpus_creator,
        occupation_classifier=top_classifier,
        output_count=2,
        output_total=True
    )
    job_aggregators['soc_code_given'] = GivenSocCodeAggregator(
        output_count=2,
        output_total=True
    )
    aggregator = aggregator_constructor(
        job_aggregators=job_aggregators,
        title_cleaner=title_cleaner
    )
    aggregator.process_postings(job_postings)
    return aggregator.job_aggregators


def define_title_counts(main_dag_name):
    dag = QuarterlySubDAG(main_dag_name, 'title_count')

    model_cache_config = config.get('model_cache', {})
    cache_local_path = model_cache_config.get('local_path', '/tmp')
    cache_s3_path = model_cache_config.get('s3_path', '')

    class GeoTitleCountOperator(GeoCountOperator):
        map_function = title_aggregate

        def aggregator_constructor(self):
            return GeoTitleAggregator

        def passthroughs(self):
            output_folder = config.get('output_folder', 'output')
            if not os.path.isdir(output_folder):
                os.mkdir(output_folder)
            return {
                'processed_folder': output_folder,
                'phase_indices': self.phase_indices,
                'download_folder': cache_local_path
            }

        def execute(self, *args, **kwargs):
            download_ann_classifier_files(
                s3_prefix=cache_s3_path,
                classifier_id='ann_0614',
                download_directory=cache_local_path,
                s3_conn=S3Hook().get_conn()
            )
            super(GeoTitleCountOperator, self).execute(*args, **kwargs)

    class GeoTitlePhaseOneCountOperator(GeoTitleCountOperator):
        group_config_key = 'geo_title_count_dir'
        rollup_config_key = 'title_count_dir'
        phase_indices = [0]

    class GeoTitlePhaseTwoCountOperator(GeoTitleCountOperator):
        group_config_key = 'cleaned_geo_title_count_dir'
        rollup_config_key = 'cleaned_title_count_dir'
        phase_indices = [0, 1]

    GeoTitlePhaseOneCountOperator(task_id='clean_phase_one_geo_count', dag=dag)
    GeoTitlePhaseTwoCountOperator(task_id='clean_phase_two_geo_count', dag=dag)

    return dag
