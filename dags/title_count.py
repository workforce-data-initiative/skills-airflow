
"""Takes common schema job listings and performs
title extraction, cleaning, and aggregation
"""
import os

from airflow.hooks import S3Hook
from skills_ml.algorithms.aggregators import\
    CountAggregator, OccupationScopedSkillAggregator, SocCodeAggregator, GivenSocCodeAggregator
from skills_ml.algorithms.aggregators.title import GeoTitleAggregator
from skills_ml.algorithms.corpus_creators.basic import SimpleCorpusCreator
from skills_ml.algorithms.jobtitle_cleaner.clean import JobTitleStringClean
from skills_ml.algorithms.string_cleaners import NLPTransforms
from skills_ml.algorithms.skill_extractors.freetext\
    import OccupationScopedSkillExtractor
from skills_ml.algorithms.occupation_classifiers.classifiers import \
    Classifier, download_ann_classifier_files
from skills_utils.s3 import download

from config import config
from operators.geo_count import GeoCountOperator, MergeOperator
from utils.dags import QuarterlySubDAG
from uuid import uuid4
from functools import partial
import logging
import joblib
from collections.abc import MutableMapping


def save(aggregator, temp_dir):
    pickle_filename = os.path.join(temp_dir, str(uuid4()) + '.pkl')
    logging.info('Pickling to %s', pickle_filename)
    joblib.dump(aggregator, pickle_filename, protocol=4)
    logging.info('Done pickling to %s', pickle_filename)
    return pickle_filename

PHASE_FUNCTIONS = [
    NLPTransforms().title_phase_one,
    JobTitleStringClean().clean_title
]


def title_clean(title, phase_indices):
    for phase_index in phase_indices:
        title = PHASE_FUNCTIONS[phase_index](title)
    return title


def skill_aggregate(
    job_postings,
    aggregator_constructor,
    temp_dir,
    processed_folder,
    phase_indices,
    download_folder
):
    title_cleaner = partial(title_clean, phase_indices=phase_indices)

    skills_filename = '{}/skills_master_table.tsv'\
        .format(processed_folder)

    if not os.path.isfile(skills_filename):
        download(
            s3_conn=S3Hook().get_conn(),
            out_filename=skills_filename,
            s3_path=config['output_tables']['s3_path'] + '/skills_master_table.tsv'
        )
    corpus_creator = SimpleCorpusCreator()
    job_aggregators = {'onet_skills': OccupationScopedSkillAggregator(
        corpus_creator=corpus_creator,
        skill_extractor=OccupationScopedSkillExtractor(
            skills_filename=skills_filename
        ),
        output_count=10
    )}
    aggregator = aggregator_constructor(
        job_aggregators=job_aggregators,
        title_cleaner=title_cleaner
    )
    aggregator.process_postings(job_postings)
    aggregator.job_aggregators['onet_skills'].skill_extractor = None
    aggregator.job_aggregators['onet_skills'].corpus_creator = None
    return save(
        aggregator,
        temp_dir,
    )


def classify_common(
    job_postings,
    aggregator_constructor,
    temp_dir,
    processed_folder,
    phase_indices,
    download_folder
):
    s3_conn = S3Hook().get_conn()
    corpus_creator = SimpleCorpusCreator()
    title_cleaner = partial(title_clean, phase_indices=phase_indices)

    common_classifier = Classifier(
        s3_conn=s3_conn,
        classifier_id='ann_0614',
        classify_kwargs={'mode': 'common'},
        temporary_directory=download_folder,
    )
    job_aggregators = {'soc_code_common': SocCodeAggregator(
        corpus_creator=corpus_creator,
        occupation_classifier=common_classifier,
        output_count=2,
        output_total=True
    )}

    aggregator = aggregator_constructor(
        job_aggregators=job_aggregators,
        title_cleaner=title_cleaner
    )

    aggregator.process_postings(job_postings)
    aggregator.job_aggregators['soc_code_common'].occupation_classifier = None
    aggregator.job_aggregators['soc_code_common'].corpus_creator = None
    return save(
        aggregator,
        temp_dir,
    )


def classify_top(
    job_postings,
    aggregator_constructor,
    temp_dir,
    processed_folder,
    phase_indices,
    download_folder
):
    s3_conn = S3Hook().get_conn()
    corpus_creator = SimpleCorpusCreator()

    title_cleaner = partial(title_clean, phase_indices=phase_indices)

    top_classifier = Classifier(
        s3_conn=s3_conn,
        classifier_id='ann_0614',
        classify_kwargs={'mode': 'top'},
        temporary_directory=download_folder,
    )
    job_aggregators = {'soc_code_top': SocCodeAggregator(
        corpus_creator=corpus_creator,
        occupation_classifier=top_classifier,
        output_count=2,
        output_total=True
    )}
    aggregator = aggregator_constructor(
        job_aggregators=job_aggregators,
        title_cleaner=title_cleaner
    )
    aggregator.process_postings(job_postings)
    aggregator.job_aggregators['soc_code_top'].occupation_classifier = None
    aggregator.job_aggregators['soc_code_top'].corpus_creator = None
    return save(
        aggregator,
        temp_dir,
    )


def given_soc_code(
    job_postings,
    aggregator_constructor,
    temp_dir,
    processed_folder,
    phase_indices,
    download_folder
):
    title_cleaner = partial(title_clean, phase_indices=phase_indices)

    job_aggregators = {'soc_code_given': GivenSocCodeAggregator(
        output_count=2,
        output_total=True
    )}
    aggregator = aggregator_constructor(
        job_aggregators=job_aggregators,
        title_cleaner=title_cleaner
    )
    aggregator.process_postings(job_postings)
    return save(
        aggregator,
        temp_dir,
    )


def count_aggregate(
    job_postings,
    aggregator_constructor,
    temp_dir,
    processed_folder,
    phase_indices,
    download_folder
):
    title_cleaner = partial(title_clean, phase_indices=phase_indices)
    job_aggregators = {'counts': CountAggregator()}
    aggregator = aggregator_constructor(
        job_aggregators=job_aggregators,
        title_cleaner=title_cleaner
    )
    aggregator.process_postings(job_postings)
    return save(
        aggregator,
        temp_dir,
    )


def define_title_counts(main_dag_name):
    dag = QuarterlySubDAG(main_dag_name, 'title_count')

    model_cache_config = config.get('model_cache', {})
    cache_local_path = model_cache_config.get('local_path', '/tmp')
    cache_s3_path = model_cache_config.get('s3_path', '')

    class GeoTitleCountOperator(GeoCountOperator):
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
        prefix = 'phase_one'

    class GeoTitlePhaseTwoCountOperator(GeoTitleCountOperator):
        group_config_key = 'cleaned_geo_title_count_dir'
        rollup_config_key = 'cleaned_title_count_dir'
        phase_indices = [0, 1]
        prefix = 'phase_two'

    for operator in [
        GeoTitlePhaseOneCountOperator,
        GeoTitlePhaseTwoCountOperator
    ]:
        skill = operator(
            task_id='{}_skill'.format(operator.prefix),
            map_function=skill_aggregate,
            func_name='skills',
            dag=dag
        )
        common = operator(
            task_id='{}_common_soc'.format(operator.prefix),
            map_function=classify_common,
            func_name='common_soc',
            dag=dag
        )
        top = operator(
            task_id='{}_top_soc'.format(operator.prefix),
            map_function=classify_top,
            func_name='top_soc',
            dag=dag
        )
        given = operator(
            task_id='{}_given_soc'.format(operator.prefix),
            map_function=given_soc_code,
            func_name='given_soc',
            dag=dag
        )
        count = operator(
            task_id='{}_count'.format(operator.prefix),
            map_function=count_aggregate,
            func_name='count',
            dag=dag
        )

        merge = MergeOperator(
            task_id='{}_merge'.format(operator.prefix),
            group_config_key=operator.group_config_key,
            rollup_config_key=operator.rollup_config_key,
            dag=dag
        )
        merge.set_upstream(skill)
        merge.set_upstream(common)
        merge.set_upstream(top)
        merge.set_upstream(given)
        merge.set_upstream(count)

    return dag
