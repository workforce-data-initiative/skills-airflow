"""Takes common schema job listings and performs
title extraction, cleaning, and aggregation
"""
import logging
import os
from collections import OrderedDict

from airflow.hooks import S3Hook
from airflow.operators import BaseOperator

from skills_utils.time import datetime_to_quarter
from skills_utils.s3 import upload
from skills_ml.datasets.job_postings import job_postings_highmem
from skills_ml.algorithms.aggregators import\
    CountAggregator, SkillAggregator, SocCodeAggregator
from skills_ml.algorithms.aggregators.title import GeoTitleAggregator
from skills_ml.algorithms.corpus_creators.basic import SimpleCorpusCreator
from skills_ml.algorithms.jobtitle_cleaner.clean import JobTitleStringClean
from skills_ml.algorithms.string_cleaners import NLPTransforms
from skills_ml.algorithms.skill_extractors.freetext import FreetextSkillExtractor
from skills_ml.algorithms.occupation_classifiers.classifiers import Classifier
from config import config

from utils.dags import QuarterlySubDAG


def define_title_counts(main_dag_name):
    dag = QuarterlySubDAG(main_dag_name, 'title_count')

    output_folder = config.get('output_folder', 'output')
    if not os.path.isdir(output_folder):
        os.mkdir(output_folder)

    class GeoTitleCountOperator(BaseOperator):
        def execute(self, context):
            s3_conn = S3Hook().get_conn()
            quarter = datetime_to_quarter(context['execution_date'])

            count_filename = '{}/{}/{}.csv'.format(
                output_folder,
                config['output_tables']['geo_title_count_dir'],
                quarter
            )
            rollup_filename = '{}/{}/{}.csv'.format(
                output_folder,
                config['output_tables']['title_count_dir'],
                quarter
            )

            job_postings_generator = job_postings_highmem(
                s3_conn,
                quarter,
                config['job_postings']['s3_path']
            )
            skills_filename = '{}/skills_master_table.tsv'\
                .format(output_folder)
            title_cleaner = NLPTransforms().title_phase_one
            corpus_creator = SimpleCorpusCreator()
            common_classifier = Classifier(
                s3_conn=s3_conn,
                classifier_id='ann_0614',
                classify_kwargs={'mode': 'common'}
            )
            top_classifier = Classifier(
                s3_conn=s3_conn,
                classifier=common_classifier.classifier,
                classifier_id='ann_0614',
                classify_kwargs={'mode': 'top'}
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
            aggregator = GeoTitleAggregator(
                job_aggregators=job_aggregators,
                title_cleaner=title_cleaner
            )
            aggregator.process_postings(job_postings_generator)
            aggregator.save_counts(count_filename)
            aggregator.save_rollup(rollup_filename)

            logging.info(
                'Found %s count rows and %s title rollup rows for %s',
                len(aggregator.job_aggregators[0].group_values.keys()),
                len(aggregator.job_aggregators[0].rollup.keys()),
                quarter,
            )
            upload(
                s3_conn,
                count_filename,
                '{}/{}'.format(
                    config['output_tables']['s3_path'],
                    config['output_tables']['geo_title_count_dir']
                )
            )
            upload(
                s3_conn,
                rollup_filename,
                '{}/{}'.format(
                    config['output_tables']['s3_path'],
                    config['output_tables']['title_count_dir']
                )
            )

    class JobTitleCleanOperator(BaseOperator):
        def execute(self, context):

            s3_conn = S3Hook().get_conn()
            quarter = datetime_to_quarter(context['execution_date'])

            count_filename = '{}/{}/{}.csv'.format(
                output_folder,
                config['output_tables']['cleaned_geo_title_count_dir'],
                quarter
            )
            rollup_filename = '{}/{}/{}.csv'.format(
                output_folder,
                config['output_tables']['cleaned_title_count_dir'],
                quarter
            )

            job_postings_generator = job_postings_highmem(
                s3_conn,
                quarter,
                config['job_postings']['s3_path']
            )
            skills_filename = '{}/skills_master_table.tsv'\
                .format(output_folder)

            first_phase_cleaner = NLPTransforms().title_phase_one
            second_phase_cleaner = JobTitleStringClean().clean_title

            def two_phases(title):
                return second_phase_cleaner(first_phase_cleaner(title))

            corpus_creator = SimpleCorpusCreator()
            common_classifier = Classifier(
                s3_conn=s3_conn,
                classifier_id='ann_0614',
                classify_kwargs={'mode': 'common'}
            )
            top_classifier = Classifier(
                s3_conn=s3_conn,
                classifier=common_classifier.classifier,
                classifier_id='ann_0614',
                classify_kwargs={'mode': 'top'}
            )
            job_aggregators = OrderedDict()
            job_aggregators['counts'] = CountAggregator()
            job_aggregators['skills'] = SkillAggregator(
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
            aggregator = GeoTitleAggregator(
                job_aggregators=job_aggregators,
                title_cleaner=two_phases
            )
            aggregator.process_postings(job_postings_generator)
            aggregator.save_counts(count_filename)
            aggregator.save_rollup(rollup_filename)

            logging.info(
                'Found %s count rows and %s title rollup rows for %s',
                len(aggregator.job_aggregators[0].group_values.keys()),
                len(aggregator.job_aggregators[0].rollup.keys()),
                quarter,
            )
            upload(
                s3_conn,
                count_filename,
                '{}/{}'.format(
                    config['output_tables']['s3_path'],
                    config['output_tables']['cleaned_geo_title_count_dir']
                )
            )
            upload(
                s3_conn,
                rollup_filename,
                '{}/{}'.format(
                    config['output_tables']['s3_path'],
                    config['output_tables']['cleaned_title_count_dir']
                )
            )

    GeoTitleCountOperator(task_id='clean_phase_one_geo_count', dag=dag)
    JobTitleCleanOperator(task_id='clean_phase_two_geo_count', dag=dag)

    return dag
