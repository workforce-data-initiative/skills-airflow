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
from skills_ml.algorithms.aggregators import CountAggregator
from skills_ml.algorithms.aggregators.soc_code import GeoSocAggregator
from skills_ml.algorithms.corpus_creators.basic import SimpleCorpusCreator
from skills_ml.algorithms.occupation_classifiers.classifiers import Classifier
from config import config

from utils.dags import QuarterlySubDAG


def define_soc_counts(main_dag_name):
    dag = QuarterlySubDAG(main_dag_name, 'soc_count')

    output_folder = config.get('output_folder', 'output')
    if not os.path.isdir(output_folder):
        os.mkdir(output_folder)

    class GeoSOCCountOperator(BaseOperator):
        def classifier(self, s3_conn):
            raise NotImplementedError()

        def execute(self, context):
            s3_conn = S3Hook().get_conn()
            quarter = datetime_to_quarter(context['execution_date'])

            count_filename = '{}/{}/{}.csv'.format(
                output_folder,
                config['output_tables'][self.group_config_key],
                quarter
            )
            rollup_filename = '{}/{}/{}.csv'.format(
                output_folder,
                config['output_tables'][self.rollup_config_key],
                quarter
            )

            job_postings_generator = job_postings_highmem(
                s3_conn,
                quarter,
                config['job_postings']['s3_path']
            )
            corpus_creator = SimpleCorpusCreator()
            count_aggregator = CountAggregator()
            job_aggregators = OrderedDict(counts=count_aggregator)
            aggregator = GeoSocAggregator(
                occupation_classifier=self.classifier(s3_conn),
                corpus_creator=corpus_creator,
                job_aggregators=job_aggregators,
            )
            aggregator.process_postings(job_postings_generator)
            aggregator.save_counts(count_filename)
            aggregator.save_rollup(rollup_filename)

            logging.info(
                'Found %s count rows and %s title rollup rows for %s',
                len(count_aggregator.group_values.keys()),
                len(count_aggregator.rollup.keys()),
                quarter,
            )
            upload(
                s3_conn,
                count_filename,
                '{}/{}'.format(
                    config['output_tables']['s3_path'],
                    config['output_tables'][self.group_config_key]
                )
            )
            upload(
                s3_conn,
                rollup_filename,
                '{}/{}'.format(
                    config['output_tables']['s3_path'],
                    config['output_tables'][self.rollup_config_key]
                )
            )

    class GeoSOCCommonCountOperator(BaseOperator):
        group_config_key = 'geo_soc_common_count_dir'
        rollup_config_key = 'soc_common_count_dir'

        def classifier(self, s3_conn):
            return Classifier(
                s3_conn=s3_conn,
                classifier_id='ann_0614',
                classify_kwargs={'mode': 'common'}
            )

    class GeoSOCTopCountOperator(BaseOperator):
        group_config_key = 'geo_soc_top_count_dir'
        rollup_config_key = 'soc_top_count_dir'

        def classifier(self, s3_conn):
            return Classifier(
                s3_conn=s3_conn,
                classifier_id='ann_0614',
                classify_kwargs={'mode': 'top'}
            )

    GeoSOCCommonCountOperator(task_id='geo_soc_common_count', dag=dag)
    GeoSOCTopCountOperator(task_id='geo_soc_top_count', dag=dag)

    return dag
