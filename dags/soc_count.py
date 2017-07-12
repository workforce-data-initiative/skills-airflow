"""Takes common schema job listings and performs
title extraction, cleaning, and aggregation
"""
from collections import OrderedDict
from uuid import uuid4
import logging
import os
import joblib

from airflow.hooks import S3Hook

from skills_ml.algorithms.aggregators import CountAggregator
from skills_ml.algorithms.aggregators.soc_code import GeoSocAggregator
from skills_ml.algorithms.corpus_creators.basic import SimpleCorpusCreator
from skills_ml.algorithms.occupation_classifiers.classifiers import Classifier

from utils.dags import QuarterlySubDAG
from operators.geo_count import GeoCountOperator, MergeOperator


def save(aggregator, temp_dir):
    pickle_filename = os.path.join(temp_dir, str(uuid4()) + '.pkl')
    logging.info('Pickling to %s', pickle_filename)
    joblib.dump(aggregator, pickle_filename)
    logging.info('Done pickling to %s', pickle_filename)
    return pickle_filename


def soc_aggregate(
    job_postings,
    aggregator_constructor,
    temp_dir,
    classifier_id,
    classify_kwargs
):
    s3_conn = S3Hook().get_conn()
    corpus_creator = SimpleCorpusCreator()
    count_aggregator = CountAggregator()
    job_aggregators = OrderedDict(counts=count_aggregator)
    classifier = None
    if classifier_id:
        classifier = Classifier(
            classifier_id=classifier_id,
            classify_kwargs=classify_kwargs,
            temporary_directory=temp_dir,
            s3_conn=s3_conn
        )
    aggregator = aggregator_constructor(
        occupation_classifier=classifier,
        corpus_creator=corpus_creator,
        job_aggregators=job_aggregators,
    )
    aggregator.process_postings(job_postings)
    aggregator.occupation_classifier = None
    return save(aggregator, temp_dir)


def define_soc_counts(main_dag_name):
    dag = QuarterlySubDAG(main_dag_name, 'soc_count')

    class GeoSOCCountOperator(GeoCountOperator):
        def __init__(self, *args, **kwargs):
            kwargs['map_function'] = soc_aggregate
            kwargs['func_name'] = 'count'
            super(GeoSOCCountOperator, self).__init__(*args, **kwargs)

        def aggregator_constructor(self):
            return GeoSocAggregator

        def passthroughs(self):
            return {
                'classifier_id': self.classifier_id,
                'classify_kwargs': self.classify_kwargs
            }

    class GeoSOCCommonCountOperator(GeoSOCCountOperator):
        prefix = 'common'
        group_config_key = 'geo_soc_common_count_dir'
        rollup_config_key = 'soc_common_count_dir'
        classifier_id = 'ann_0614'
        classify_kwargs = {'mode': 'common'}

    class GeoSOCTopCountOperator(GeoSOCCountOperator):
        prefix = 'top'
        group_config_key = 'geo_soc_top_count_dir'
        rollup_config_key = 'soc_top_count_dir'
        classifier_id = 'ann_0614'
        classify_kwargs = {'mode': 'top'}

    class GeoSOCGivenCountOperator(GeoSOCCountOperator):
        prefix = 'given'
        group_config_key = 'geo_soc_given_count_dir'
        rollup_config_key = 'soc_given_count_dir'
        classifier_id = None
        classify_kwargs = {}

    for operator in [
        GeoSOCCommonCountOperator,
        GeoSOCTopCountOperator,
        GeoSOCGivenCountOperator
    ]:
        count = operator(
            task_id='geo_soc_{}_count'.format(operator.prefix),
            func_name='count',
            dag=dag
        )

        merge = MergeOperator(
            task_id='{}_merge'.format(operator.prefix),
            group_config_key=operator.group_config_key,
            rollup_config_key=operator.rollup_config_key,
            dag=dag
        )
        merge.set_upstream(count)

    return dag
