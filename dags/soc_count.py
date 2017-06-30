"""Takes common schema job listings and performs
title extraction, cleaning, and aggregation
"""
from collections import OrderedDict

from airflow.hooks import S3Hook

from skills_ml.algorithms.aggregators import CountAggregator
from skills_ml.algorithms.aggregators.soc_code import GeoSocAggregator
from skills_ml.algorithms.corpus_creators.basic import SimpleCorpusCreator
from skills_ml.algorithms.occupation_classifiers.classifiers import Classifier

from utils.dags import QuarterlySubDAG
from operators.geo_count import GeoCountOperator


def soc_aggregate(job_postings, aggregator_constructor, classifier_id, classify_kwargs):
    s3_conn = S3Hook().get_conn()
    corpus_creator = SimpleCorpusCreator()
    count_aggregator = CountAggregator()
    job_aggregators = OrderedDict(counts=count_aggregator)
    classifier = None
    if classifier_id:
        classifier = Classifier(
            classifier_id=classifier_id,
            classify_kwargs=classify_kwargs,
            s3_conn=s3_conn
        )
    aggregator = aggregator_constructor(
        occupation_classifier=classifier,
        corpus_creator=corpus_creator,
        job_aggregators=job_aggregators,
    )
    aggregator.process_postings(job_postings)
    return aggregator.job_aggregators


def define_soc_counts(main_dag_name):
    dag = QuarterlySubDAG(main_dag_name, 'soc_count')

    class GeoSOCCountOperator(GeoCountOperator):
        def aggregator_constructor(self):
            return GeoSocAggregator

        def passthroughs(self):
            return {
                'classifier_id': self.classifier_id,
                'classify_kwargs': self.classify_kwargs
            }

        map_function = soc_aggregate

    class GeoSOCCommonCountOperator(GeoSOCCountOperator):
        group_config_key = 'geo_soc_common_count_dir'
        rollup_config_key = 'soc_common_count_dir'
        classifier_id = 'ann_0614'
        classify_kwargs = {'mode': 'common'}

    class GeoSOCTopCountOperator(GeoSOCCountOperator):
        group_config_key = 'geo_soc_top_count_dir'
        rollup_config_key = 'soc_top_count_dir'
        classifier_id = 'ann_0614'
        classify_kwargs = {'mode': 'top'}

    class GeoSOCGivenCountOperator(GeoSOCCountOperator):
        group_config_key = 'geo_soc_given_count_dir'
        rollup_config_key = 'soc_given_count_dir'
        classifier_id = None

    GeoSOCCommonCountOperator(task_id='geo_soc_common_count', dag=dag)
    GeoSOCTopCountOperator(task_id='geo_soc_top_count', dag=dag)
    GeoSOCGivenCountOperator(task_id='geo_soc_given_count', dag=dag)

    return dag
