import logging

from airflow.operators import BaseOperator
from airflow.hooks import S3Hook

from skills_ml.datasets.job_postings import job_postings_highmem
from skills_ml.algorithms.geocoders import S3CachedGeocoder
from skills_ml.algorithms.geocoders.cbsa import S3CachedCBSAFinder
from skills_utils.time import datetime_to_quarter

from config import config
from utils.dags import QuarterlySubDAG


class GeocodeCacheOperator(BaseOperator):
    def __init__(self, *args, **kwargs):
        kwargs['depends_on_past'] = True
        super(GeocodeCacheOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        s3_conn = S3Hook().get_conn()
        quarter = datetime_to_quarter(context['execution_date'])

        job_postings_generator = job_postings_highmem(
            s3_conn,
            quarter,
            config['job_postings']['s3_path']
        )

        geocoder = S3CachedGeocoder(
            s3_conn=s3_conn,
            cache_s3_path=config['geocoder']['s3_path']
        )
        logging.info('Starting geocoding')
        geocoder.geocode_job_postings_and_save(job_postings_generator)
        logging.info('Done geocoding')


class CBSACacheOperator(BaseOperator):
    def __init__(self, *args, **kwargs):
        kwargs['depends_on_past'] = True
        super(CBSACacheOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        s3_conn = S3Hook().get_conn()

        geocoder = S3CachedGeocoder(
            s3_conn=s3_conn,
            cache_s3_path=config['geocoder']['s3_path']
        )
        finder = S3CachedCBSAFinder(
            s3_conn=s3_conn,
            cache_s3_path=config['cbsa_lookup']['s3_path']
        )
        logging.info('Finding all CBSAs')
        finder.find_all_cbsas_and_save(geocoder.all_cached_geocodes)
        logging.info('Done finding CBSAs')


def define_geocode(main_dag_name):
    dag = QuarterlySubDAG(main_dag_name, 'geocode')

    geocode = GeocodeCacheOperator(
        dag=dag,
        task_id='geocode',
    )

    find_cbsa = CBSACacheOperator(
        dag=dag,
        task_id='cbsa',
    )

    find_cbsa.set_upstream(geocode)

    return dag
