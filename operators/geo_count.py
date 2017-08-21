import logging
import os
from functools import partial
from multiprocessing import Pool
import tempfile

from airflow.hooks import S3Hook
from airflow.operators import BaseOperator

from skills_ml.datasets.job_postings import job_postings_highmem
from skills_ml.algorithms.job_geography_queriers import JobCBSAFromGeocodeQuerier
from skills_ml.algorithms.geocoders.cbsa import S3CachedCBSAFinder
from skills_utils.time import datetime_to_quarter
from skills_utils.iteration import Batch
from skills_utils.s3 import upload, split_s3_path
import pandas

from config import config
import joblib


class GeoCountOperator(BaseOperator):
    def __init__(self, *args, **kwargs):
        if 'map_function' in kwargs:
            self.map_function = kwargs.pop('map_function')

        if 'func_name' in kwargs:
            self.func_name = kwargs.pop('func_name')

        super(GeoCountOperator, self).__init__(*args, **kwargs)

    def map(self, pool, job_postings_generator, geo_querier, temp_dir):
        passthroughs = self.passthroughs()
        aggregator_constructor_with_geo_querier = partial(
            self.aggregator_constructor(),
            geo_querier=geo_querier
        )

        bound_mapping_function = partial(
            self.map_function,
            aggregator_constructor=aggregator_constructor_with_geo_querier,
            temp_dir=temp_dir,
            **passthroughs
        )
        batched_dataset = (
            list(batch) for batch in
            Batch(
                iterable=job_postings_generator,
                limit=config['aggregation']['batch_size']
            )
        )
        logging.info('Processing batched dataset')
        return pool.imap_unordered(bound_mapping_function, batched_dataset)

    def reduce(self, pickle_filenames):
        first_pickle_filename = next(pickle_filenames)
        aggregator_obj = joblib.load(first_pickle_filename)

        for pickle_filename in pickle_filenames:
            logging.info('Merging aggregations from %s', pickle_filename)
            aggregator_obj.merge_job_aggregators(joblib.load(pickle_filename).job_aggregators)
        return aggregator_obj

    def output_folder(self):
        output_folder = config.get('output_folder', 'output')
        if not os.path.isdir(output_folder):
            os.mkdir(output_folder)
        return output_folder

    def save(self, combined_aggregator, quarter, s3_conn):
        logging.info('Saving group counts and rollup')
        count_folder = '{}/{}'.format(
            self.output_folder(),
            config['output_tables'][self.group_config_key]
        )
        if not os.path.isdir(count_folder):
            os.makedirs(count_folder)

        count_filename = '{}/{}_{}.csv'.format(
            count_folder,
            quarter,
            self.func_name
        )

        rollup_folder = '{}/{}'.format(
            self.output_folder(),
            config['output_tables'][self.rollup_config_key],
        )

        if not os.path.isdir(rollup_folder):
            os.makedirs(rollup_folder)

        rollup_filename = '{}/{}_{}.csv'.format(
            rollup_folder,
            quarter,
            self.func_name
        )
        combined_aggregator.save_counts(count_filename)
        combined_aggregator.save_rollup(rollup_filename)

        logging.info('Uploading to s3')
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

    def execute(self, context):
        s3_conn = S3Hook().get_conn()
        quarter = datetime_to_quarter(context['execution_date'])

        with tempfile.TemporaryDirectory() as temp_dir:
            job_postings_generator = job_postings_highmem(
                s3_conn,
                quarter,
                config['job_postings']['s3_path']
            )
            geo_querier = JobCBSAFromGeocodeQuerier(
                cbsa_results=S3CachedCBSAFinder(
                    s3_conn=s3_conn,
                    cache_s3_path=config['cbsa_lookup']['s3_path']
                ).all_cached_cbsa_results
            )

            logging.basicConfig(
                format='%(asctime)s %(process)d %(levelname)s: %(message)s'
            )
            with Pool(processes=config['aggregation']['n_processes']) as pool:
                it = self.map(
                    pool=pool,
                    job_postings_generator=job_postings_generator,
                    geo_querier=geo_querier,
                    temp_dir=temp_dir
                )
                combined_agg = self.reduce(it)
            self.save(combined_agg, quarter, s3_conn)


def download_with_prefix(s3_conn, s3_prefix, out_directory):
    bucket_name, prefix = split_s3_path(s3_prefix)
    bucket = s3_conn.get_bucket(bucket_name)
    out_filenames = []
    for key in bucket.list(prefix=prefix):
        leaf_name = key.name.split('/')[-1]
        out_filename = os.path.join(out_directory, leaf_name)
        key.get_contents_to_filename(out_filename)
        out_filenames.append(out_filename)
    return out_filenames


def merge(s3_conn, config_key, quarter, output_folder):
    prefix = '{}{}/{}'.format(
        config['output_tables']['s3_path'],
        config['output_tables'][config_key],
        quarter
    )
    files = download_with_prefix(s3_conn, prefix + '_', output_folder)
    merge_df = pandas.read_csv(files[0])
    for other_file in files[1:]:
        merge_df = merge_df.merge(pandas.read_csv(other_file))
    merged_filename = os.path.join(output_folder, quarter + '.csv')
    merge_df.to_csv(merged_filename, index=False)

    upload(
        s3_conn,
        merged_filename,
        '{}/{}'.format(
            config['output_tables']['s3_path'],
            config['output_tables'][config_key]
        )
    )


class MergeOperator(BaseOperator):
    def __init__(self, *args, **kwargs):
        if 'group_config_key' in kwargs:
            self.group_config_key = kwargs.pop('group_config_key')

        if 'rollup_config_key' in kwargs:
            self.rollup_config_key = kwargs.pop('rollup_config_key')

        super(MergeOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        s3_conn = S3Hook().get_conn()
        quarter = datetime_to_quarter(context['execution_date'])
        output_folder = config.get('output_folder', 'output')
        if not os.path.isdir(output_folder):
            os.mkdir(output_folder)

        merge(s3_conn, self.group_config_key, quarter, output_folder)
        merge(s3_conn, self.rollup_config_key, quarter, output_folder)
