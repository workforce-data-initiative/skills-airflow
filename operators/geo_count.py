import logging
import os
from functools import partial
from multiprocessing import Pool

from airflow.hooks import S3Hook
from airflow.operators import BaseOperator

from skills_ml.datasets.job_postings import job_postings_highmem
from skills_utils.time import datetime_to_quarter
from skills_utils.iteration import Batch
from skills_utils.s3 import upload

from config import config


class GeoCountOperator(BaseOperator):
    def map(self, pool, job_postings_generator):
        passthroughs = self.passthroughs()
        bound_mapping_function = partial(
            self.map_function.__func__,
            aggregator_constructor=self.aggregator_constructor(),
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

    def reduce(self, aggregators):
        combined_aggregator = self.aggregator_constructor()(
            job_aggregators=next(aggregators)
        )
        for aggregator in aggregators:
            combined_aggregator.merge_job_aggregators(aggregator)
        return combined_aggregator

    def output_folder(self):
        output_folder = config.get('output_folder', 'output')
        if not os.path.isdir(output_folder):
            os.mkdir(output_folder)
        return output_folder

    def save(self, combined_aggregator, quarter, s3_conn):
        count_filename = '{}/{}/{}.csv'.format(
            self.output_folder(),
            config['output_tables'][self.group_config_key],
            quarter
        )
        rollup_filename = '{}/{}/{}.csv'.format(
            self.output_folder(),
            config['output_tables'][self.rollup_config_key],
            quarter
        )
        combined_aggregator.save_counts(count_filename)
        combined_aggregator.save_rollup(rollup_filename)

        logging.info(
            'Found %s count rows and %s rollup rows for %s',
            len(combined_aggregator.job_aggregators['counts'].group_values.keys()),
            len(combined_aggregator.job_aggregators['counts'].rollup.keys()),
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

    def execute(self, context):
        s3_conn = S3Hook().get_conn()
        quarter = datetime_to_quarter(context['execution_date'])

        job_postings_generator = job_postings_highmem(
            s3_conn,
            quarter,
            config['job_postings']['s3_path']
        )

        pool = Pool(processes=config['aggregation']['n_processes'])
        combined_aggregator = self.reduce(
            self.map(pool, job_postings_generator)
        )
        self.save(combined_aggregator, quarter, s3_conn)
