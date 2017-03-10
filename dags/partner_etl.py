"""Ingest job listing data from partners, converting into common schema"""
from skills_ml.datasets.raw_job_postings import importers
try:
    from dags.private import importers as private_importers
    importers.update(private_importers)
except ImportError:
    pass

from skills_utils.s3 import split_s3_path

from config import config
from operators.partner_etl import PartnerETLOperator
from utils.dags import QuarterlySubDAG


def define_partner_etl(main_dag_name):
    dag = QuarterlySubDAG(main_dag_name, 'partner_etl')

    raw_jobs = config.get('raw_jobs_s3_paths', {})
    if not raw_jobs:
        return dag
    bucket, prefix = split_s3_path(config['job_postings']['s3_path'])

    for partner_id, s3_path in raw_jobs.items():
        importer_class = importers[partner_id]

        input_bucket, input_prefix = split_s3_path(s3_path)

        PartnerETLOperator(
            task_id=partner_id,
            dag=dag,
            transformer_class=importer_class,
            output_bucket=bucket,
            output_prefix=prefix,
            partner_id=partner_id,
            passthrough_kwargs={
                'bucket_name': input_bucket,
                'prefix': input_prefix,
            }
        )

    return dag
