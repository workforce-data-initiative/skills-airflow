import logging
from airflow.models import BaseOperator
from airflow.hooks import S3Hook
from airflow.utils.decorators import apply_defaults
from skills_utils.s3 import upload_dict
from skills_utils.time import datetime_to_quarter
from datetime import datetime


class PartnerSnapshotOperator(BaseOperator):
    @apply_defaults
    def __init__(
            self,
            updater_class,
            passthrough_kwargs,
            s3_prefix,
            *args, **kwargs):
        super(PartnerSnapshotOperator, self).__init__(*args, **kwargs)
        self.updater_class = updater_class
        self.passthrough_kwargs = passthrough_kwargs
        self.s3_prefix = s3_prefix

    def execute(self, context):
        conn = S3Hook().get_conn()
        execution_date = context['execution_date']
        quarter = datetime_to_quarter(execution_date)
        if quarter != datetime_to_quarter(datetime.now()):
            logging.warning('PartnerSnapshotOperator cannot be backfilled. Skipping')
            return
        updater = self.updater_class(**(self.passthrough_kwargs))
        postings = updater.deduplicated_postings()
        upload_dict(
            s3_conn=conn,
            s3_prefix=self.s3_prefix + '/' + quarter,
            data_to_sync=postings
        )
