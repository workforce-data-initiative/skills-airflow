from airflow.models import BaseOperator
from airflow.hooks import S3Hook
from airflow.utils.decorators import apply_defaults
from skills_utils.iteration import Batch
import logging
import tempfile
import requests
import boto
import uuid
import json


class PartnerUpdateOperator(BaseOperator):
    @apply_defaults
    def __init__(
            self,
            sources,
            output_bucket,
            output_prefix,
            cache_headers=['Date', 'Last-Modified'],
            *args, **kwargs):
        super(PartnerUpdateOperator, self).__init__(*args, **kwargs)
        self.sources = sources
        self.output_bucket = output_bucket
        self.output_prefix = output_prefix
        self.cache_headers = cache_headers
        self.postings_per_file = 10000

    def execute(self, context):
        conn = S3Hook().get_conn()
        bucket = conn.get_bucket(self.output_bucket)

        for url in self.sources:
            name = url.split('/')[-1]
            r = requests.get(url, stream=True)

            # Check the remote headers against the stored headers
            cache_dict = {k: r.headers[k] for k in self.cache_headers}
            cache_key = boto.s3.key.Key(
                bucket=bucket,
                name='{}/{}/.cache.json'.format(self.output_prefix, name)
            )
            if cache_key.exists():
                logging.info("Checking %s for updates", name)
                stored_cache = json.loads(cache_key.get_contents_as_string())
                if cache_dict == stored_cache:
                    logging.info("Skipping %s", name)
                    continue
            logging.info("Downloading %s", name)
            # Cached headers differ; DELETE ALL EXISTING DATA
            for key in bucket.list(
                prefix="{}/{}/".format(self.output_prefix, name)
            ):
                key.delete()
            cache_key.set_contents_from_string(json.dumps(cache_dict))

            for batch in Batch(r.iter_lines(), self.postings_per_file):
                key = boto.s3.key.Key(
                    bucket=bucket,
                    name='{}/{}/{}'.format(
                        self.output_prefix,
                        name,
                        str(uuid.uuid4()) + ".json"
                    )
                )
                with tempfile.TemporaryFile() as f:
                    for posting in batch:
                        f.write(posting)
                        f.write('\n')
                    f.seek(0)
                    key.set_contents_from_file(f)
