from airflow import DAG
from operators.partner_update import PartnerUpdateOperator
from datetime import datetime
from config import config

from skills_utils.s3 import split_s3_path

default_args = {
    'depends_on_past': False,
    'start_date': datetime(2010, 1, 1),
}


def define_partner_update(main_dag_name):
    dag = DAG(
        dag_id='{}.partner_update'.format(main_dag_name),
        default_args=default_args,
        schedule_interval='@once'
    )

    raw_jobs = config.get('raw_jobs_s3_paths', {})
    if not raw_jobs:
        return dag
    va_bucket, va_prefix = split_s3_path(raw_jobs['VA'])
    PartnerUpdateOperator(
        task_id='va_jobs_update',
        dag=dag,
        sources=[
            'http://opendata.cs.vt.edu/dataset/2002e48d-363e-40d1-9d1b-a134301126a7/resource/d40efa75-ed86-4a01-9854-90d27539d477/download/joblistings.merged.parsed.unique.grpbyyear.2010-2015.01.json',
            'http://opendata.cs.vt.edu/dataset/2002e48d-363e-40d1-9d1b-a134301126a7/resource/c7d4d3e6-61fb-4985-920d-7ac20732083d/download/joblistings.merged.parsed.unique.grpbyyear.2010-2015.02.json',
            'http://opendata.cs.vt.edu/dataset/2002e48d-363e-40d1-9d1b-a134301126a7/resource/638255b0-cd2f-4b34-8abb-cf46f075bdfd/download/joblistings.merged.parsed.unique.grpbyyear.2010-2015.03.json',
            'http://opendata.cs.vt.edu/dataset/2002e48d-363e-40d1-9d1b-a134301126a7/resource/7e14bb60-2474-420b-ae7b-62b195051f1f/download/joblistings.merged.parsed.unique.grpbyyear.2010-2015.04.json',
            'http://opendata.cs.vt.edu/dataset/b67a5b8e-679a-4442-a8c8-4bb55a4618d6/resource/62da570a-6970-46de-b206-dab067ba51eb/download/joblistings.merged.parsed.unique.grpbyyear.2016.json'],
        output_bucket=va_bucket,
        output_prefix=va_prefix,
        cache_headers=['Content-Range'],
    )

    return dag
