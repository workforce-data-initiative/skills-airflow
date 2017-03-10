# skills-airflow
Orchestration of data processing tasks to power the Open Skills API

## Getting Started
- Create virtualenv (ie `virtualenv -p /usr/bin/python3.4 venv`)

- Install requirements: `pip install -r requirements.txt` and `pip install -r requirements_dev.txt`

- Quick airflow setup and run webserver: `sh airflow_setup.sh`

- Upon completion of the prior task, the airflow webserver will launch.

- Modify the config file to match locations of needed data:

```
onet:
    s3_path: # an s3 path (starting with bucket) to an ONET extract
    cache_dir: # the required relative local directory for caching ONET data
job_postings:
    s3_path: # an s3 path for common schema job postings
labeled_postings:
    s3_path: # an s3 path to output job postings with labeled skills
output_tables:
    s3_path: # an s3 path to output tabular data
local_mode: False # whether or not to no-op all s3 communication
output_folder: # a relative local directory for outputting tabular data
normalizer:
    es_index_name: # name of desired elasticsearch normalizer index name
    titles_master_index_name: # name of desired job title index name
raw_jobs_s3_paths:
    XX: 'some-bucket/jobs/dump/' # key is partner code, value is s3 path to partner's s3 path
airflow_contacts: # a list of email addresses to send errors to
    - myemail@email.com
```

- For the elasticsearch indexing tasks to work, you will also need to define the `ELASTICSEARCH_ENDPOINT` environment variable to an elasticsearch 5.0 endpoint, and the `PYTHONPATH` environment variable (should set to '.').

- Run scheduler: `airflow scheduler`


## Adding private job listing data sources

The PartnerETL DAG is designed to mix job listings from private sources and public sources without adding any private-source specific code into this public repository. If you'd like to add ETL for a private data source, follow these steps:

- Subclass JobPostingImportBase from the skills_utils public repository; this involves implementing an `_iter_postings` to define business logic to iterate through available postings for a given quarter, a `_transform` method to transform a given posting into the common schema, and an `_id` method to define a source-unique id for a given posting. Reference: https://github.com/workforce-data-initiative/skills-utils/blob/master/skills_utils/job_posting_import.py

- Make this available in an __init__.py. Give the source a unique prefix, and define an `importers` dict in the __init__.py that points to the JobPostingImportBase subclass. (ie `importers = {'CB': CareerBuilderTransformer}`. Puts this __init__.py in `dags/private`. This path is ignored by git and will be imported only if present.

- Add any s3 paths to your config.yaml's `raw_jobs_s3_paths` dictionary, keyed on the prefix you chose.

The PartnerETL DAG will instantiate an operator for all private data sources and run the quarterly ETL tasks.
