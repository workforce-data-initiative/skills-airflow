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
```

- For the elasticsearch indexing tasks to work, you will also need to define the `ELASTICSEARCH_ENDPOINT` environment variable to an elasticsearch 5.0 endpoint, and the `PYTHONPATH` environment variable (should set to '.').

- Run scheduler: `airflow scheduler`
