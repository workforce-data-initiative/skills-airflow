# skills-airflow
Orchestration of data processing tasks to power the Open Skills API

## Dependencies

- Postgresql database (tested on 9.5 but could work on earlier version)
- Elasticsearch (5.x) cluster

## Getting Started
- Create virtualenv (ie `virtualenv -p /usr/bin/python3.4 venv`)

- Install requirements: `pip install -r requirements.txt` and `pip install -r requirements_dev.txt`

- Copy [config/api_v1_db_example.yaml](config/api_v1_db_example.yaml) to `config/api_v1_db_config.yaml` and modify with your Postgres DB information

- Upgrade database: `alembic upgrade head`

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
usa_jobs_credentials: # if you want to sync USAJobs data, include your API key information
    auth_key: 'akey'
    key_email: 'anemail@email.com'
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

## Modifying the API database

The API database is managed using the [Alembic](http://alembic.zzzcomputing.com/en/latest/index.html) schema migrations library to make the process of incremental API upgrades easy and reproducible.

To make sure alembic is set up correctly (it should be once installed from requirements.txt), try running the following command to see the migration history:

`alembic history`

You should see output similar to this:


```
89028ebc40d1 -> d523e7e45456 (head), Added ksa_type
465785295fcb -> 89028ebc40d1, empty message
40f2531a009a -> 465785295fcb, empty message
3e88156664e1 -> 40f2531a009a, empty message
3a1028f9d733 -> 3e88156664e1, empty message
4c7ffc4b5b09 -> 3a1028f9d733, empty message
502662155e36 -> 4c7ffc4b5b09, empty message
838b8a109034 -> 502662155e36, empty message
b6ad26785b00 -> 838b8a109034, empty message
bc3dd5c62a9e -> b6ad26785b00, empty message
<base> -> bc3dd5c62a9e, empty message
```

The hashes refer to filenames in the [alembic/versions](alembic/versions) directory, each of which is a separate schema migration.

### Adding a column

The database tables are defined in [api_sync/v1/models](api_sync/v1/models) in SQLAlchemy Declarative format. Alembic's migration auto-generator can pick up many changes to these files, such as new columns or tables.

- `alembic revision --autogenerate -m "Added new column"` (creates a new migration based on changes to the models not present in the database)
- `alembic upgrade head` (Upgrades the database to the latest migration)

For a more complete view of what is possible with Alembic, visit its [tutorial](http://alembic.zzzcomputing.com/en/latest/tutorial.html).

Once a column or table is created, data can be loaded into it from the [api_sync DAG](dags/api_sync_v1.py). New operators are defined in this DAG for each table, which generally call loaders defined in [api_sync/v1](api_sync/v1) to do the lower-level data loading.
