# DAGs

TODO: Intro

## Adding a task to the open_skills_master DAG

1. Determine where it fits

What other pieces of data does the task you want to write depend on? You can inspect the sub DAGs in dags/open_skills_master.py, or via the Airflow web UI to see the overall workflow structure. It's possible that your task makes most sense as part of an existing sub DAG (for instance, writing a new v1 API database table would belong as a new task within api_sync_v1), or as a new	sub DAG. The choice here has some room for subjectivity; we generally try and group related tasks with the same general data dependency together into sub DAGs when it makes sense, without letting these sub DAGs get too big. The key is to keep the master DAG small enough to understand the overall data flow at a glance, while hiding the appropriate amount of implementation within sub DAGs.

### Creating a new Sub DAG
If you need to create a new sub DAG, now is a good time to put the high-level definition where it belongs in the master DAG and defining its dependencies, before moving into implementation.  Within the master DAG at `dags/open_skills_master.py` there are several steps to importing a sub DAG and defining its dependencies.

* Add import at the top (example: `from dags.tabular_upload import define_tabular_upload`)
* Define DAG which is usable as a sub DAG (example: `tabular_upload_dag = define_tabular_upload(MAIN_DAG_NAME)`)
* Instantiate SubDAGOperator with defined DAG (example: `tabular_upload = SubDagOperator(subdag=tabular_upload_dag, task_id='tabular_upload', dag=dag)`)
* Define each upstream dependency (example: `tabular_upload.set_upstream(title_count)`)
* Create a boilerplate DAG definition: example:
```
"""Uploads tabular datasets to a public bucket, along with a README"""
from airflow.operators import BaseOperator
from utils.dags import QuarterlySubDAG


def define_tabular_upload(main_dag_name):
    dag = QuarterlySubDAG(main_dag_name, 'tabular_upload')

    class TabularUploadOperator(BaseOperator):
        def execute(self, context):
			pass

    TabularUploadOperator(task_id='tabular_upload', dag=dag)

    return dag
```

There is some Airflow boilerplate here, which results in a function that creates a sub DAG, running on a quarterly schedule, that currently does nothing.


### Adding an Operator to an existing SubDAG

If your new code makes more sense within an existing SubDAG, then you generally just need to add a new operator and place it in the DAG, along with defining any intra-DAG dependencies
```
    class TabularUploadOperator(BaseOperator):
        def execute(self, context):
			pass

    tabular_upload = TabularUploadOperator(task_id='tabular_upload', dag=dag)
	tabular_upload.set_upstream(???)
```

2. Implementing the task

At this point, you have a skeleton for where your code should go (the body of the execute method). A couple things are important to keep in mind:

* Since this repository is meant for orchestration and not for algorithms, the code in the execute method tends to be fairly simple. Most of these involve gathering dependencies and configuration (such as file paths, s3 paths, s3 connections) and passing them to an imported function that implements some algorithmic work.

* The Open Skills Master DAG is on a quarterly schedule. This means that your operator will be instantiated with a quarter, and is expected to only process data relevant to that quarter. Sometimes, what this involves is working on input files and generating output files that only represent one quarter. In cases where there is one resource or file that is being produced across all quarters, that could mean using a merge strategy to merge one quarter's data into an existing file. Your code is also expected to be idempotent; if you are merging a quarter's worth of data into a file, make sure you deal with the possibility of there already being data from this quarter in that file.
