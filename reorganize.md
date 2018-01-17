# Skills-Airflow Rearchitecture

This document is to describe the problems with the current skills-airflow architecture for processing job postings and two proposals for fixing it.

## The Problems

### Quarterly Partitions
The way data is currently transformed into the system and processed is through quarterly partitions. Convenience functions are available to operate on the entire dataset, but operating on smaller partitions of the data is not easy. Each quarterly partition can be quite large, and processing a full quarter of data can represent a lot of computation time. So far this has been worked around by using multiprocessing to parallelize the dataset within it. This sometimes works just fine, but managing memory on the instances is tough, and often there are code errors that don't present themselves until a million job postings in.

### Aggregation and Processing are Conflated
After the ETL step, immediately comes an aggregation step. This is divided into subtasks that each perform one specific type of aggregation, like for instance a specific type of skill extraction, or occupational classification, on the entire quarter. This outputs aggregate data in CSV form, similar to how our research hub datasets are structured.

This data is fully aggregated, in a way that prevents us from later doing different forms of aggregation without reprocessing the data: for instance, our research hub datasets define the 'top 10 ONET skills' for a particular row. If you decide that you want to change 10 to 15, there is no saved data with which to do this. You also can't combine rows of the CSV. There are no skill counts, so if someone post-processing the data wants to merge two rows together (similar job titles that normalize together, for instance, or analyzing SOC major groups instead of individual codes, or analyzing a different temporal rollup level), they cannot do this based on the output. They have to run the entire reprocessing task again

This means that every single rollup level has to be separately computed. So far, our only rollup level is by CBSA or overall.  We have custom code in skills-ml to specifically compute both of these rollup levels side-by-side without doing work twice, but this isn't extensible. Any other desired rollup level either needs custom code to avoid duplicating work.

As a side effect, it also makes it tough for internal researchers to see how an individual job posting was processed. What skills did we extract from a particular job posting? What SOC code did we assign to a particular job posting? We don't know without manually running it ourselves.

## Potential Solution Options

### Spark/Dask
In theory, maintaining and using a Spark cluster for these expensive computations would help the quarterly partitioning problem. I am assuming that Spark would be a more robust way to do these large computational loads than multiprocessing. I would have to learn to maintain a Spark cluster, which is doable but it is work. However, many of the problems stem from code errors, that Spark doesn't do anything to fix. If there is a code error halfway through, the fact that Spark is controlling the infrastructure doesn't solve any of these problems so we can still get into a loop of restarting these big tasks a bunch of times.

I also would prefer to not require the use of a Spark cluster to run our code (though this is not a dealbreaker if it does help us out a lot). Dask seems like a halfway point, that does what Spark does but also allows somebody to run it on their laptop without complicated dependencies if they wish.

### Reorganize the Data and Split up Processing and Aggregation
Another proposal involves a few related changes, which somewhat depend on the other.

*Partition and process the data daily instead of quarterly*. Breaking up our units of work into smaller, more manageable pieces would ease the burden on whatever is processing the code. The choice of quarter is merely one of output - we want to produce end results quarterly, but this aggregation can happen at the end.

Daily processing would obviate the need implement multicore code in our libraries. Airflow's task scheduling takes care of all of the parallelism we need.

However, given our problems with rollup levels, we would be unable to produce quarterly output without doing the next change.

*Save job-posting level attributes to S3*

Our system can be more resilient to failure if we save more pieces of data on each individual job posting on S3, and make the processing step its own task with no aggregation. This makes our system more trivially parallelizable - many of our problems happen on the 'reduce' step of the multiprocessing load.

This can be organized in S3 like the following:

`job_posting_attributes/skill_extract_onet_ksat_exact/<job_posting_id>`

or

`job_posting_attributes/<job_posting_id>/skill_extract_onet_ksat_exact`


I think from a technical perspective either way would be workable, but either might be preferable from our perspective in terms of inspecting this intermediate output manually.


*Make the aggregation outputs freely rollup-able*
We can take this a step further by also saving aggregations when they are computed in a more detailed, reusable way on S3. So for a daily-partitioned task, we would also save enough information on S3 for each day to combine the results of that day's processing with other days as we wish. Instead of saving in the current columnar format, we can do something similar to the following:


```
"date": "2016-01-05",
"title_counts": [
	{
		"cbsa": 27526,
		"title": "Cupcake ninja",
		"skill_counts_onet_ksat_exact": {
			"rate monitoring": 5,
			"baking": 26
		}",
		"soc_code_common": {
			"10-1234.00": 31
		}
	}
}
```

This is basically a JSON version of our researcher data hub output, but it's one that can be freely rolled up in different ways. It is also very verbose, but so are our 'aggregated' outputs (and this won't change until we improve our job title cleaning). The example above is for a given day, but even without the quarterly to daily change this could be implemented (just change the "date" key to "quarter").

This is both more resilient (if there is an error rolling up by quarter, the daily aggregation doesn't have to be recomputed) and flexible (we can easily produce state-level or region-level aggregates without doing any significant reprocessing).

## Plan of Attack
From an engineering perspective, large changes like these can be tough to implement. But if done in a certain order, we can make the changes one-by-one, and get incremental value while not requiring significant scaffolding to make the changes work separately. If we were to abandon this rearchitecture project midway through, the incremental improvements would still be in effect.

1. *Save job-posting level attributes*. This change isn't really dependent on any other changes, and can make the system more resilient on its own. The processing methods (skill extraction, occupation classification) already have their own tasks defined; just make these save raw data instead of aggregations. So there is a quarterly skill extraction task that saves all the extracted skills for that quarter to the given place on S3. Make the aggregation task depend on all of them being complete. We have to figure out caching and cache-busting, but I think a simple flag to control reusing existing data versus replace it would work just fine.

2. *Make the aggregation outputs freely rollup-able*
Similarly, the change in aggregation format is not too dependent on the time granularity of the aggregation. It can simply save in the new format, and the task that produces the research hub datasets can now take on the additional task of converting the detailed JSON output into a tabular format instead of the simple columnar join it's doing now. The aggregation tasks still are working with a lot of data, so multiprocessing will still be used in the stack at this point.

3. *Partition and process the data daily.* 
This final step will allow us to run the data in smaller chunks, remove multiprocessing, and let Airflow take care of all the parallelism we need. The ETLs will have to be changed, and reprocessing this will take time. There is a lot of quarter hardcoding happening in the code, so this is the most code-invasive change.  There also needs to be a new quarterly DAG that actually produces quarterly datasest for the Research Hub.
