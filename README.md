# Large-Scale Data Analysis with PySpark, Spark SQL and Hadoop

Big data projects from my graduate course in Large-Scale Data Analysis at Boise State University (Fall 2023), run on a Spark and Hadoop (HDFS) cluster.

**Stack:** PySpark · Spark SQL · DataFrames · RDDs · Hadoop HDFS · spark-submit · Excel Power Query (validation)

## Projects

### 1. MovieLens Data Mining at Scale (final project)
[`MovieLens Data Analysis/`](MovieLens%20Data%20Analysis)

Mined the MovieLens 1M dataset (1 million ratings from 6,000 users on 4,000 movies) and extended the analysis to MovieLens 25M to test scalability.

- **Questions answered (10+):** most popular movies overall, by year, by age group and by season; widely watched but low-rated movies; average rating by genre and age group; genre preferences by occupation and gender; top-rated movies per genre with `RANK()` window functions; tag trends over time (25M).
- **Scale:** the 1M analysis ran in **46 seconds on the cluster**, versus hours on a single VM that could not finish it for lack of memory. The 25M tag analysis used caching to cut processing time.
- **Validation:** I first ran each analysis on a 32,373-rating sample in Excel (Power Query and pivot tables), then on the VM and the cluster, and confirmed the outputs matched.

| Run | Sample (32K ratings) | Full 1M dataset |
| --- | --- | --- |
| Single VM (Spark) | 43 s | Did not finish (hours, out of memory) |
| Cluster (Spark on HDFS) | 31 s | 46 s |

### 2. Inverted Index in Spark
[`BetterInvertedIndex.ipynb`](BetterInvertedIndex.ipynb)

A PySpark job that builds an inverted index (word → documents) from a text corpus, run with `spark-submit`.

### 3. RDD Fundamentals
[`Resilient Distributed Datasets (RDDs) .ipynb`](Resilient%20Distributed%20Datasets%20(RDDs)%20.ipynb)

Hands-on notebook covering Spark contexts and RDD transformations and actions: map, filter, reduce, sampling and set operations.

## Running the MovieLens project on a cluster

```bash
hdfs dfs -put movies
spark-submit --master spark://<master-host>:7077 P3_1M_DataAnalysis.py \
  hdfs://<namenode>:9000/<path>/ml-1m/movies.dat \
  hdfs://<namenode>:9000/<path>/ml-1m/ratings.dat \
  hdfs://<namenode>:9000/<path>/ml-1m/users.dat \
  hdfs://<namenode>:9000/<path>/ml-1m-output
hdfs dfs -get ml-1m-output
```

Data: [MovieLens](https://grouplens.org/datasets/movielens/) by GroupLens Research.
