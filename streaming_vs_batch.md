# Streaming vs. Batch Processing in Data Pipeline

## Introduction

In this project, we were tasked with building an ETL pipeline to extract, transform, and load (ETL) data from a MySQL database to a storage system (MinIO) using Apache Spark.
Given the nature of the data and the task, the choice between using streaming or batch processing for data processing was considered. Below is an explanation of why **batch processing** was chosen over streaming and how streaming could have been implemented.

## Difference Between Streaming and Batch Processing
**Batch Processing** is a traditional approach where data is processed in chunks or batches. The data is usually processed in scheduled intervals (e.g., daily or hourly), making it well-suited for tasks that don't require immediate updates and where the data volume is manageable.
**Streaming Processing** involves continuously processing data as it arrives, typically in real-time or near real-time. This is ideal for applications where timely insights or updates are required based on the latest available data.

### Streaming vs. Batch Use Cases

- **Batch Processing**: 
  - Ideal for ETL tasks where data is aggregated over a time period (e.g., daily, monthly).
  - Suited for analytics and reporting tasks where real-time processing isn't critical.
  - Reduces the complexity and cost associated with continuously processing large volumes of data.
  
- **Streaming Processing**: 
  - Ideal for real-time analytics, monitoring, and applications that need instant data updates.
  - Useful in use cases like fraud detection, stock market analysis, or IoT data streaming.

## How Streaming Could Be Implemented
If we were to implement streaming, the pipeline could process incoming data as it gets ingested from the MySQL database or any other sources. With Apache Spark, this could be achieved using Spark Structured Streaming.

Here’s an example of how streaming might be set up using Spark:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Streaming ETL") \
    .getOrCreate()

# Define the input source for streaming
streaming_df = spark.readStream \
    .format("jdbc") \
    .option("url", MYSQL_URL) \
    .option("driver", MYSQL_DRIVER) \
    .option("dbtable", "streaming_data_table") \
    .option("user", MYSQL_USER) \
    .option("password", MYSQL_PASSWORD) \
    .load()

# Perform the transformations on the streaming data
processed_stream_df = streaming_df.groupBy("yearID").agg({"ERA": "avg"})

# Write the streaming output to MinIO in real-time
query = processed_stream_df.writeStream \
    .outputMode("complete") \
    .format("csv") \
    .option("path", "minio_output_path") \
    .option("checkpointLocation", "checkpoint_location") \
    .start()

query.awaitTermination()
```

This setup would require a constant stream of data and continuous monitoring of the source data. The process involves reading data from a stream, transforming it in real-time, and writing it to the destination in near-real-time.

## Why Batch Processing Was Chosen
After evaluating the nature of the task and the data involved, **batch processing** was chosen for the following reasons:
- **Nature of the Data**: The data in the `lahman2016` database is historical and aggregated over multiple years. Processing this data in batches is more efficient than continuously processing incoming data in real-time.
- **No Need for Real-Time Processing**: The application focuses on performing aggregations and transformations on historical data. Since the data is already available and doesn’t require real-time updates, batch processing is sufficient for the task.
- **Cost Considerations**: Streaming incurs additional costs for infrastructure and monitoring due to the continuous processing of data. Since there is no immediate need for real-time results, using batch processing helps avoid unnecessary overhead.
- **Simplicity and Maintainability**: Batch processing is simpler to implement and maintain, as it processes data in defined intervals and doesn’t require a continuous pipeline. This also simplifies debugging and error handling, as errors can be addressed between batches.

## Conclusion
While streaming would be a great solution for real-time applications, batch processing is more appropriate for this ETL pipeline due to the nature of the data and the task requirements. The choice to use batch processing helps optimize resource usage, reduces complexity, and ensures that the application meets the project requirements efficiently.
