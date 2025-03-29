from pyspark.sql import SparkSession
import shutil
import os
import average_salary
import allstar_appearance
import top_10_pitchers
import first_last_place

schema_path = "/app/schema_config.json"

def create_spark_session():
    """
    Initialize and return a Spark session.
    """
    return SparkSession.builder \
    .appName("ETL Job") \
    .config("spark.jars", "/opt/spark/jars/mysql-connector-java-8.0.29.jar") \
    .getOrCreate()

def main():
    """
    Main ETL pipeline that executes all transformations.
    """
    spark = create_spark_session()

    print(">>>>>>>>>>>>>> Starting ETL Process")

    average_salary.main(spark, schema_path)
    # allstar_appearance.main(spark, schema_path)
    top_10_pitchers.main(spark, schema_path)
    first_last_place.main(spark, schema_path)

    print(">>>>>>>>>>>>>> ETL Process Completed")

    spark.stop()

if __name__ == "__main__":
    main()
