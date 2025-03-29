from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, max as spark_max
from pyspark.sql.window import Window

from utils import *
from test import test_dataframe


def transformation(spark: SparkSession) -> DataFrame:
    teams_df = read_from_mysql(spark,"(SELECT yearID, teamID, `Rank`, AB FROM Teams WHERE `Rank` IS NOT NULL AND AB IS NOT NULL) AS teams_filtered")

    # Find the last-place rank per year
    last_place_rank_df = teams_df.groupBy("yearID").agg(spark_max("Rank").alias("last_place_rank"))

    # Get first-place teams (Rank = 1)
    first_place_df = teams_df.filter(col("Rank") == 1)

    # Get last-place teams (teams where Rank = max Rank for that year)
    last_place_df = teams_df.join(last_place_rank_df, "yearID").filter(col("Rank") == col("last_place_rank")).drop(
        "last_place_rank")

    # Combine first and last place teams
    final_df = first_place_df.union(last_place_df).withColumnRenamed("Rank", "rank")

    # Casting and renaming columns
    final_df = (final_df.selectExpr(
        "teamID as `Team ID`",
        "yearID as `Year`",
        "Rank as `Rank`",
        "AB as `At Bats`"
        ).withColumn("Team ID", col("Team ID").cast("string")) \
            .withColumn("Rank", col("Rank").cast("int")) \
            .withColumn("Year", col("Year").cast("int")) \
            .withColumn("At Bats", col("At Bats").cast("int")))

    return final_df


def main(spark: SparkSession, schema_path: str):
    final_df = transformation(spark)
    test_dataframe(final_df, schema_path, "Rankings")

    # write CSV to local file system
    write_single_csv(final_df, "/app/output", "Rankings")

    # save_to_minio(final_df, "Rankings")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Rankings").getOrCreate()
    schema_path = "/app/schema_config.json"
    main(spark, schema_path)

    spark.stop()
