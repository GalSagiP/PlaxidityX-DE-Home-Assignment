from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, avg, when

from utils import *
from test import test_dataframe

def transformation(spark: SparkSession) -> DataFrame:
    salaries_df = read_from_mysql(spark, "(SELECT playerID, yearID, salary FROM Salaries) AS salaries_filtered")
    appearances_df = read_from_mysql(spark, "(SELECT playerID, yearID, G_1B, G_2B, G_3B, G_SS, G_P FROM Appearances) AS appearances_filtered")

    # Identify player roles (Fielders vs Pitchers), Classify based on majority appearances
    players_roles = appearances_df.withColumn(
        "role",
        when((col("G_P") > (col("G_1B") + col("G_2B") + col("G_3B") + col("G_SS"))), "Pitching")
        .when((col("G_1B") + col("G_2B") + col("G_3B") + col("G_SS")) > 0, "Fielding")
    ).filter(col("role").isNotNull()).drop("G_1B", "G_2B", "G_3B", "G_SS", "G_P")

    # Join Salaries with player roles
    salaries_roles_df = salaries_df.join(players_roles, ["playerID", "yearID"])

    # Compute the average salary for each role per year
    avg_salary_df = salaries_roles_df.groupBy("yearID", "role").agg(avg("salary").alias("avg_salary"))

    # Pivot to get columns Year | Fielding | Pitching
    final_avg_salary = avg_salary_df.groupBy("yearID").pivot("role").agg(avg("avg_salary"))

    # Casting and renaming columns for AverageSalaries DataFrame
    final_df = final_avg_salary.selectExpr(
        "yearID as `Year`",
        "Fielding as `Fielding`",
        "Pitching as `Pitching`"
        ).withColumn("Year", col("Year").cast("int")) \
            .withColumn("Fielding", col("Fielding").cast("double")) \
            .withColumn("Pitching", col("Pitching").cast("double"))

    return final_df

def main(spark: SparkSession, schema_path: str):
    final_df = transformation(spark)
    test_dataframe(final_df, schema_path, "AverageSalaries")

    # write CSV to local file system
    write_single_csv(final_df, "/app/output", "AverageSalaries")

    # save_to_minio(final_df, "AverageSalaries")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("AverageSalaries").getOrCreate()
    schema_path = "/app/schema_config.json"
    main(spark, schema_path)

    spark.stop()
