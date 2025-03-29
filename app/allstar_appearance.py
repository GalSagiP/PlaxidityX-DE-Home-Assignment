from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from utils import *
from test import test_dataframe

def transformation(spark: SparkSession) -> DataFrame:
    hof_df = read_from_mysql(spark,"(SELECT playerID, yearID FROM HallOfFame WHERE inducted = 'Y') AS hof_filtered")
    all_star_df = read_from_mysql(spark,"(SELECT playerID, yearID FROM AllstarFull WHERE GP = 1) AS allstar_filtered")
    pitching_df = read_from_mysql(spark, "(SELECT playerID, yearID, ERA FROM Pitching) AS pitching_filtered")

    # Step 1: Rename the yearID column in both Hall of Fame and Pitching tables to make them distinct
    induction_year_df = hof_df.groupBy("playerID").agg(F.min("yearID").alias("induction_year"))

    hof_df_filtered = induction_year_df.filter(F.col("playerID").isNotNull())
    allstar_df_filtered = all_star_df.filter(F.col("playerID").isNotNull())
    pitching_df_filtered = pitching_df.filter(F.col("playerID").isNotNull())

    # Perform left join between the Hall of Fame DataFrame and the AllStar DataFrame
    hof_allstar_df = hof_df_filtered.join(allstar_df_filtered, "playerID", "inner")

    # Count the number of All-Star appearances for each player
    hof_allstar_with_count_df = hof_allstar_df.groupBy("playerID").agg(
        F.count("yearID").alias("all_star_count"))

    # Join the allstar with the pitching, in order to find the ERA in the all-star years
    allstar_pitching_df = allstar_df_filtered.join(pitching_df_filtered,["playerID", "yearID"],"inner"
                                                   ).select(allstar_df_filtered["playerID"],
                                                            allstar_df_filtered["yearID"],
                                                            pitching_df_filtered["ERA"])

    # Calculate average ERA per player
    allstar_pitching_avg_era_df = allstar_pitching_df.groupBy("playerID").agg(
        F.avg("ERA").alias("avg_ERA"))

    # combine the avg era with the # all-star
    hof_allstar_pitching_avg_era_df = hof_allstar_with_count_df.join(allstar_pitching_avg_era_df,"playerID","inner"
                                                                      ).select(hof_allstar_with_count_df["playerID"],
                                                                        hof_allstar_with_count_df["all_star_count"],
                                                                        allstar_pitching_avg_era_df["avg_ERA"])

    final_df = hof_df_filtered.join(hof_allstar_pitching_avg_era_df,"playerID","inner")

    # Casting and renaming columns for HallOfFameAllStarPitchers DataFrame
    final_df = (final_df.selectExpr(
        "playerID as `Player`",
        "avg_ERA as `ERA`",
        "all_star_count as `# All Star Appearances`",
        "induction_year as `Hall of Fame Induction Year`"
        ).withColumn("Player", F.col("Player").cast("string")) \
        .withColumn("ERA", F.col("ERA").cast("double")) \
        .withColumn("# All Star Appearances", F.col("# All Star Appearances").cast("bigint")) \
        .withColumn("Hall of Fame Induction Year", F.col("Hall of Fame Induction Year").cast("int")))

    return final_df

def main(spark: SparkSession, schema_path: str):
    final_df = transformation(spark)
    test_dataframe(final_df, schema_path, "HallOfFameAllStarPitchers")

    # write CSV to local file system
    write_single_csv(final_df, "/app/output", "HallOfFameAllStarPitchers")

    # save_to_minio(final_df, "HallOfFameAllStarPitchers")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("HallOfFameAllStarPitchers").getOrCreate()
    schema_path = "/app/schema_config.json"
    main(spark, schema_path)

    spark.stop()