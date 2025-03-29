from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, avg, row_number
from pyspark.sql.window import Window


from utils import *
from test import test_dataframe

def transformation(spark: SparkSession) -> DataFrame:
    regular_season_df = read_from_mysql(spark,"(SELECT yearID, playerID, teamID, ERA, W, L FROM Pitching WHERE ERA IS NOT NULL AND W IS NOT NULL AND L IS NOT NULL) AS pitching_filtered")
    post_season_df = read_from_mysql(spark,"(SELECT yearID, playerID, teamID, ERA, W, L FROM PitchingPost WHERE ERA IS NOT NULL AND W IS NOT NULL AND L IS NOT NULL) AS postpitching_filtered")

    # Get the team each player played for in the post-season
    post_season_teams = post_season_df.select("yearID", "playerID", "teamID").distinct()

    # Keep only the regular-season stats from that team
    regular_season_filtered = regular_season_df.join(
        post_season_teams, ["yearID", "playerID", "teamID"],"inner"
    )

    # Compute Regular Season Metrics
    regular_season_stats = regular_season_filtered.groupBy("yearID", "playerID").agg(
        avg("ERA").alias("regular_season_ERA"),
        (avg("W") / (avg("W") + avg("L"))).alias("regular_season_win_loss")
    )

    # Compute Post-Season Metrics
    post_season_stats = post_season_df.groupBy("yearID", "playerID").agg(
        avg("ERA").alias("post_season_ERA"),
        (avg("W") / (avg("W") + avg("L"))).alias("post_season_win_loss")
    )

    # Rank Regular Season Pitchers
    window_spec = Window.partitionBy("yearID").orderBy("regular_season_ERA", "regular_season_win_loss")
    regular_season_ranked = regular_season_stats.withColumn("rank", row_number().over(window_spec))

    # Join with Post-Season Data
    joined_df = regular_season_ranked.join(post_season_stats, ["playerID", "yearID"], "inner")

    # Filter the top 10 per year AFTER the join
    final_top_10 = joined_df.filter(col("rank") <= 10)

    # Compute Final Averages
    final_agg_df = final_top_10.groupBy("yearID").agg(
        avg("regular_season_ERA").alias("avg_regular_season_ERA"),
        avg("regular_season_win_loss").alias("avg_regular_season_win_loss"),
        avg("post_season_ERA").alias("avg_post_season_ERA"),
        avg("post_season_win_loss").alias("avg_post_season_win_loss")
    )

    # Duplicate Rows to Attach Top 10 Players
    final_df = final_top_10.join(final_agg_df, ["yearID"], "inner").select(
        "yearID", "playerID", "avg_regular_season_ERA", "avg_regular_season_win_loss",
        "avg_post_season_ERA", "avg_post_season_win_loss"
    ).orderBy("yearID", "playerID")

    # Casting and renaming columns
    final_df = final_df.selectExpr(
        "yearID as `Year`",
        "playerID as `Player`",
        "avg_regular_season_ERA as `Regular Season ERA`",
        "avg_regular_season_win_loss as `Regular Season Win/Loss`",
        "avg_post_season_ERA as `Post-season ERA`",
        "avg_post_season_win_loss as `Post-season Win/Loss`"
    ).withColumn("Year", col("Year").cast("int")) \
        .withColumn("Player", col("Player").cast("string")) \
        .withColumn("Regular Season ERA", col("Regular Season ERA").cast("double")) \
        .withColumn("Regular Season Win/Loss", col("Regular Season Win/Loss").cast("double")) \
        .withColumn("Post-season ERA", col("Post-season ERA").cast("double")) \
        .withColumn("Post-season Win/Loss", col("Post-season Win/Loss").cast("double"))

    return final_df


def main(spark: SparkSession, schema_path: str):
    final_df = transformation(spark)
    test_dataframe(final_df, schema_path, "Pitching")

    # write CSV to local file system
    write_single_csv(final_df, "/app/output", "Pitching")

    # save_to_minio(final_df, "Pitching")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Pitching").getOrCreate()
    schema_path = "/app/schema_config.json"
    main(spark, schema_path)

    spark.stop()
