from spark import dataframes
from pyspark.sql import SparkSession


if __name__ == "__main__":

    spark: SparkSession = (
        SparkSession
        .builder
        .appName("localQueries")
        .master("local[*]")
        .getOrCreate()
    )

    (
        dataframes.create_valueaddedhistoryquarterly_df(
            spark=spark,
            file_name="valueaddedhistoryquarterly_2026_06_05.json",
        )
        .show(5)
    )