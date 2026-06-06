import constants

from spark import dataframes
from pyspark.sql import SparkSession


def create_spark_session(app_name: str) -> None:
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .getOrCreate()
    )

def load_spark_tables(spark: SparkSession) -> None:
    table_creation_functions = [
        func for func in dir(dataframes)
        if func.startswith('create_') and callable(getattr(dataframes, func))
    ]
    for func_name in table_creation_functions:
        logical_name = func_name.replace('create_', '').replace('_df', '')
        func = getattr(dataframes, func_name)
        df = func(spark=spark)
        df.createOrReplaceTempView(logical_name)

        df.write.format("parquet").mode("overwrite").save(f"{constants.PROCESSED_LAYER}{logical_name}")