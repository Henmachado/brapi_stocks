from spark import dataframes
from pyspark.sql import SparkSession


def load_all_tables(spark: SparkSession) -> None:
    table_creation_functions = [
        func for func in dir(dataframes)
        if func.startswith('create_') and callable(getattr(dataframes, func))
    ]
    for func_name in table_creation_functions:
        logical_name = func_name.replace('create_', '').replace('_df', '')
        func = getattr(dataframes, func_name)
        func(spark).createOrReplaceTempView(logical_name)