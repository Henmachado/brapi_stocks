import os
import re

import constants

from spark import dataframes
from pyspark.sql import SparkSession


def load_all_tables(spark: SparkSession, data_dir: str = constants.RAW_LAYER) -> None:
    """
    Automatically discovers the latest raw JSON files and creates DataFrames
    for all 'create_*' functions defined in spark.dataframes.
    """
    # 1. Get all available functions that start with 'create_'
    table_creation_functions = [f for f in dir(dataframes) if f.startswith('create_') and callable(getattr(dataframes, f))]
    
    # 2. Map function names to their base file names
    # e.g., 'create_activetickers_df' -> 'active_tickers'
    # we'll use a simple heuristic or regex to match them to files
    
    all_files = os.listdir(data_dir)
    
    for func_name in table_creation_functions:
        # Extract the logical name, e.g., 'balancesheethistoryquarterly'
        logical_name = func_name.replace('create_', '').replace('_df', '')
        
        # Find files that start with this logical name (ignoring underscores for matching)
        # The files in data/raw look like 'balancesheethistoryquarterly_2026_06_06.json'
        regex_pattern = rf"^{logical_name.replace('_', '')}.*\.json$"
        matching_files = [f for f in all_files if re.match(regex_pattern, f.replace('_', ''), re.IGNORECASE)]
        
        if not matching_files:
            continue
            
        # Get the latest file (sorting works because of YYYY_MM_DD format)
        latest_file = sorted(matching_files)[-1]
        
        # Call the creation function
        func = getattr(dataframes, func_name)
        df = func(spark, latest_file)
        
        # Register as a Temp View for SQL queries
        # View name will be the logical name (e.g., 'balancesheethistoryquarterly')
        df.createOrReplaceTempView(logical_name)