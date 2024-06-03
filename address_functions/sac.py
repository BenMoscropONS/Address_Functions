import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, regexp_replace, when, trim, upper
from pyspark.sql.types import StringType
from address_index.address_functions.config.settings import town_list

def extract_postcode_town_address(df, address_col):
    # Define regular expressions for postcode and town extraction
    postcode_pattern = r"([A-Za-z]{1,2}\d{1,2}\s*\d[A-Za-z]{2}|\w{1,2}\d\w\s*\d\w{2})"
    town_regex = r',\s*(?!STREET|ROAD|LANE|AVENUE|DRIVE|SQUARE|HILL)\b([A-Za-z ]+?)\b(?=,|$)'
    towns_pattern = f"({'|'.join([re.escape(town) for town in town_list])})"

    # Extract postcode
    df = df.withColumn('postcode', regexp_extract(col(address_col), postcode_pattern, 0))

    # Try to extract town using the predefined list or the general pattern
    df = df.withColumn('town', regexp_extract(col(address_col), towns_pattern, 0))
    df = df.withColumn('town', when(col('town') == "", regexp_extract(col(address_col), town_regex, 1)).otherwise(col('town')))

    # Clean address by removing extracted parts and extra punctuation
    df = df.withColumn('address_lines', regexp_replace(col(address_col), rf"({postcode_pattern}|{towns_pattern})", ""))
    df = df.withColumn('address_lines', regexp_replace(col('address_lines'), r'\s*,\s*', ', '))  # Normalize spaces around commas
    df = df.withColumn('address_lines', trim(regexp_replace(col('address_lines'), r',+', ',')))  # Reduce multiple commas to a single comma
    df = df.withColumn('address_lines', trim(regexp_replace(col('address_lines'), r'(^,|,$)', '')))  # Remove leading and trailing commas
    df = df.withColumn('address_lines', regexp_replace(col('address_lines'), r', ,', ','))  # Remove consecutive commas separated by spaces
    df = df.withColumn('address_lines', regexp_replace(col('address_lines'), r', ,', ','))  # Additional pass to catch any missed instances
    df = df.withColumn('address_lines', trim(regexp_replace(col('address_lines'), r'\s+', ' ')))  # Trim whitespace

    return df
