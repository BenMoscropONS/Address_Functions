import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, regexp_replace, when, trim, upper, concat_ws, lit
from pyspark.sql.types import StringType
from address_functions.config.settings import town_list

def extract_postcode_town_address(df, address_col):
    """
    Extract postcode, town, and address lines from the address column.

    This function extracts the postcode and town from the specified address column
    and cleans the remaining address to form the address lines.

    Parameters:
    - df (DataFrame): The input DataFrame.
    - address_col (str): The name of the address column.

    Returns:
    - df (DataFrame): The DataFrame with extracted postcode, town, and cleaned address lines.
    """
    # Define regular expressions for postcode and town extraction
    postcode_pattern = r"([A-Za-z]{1,2}\d{1,2}\s*\d[A-Za-z]{2}|\w{1,2}\d\w\s*\d\w{2})"
    town_regex = r',\s*(?!STREET|ROAD|LANE|AVENUE|DRIVE|SQUARE|HILL)\b([A-Za-z ]+?)\b(?=,|$)'
    towns_pattern = f"({'|'.join([re.escape(town) for town in town_list])})"

    # Extract postcode
    df = df.withColumn('postcode', regexp_extract(col(address_col), postcode_pattern, 0))

    # Try to extract town using the predefined list or the general pattern
    df = df.withColumn('town', regexp_extract(col(address_col), towns_pattern, 0))
    df = df.withColumn('town', when(col('town') == "", regexp_extract(col(address_col), town_regex, 1)).otherwise(col('town')))

    # Combine tokens to create address_lines
    df = df.withColumn('address_lines', concat_ws(" ",
        col('unclass_num'),
        col('single_num'),
        col('txt_b4_num'),
        col('end_num'),
        col('start_num'),
        col('start_num_suff'),
        col('end_num_suff'),
        col(address_col)
    ))

    # Clean address by removing extracted parts and extra punctuation
    df = df.withColumn('address_lines', regexp_replace(col('address_lines'), f"({postcode_pattern}|{towns_pattern})", ""))
    df = df.withColumn('address_lines', regexp_replace(col('address_lines'), r'\s*,\s*', ', '))  # Normalise spaces around commas
    df = df.withColumn('address_lines', trim(regexp_replace(col('address_lines'), r',+', ',')))  # Reduce multiple commas to a single comma
    df = df.withColumn('address_lines', trim(regexp_replace(col('address_lines'), r'(^,|,$)', '')))  # Remove leading and trailing commas
    df = df.withColumn('address_lines', regexp_replace(col('address_lines'), r', ,', ','))  # Remove consecutive commas separated by spaces
    df = df.withColumn('address_lines', regexp_replace(col('address_lines'), r', ,', ','))  # Additional pass to catch any missed instances
    df = df.withColumn('address_lines', trim(regexp_replace(col('address_lines'), r'\s+', ' ')))  # Trim whitespace

    return df

################################################################

def standardise_street_types(df, column):
    """
    Standardise street types in the address column.

    This function replaces common abbreviations and misspellings of street types 
    with their standard forms in the specified column of the DataFrame.

    Parameters:
    - df (DataFrame): The input DataFrame.
    - column (str): The name of the column to standardise.

    Returns:
    - df (DataFrame): The DataFrame with standardised street types.
    """
    df = df.withColumn(column, regexp_replace(col(column), r'\bRD\b|RAOD', 'ROAD'))
    df = df.withColumn(column, regexp_replace(col(column), r'\bAVE\b|\bAVE\.\b|\bAVENEU\b', 'AVENUE'))
    df = df.withColumn(column, regexp_replace(col(column), r'\bSTR|STRT\b', 'STREET'))
    df = df.withColumn(column, regexp_replace(col(column), r'\bST(?!\.\s[A-Z]|\s[A-Z]|[A-Z])', 'STREET'))
    df = df.withColumn(column, regexp_replace(col(column), r'\bCRT|\bCRT\.\b|\bCT\b', 'COURT'))
    df = df.withColumn(column, regexp_replace(col(column), r'\bCRESENT\b|\bCRSNT\b', 'CRESCENT'))
    df = df.withColumn(column, regexp_replace(col(column), r'\bDRV\b|\bDR\b', 'DRIVE'))
    df = df.withColumn(column, regexp_replace(col(column), r'\bGRDN(?=S\b)?\b|\bGDN(?=S\b)?\b', 'GARDEN'))
    df = df.withColumn(column, regexp_replace(col(column), r'\bPK\b', 'PARK'))
    df = df.withColumn(column, regexp_replace(col(column), r'\bCL\b', 'CLOSE'))
    return df
  
#################################################################
def identify_patterns(df, column):
    """
    Identify number patterns in the address column.

    This function extracts various patterns related to numbers from the specified column
    in the DataFrame.

    Parameters:
    - df (DataFrame): The input DataFrame.
    - column (str): The name of the column to analyse.

    Returns:
    - df (DataFrame): The DataFrame with additional columns for identified patterns.
    """
    df = df.withColumn('unclass_num', regexp_extract(col(column), r'(^|\s)(\D?(\d+\.\d+(\.\d+)?\.?\s)|([A-Z]{1,2}\d+\s))', 0))
    df = df.withColumn('single_num', regexp_extract(col(column), r'((^|^\D*\s)\d+(\s?[A-Z]\b)?)((\s?-\s?|\sTO\s|\\|\/|\s)(\d+(\s?[A-Z]\b)?))?(?!.+\b\d)', 0))
    df = df.withColumn('txt_b4_num', regexp_extract(col(column), r'^\D+\s(?=\d+)', 0))
    df = df.withColumn('end_num', regexp_extract(col(column), r'((?<=\s)\d+(\s?[A-Z]\b)?)((\s?-\s?|\sTO\s|\\|\/|\s)(\d+(\s?[A-Z]\b)?))?(?!.+(\b\d+(\s?[A-Z]\b)?)((\s?-\s?|\sTO\s|\\|\/|\s)(\d+(\s?[A-Z]\b)?))?)', 0))
    df = df.withColumn('start_num', regexp_extract(col(column), r'^\d+', 0))
    df = df.withColumn('start_num_suff', regexp_extract(col(column), r'((?<=^\d{1})|(?<=^\d{2})|(?<=^\d{3})|(?<=^\d{4}))\s?([A-Z]\b)', 0))
    df = df.withColumn('end_num_suff', regexp_extract(col(column), r'((?<=^\d{1})|(?<=^\d{2})|(?<=^\d{3})|(?<=^\d{4}))(?<=\s)?([A-Z]\b)?(\s?-\s?|\sTO\s|\\|\/|\s)\s?\d+', 0))
    return df

#################################################################

def identify_location_units(df, column):
    """
    Identify location unit patterns in the address column.

    This function extracts patterns related to flats, rooms, units, blocks, apartments, 
    and floors from the specified column in the DataFrame.

    Parameters:
    - df (DataFrame): The input DataFrame.
    - column (str): The name of the column to analyse.

    Returns:
    - df (DataFrame): The DataFrame with additional columns for identified location units.
    """
    df = df.withColumn('flat', regexp_extract(col(column), r'((\bFLAT\s((NO|NO\.)\s?)?([-A-Z0-9\.]{1,4}|([^A-Z]{1,6})))\s|\bFLAT[-0-9A-RU-Z\.]{1,4}\s|\bFLAT\s)((\s?-\s?|\s?TO\s|\s?&\s?|\s?\\\s?)((\bFLAT\s)?[-A-Z0-9\.]{1,4}\b)\s)?', 0))
    df = df.withColumn('room', regexp_extract(col(column), r'((\bROOM\s((NO|NO\.)\s?)?([-A-Z0-9\.]{1,4}|([^A-Z]{1,6})))\s|\bROOM[-0-9A-RT-Z\.]{1,4}\s|\bROOM\s)((\s?-\s?|\s?TO\s|\s?&\s?|\s?\\\s?)((\bROOM\s)?[-A-Z0-9\.]{1,4}\b)\s)?', 0))
    df = df.withColumn('unit', regexp_extract(col(column), r'((\bUNIT\s((NO|NO\.)\s?)?([-A-Z0-9\.]{1,4}|([^A-Z]{1,6})))\s|\bUNIT[-0-9A-RT-Z\.]{1,4}\s|\bUNIT\s)((\s?-\s?|\s?TO\s|\s?&\s?|\s?\\\s?)((\bUNIT\s)?[-A-Z0-9\.]{1,4}\b)\s)?', 0))
    df = df.withColumn('block', regexp_extract(col(column), r'((\bBLOCK\s((NO|NO\.)\s?)?([-A-Z0-9\.]{1,4}|([^A-Z]{1,6})))\s|\bBLOCK[-0-9A-RT-Z\.]{1,4}\s|\bBLOCK\s)((\s?-\s?|\s?TO\s|\s?&\s?|\s?\\\s?)((\bBLOCK\s)?[-A-Z0-9\.]{1,4}\b)\s)?', 0))
    df = df.withColumn('apartment', regexp_extract(col(column), r'((\bAPARTMENT\s((NO|NO\.)\s?)?([-A-Z0-9\.]{1,4}|([^A-Z]{1,6})))\s|\bAPARTMENT[-0-9A-RT-Z\.]{1,4}\s|\bAPARTMENT\s)((\s?-\s?|\s?TO\s|\s?&\s?|\s?\\\s?)((\bAPARTMENT\s)?[-A-Z0-9\.]{1,4}\b)\s)?', 0))
    df = df.withColumn('floor', regexp_extract(col(column), r'BASEMENT|((GROUND|FIRST|SECOND|THIRD|FOURTH|FIFTH|SIXTH|SEVENTH)\sFLOOR\b)', 0))
    return df
  
#################################################################

def remove_unwanted_characters(df, column):
    """
    Remove unwanted characters from the address column.

    This function replaces unwanted characters and words with spaces in the specified 
    column of the DataFrame.

    Parameters:
    - df (DataFrame): The input DataFrame.
    - column (str): The name of the column to clean.

    Returns:
    - df (DataFrame): The DataFrame with unwanted characters removed.
    """
    df = df.withColumn(column, regexp_replace(col(column), r'\.|-|\bTO\b|\bOVER\b|\bNO\b|\bNO\.\b|\\|/', ' '))
    return df