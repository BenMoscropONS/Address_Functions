import re

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, regexp_extract, regexp_replace, when, trim, upper, concat_ws, lit
from pyspark.sql.types import StringType
from address_functions.config.settings import town_list

def extract_postcode_town_address(df: DataFrame, address_col: str = "final_cleaned_address") -> DataFrame:
    postcode_pattern = r"([A-Za-z]{1,2}\d{1,2}\s*\d[A-Za-z]{2}|\w{1,2}\d\w\s*\d\w{2})"
    town_regex = r',\s*(?!STREET|ROAD|LANE|AVENUE|DRIVE|SQUARE|HILL)\b([A-Za-z ]+?)\b(?=,|$)'
    towns_pattern = f"({'|'.join([re.escape(town) for town in town_list])})"
    
    df = df.withColumn('postcode', regexp_extract(col(address_col), postcode_pattern, 0))
    
    df = df.withColumn('town', regexp_extract(col(address_col), towns_pattern, 0))
    df = df.withColumn('town', when(col('town') == "", regexp_extract(col(address_col), town_regex, 1)).otherwise(col('town')))
    
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
    
    df = df.withColumn('address_lines', regexp_replace(col('address_lines'), f"({postcode_pattern}|{towns_pattern})", ""))
    df = df.withColumn('address_lines', regexp_replace(col('address_lines'), r'\s*,\s*', ', '))
    df = df.withColumn('address_lines', trim(regexp_replace(col('address_lines'), r',+', ',')))
    df = df.withColumn('address_lines', trim(regexp_replace(col('address_lines'), r'(^,|,$)', '')))
    df = df.withColumn('address_lines', regexp_replace(col('address_lines'), r', ,', ','))
    df = df.withColumn('address_lines', regexp_replace(col('address_lines'), r', ,', ','))
    df = df.withColumn('address_lines', trim(regexp_replace(col('address_lines'), r'\s+', ' ')))
    
    return df

################################################################

def standardise_street_types(df: DataFrame, address_col: str = "final_cleaned_address") -> DataFrame:
    original_column = col(address_col)
    df = df.withColumn(address_col, regexp_replace(col(address_col), r'\bRD\b|RAOD', 'ROAD'))
    df = df.withColumn(address_col, regexp_replace(col(address_col), r'\bAVE\b|\bAVE\.\b|\bAVENEU\b', 'AVENUE'))
    df = df.withColumn(address_col, regexp_replace(col(address_col), r'\bSTR|STRT\b', 'STREET'))
    df = df.withColumn(address_col, regexp_replace(col(address_col), r'\bST(?!\.\s[A-Z]|\s[A-Z]|[A-Z])', 'STREET'))
    df = df.withColumn(address_col, regexp_replace(col(address_col), r'\bCRT|\bCRT\.\b|\bCT\b', 'COURT'))
    df = df.withColumn(address_col, regexp_replace(col(address_col), r'\bCRESENT\b|\bCRSNT\b', 'CRESCENT'))
    df = df.withColumn(address_col, regexp_replace(col(address_col), r'\bDRV\b|\bDR\b', 'DRIVE'))
    df = df.withColumn(address_col, regexp_replace(col(address_col), r'\bGRDN(?=S\b)?\b|\bGDN(?=S\b)?\b', 'GARDEN'))
    df = df.withColumn(address_col, regexp_replace(col(address_col), r'\bPK\b', 'PARK'))
    df = df.withColumn(address_col, regexp_replace(col(address_col), r'\bCL\b', 'CLOSE'))
    df = df.withColumn('street_type_standardised_flag', when(col(address_col) != original_column, lit(1)).otherwise(lit(0)))
    return df
  
#################################################################

def identify_patterns(df: DataFrame, address_col: str = "final_cleaned_address") -> DataFrame:
    df = df.withColumn('unclass_num', regexp_extract(col(address_col), r'(^|\s)(\D?(\d+\.\d+(\.\d+)?\.?\s)|([A-Z]{1,2}\d+\s))', 0))
    df = df.withColumn('single_num', regexp_extract(col(address_col), r'((^|^\D*\s)\d+(\s?[A-Z]\b)?)((\s?-\s?|\sTO\s|\\|\/|\s)(\d+(\s?[A-Z]\b)?))?(?!.+\b\d)', 0))
    df = df.withColumn('txt_b4_num', regexp_extract(col(address_col), r'^\D+\s(?=\d+)', 0))
    df = df.withColumn('end_num', regexp_extract(col(address_col), r'((?<=\s)\d+(\s?[A-Z]\b)?)((\s?-\s?|\sTO\s|\\|\/|\s)(\d+(\s?[A-Z]\b)?))?(?!.+(\b\d+(\s?[A-Z]\b)?)((\s?-\s?|\sTO\s|\\|\/|\s)(\d+(\s?[A-Z]\b)?))?)', 0))
    df = df.withColumn('start_num', regexp_extract(col(address_col), r'^\d+', 0))
    df = df.withColumn('start_num_suff', regexp_extract(col(address_col), r'((?<=^\d{1})|(?<=^\d{2})|(?<=^\d{3})|(?<=^\d{4}))\s?([A-Z]\b)', 0))
    df = df.withColumn('end_num_suff', regexp_extract(col(address_col), r'((?<=^\d{1})|(?<=^\d{2})|(?<=^\d{3})|(?<=^\d{4}))(?<=\s)?([A-Z]\b)?(\s?-\s?|\sTO\s|\\|\/|\s)\s?\d+', 0))
    df = df.withColumn('patterns_identified_flag', lit(1))
    return df

#################################################################

def identify_location_units(df: DataFrame, address_col: str = "final_cleaned_address") -> DataFrame:
    df = df.withColumn('flat', regexp_extract(col(address_col), r'((\bFLAT\s((NO|NO\.)\s?)?([-A-Z0-9\.]{1,4}|([^A-Z]{1,6})))\s|\bFLAT[-0-9A-RU-Z\.]{1,4}\s|\bFLAT\s)((\s?-\s?|\s?TO\s|\s?&\s?|\s?\\\s?)((\bFLAT\s)?[-A-Z0-9\.]{1,4}\b)\s)?', 0))
    df = df.withColumn('room', regexp_extract(col(address_col), r'((\bROOM\s((NO|NO\.)\s?)?([-A-Z0-9\.]{1,4}|([^A-Z]{1,6})))\s|\bROOM[-0-9A-RT-Z\.]{1,4}\s|\bROOM\s)((\s?-\s?|\s?TO\s|\s?&\s?|\s?\\\s?)((\bROOM\s)?[-A-Z0-9\.]{1,4}\b)\s)?', 0))
    df = df.withColumn('unit', regexp_extract(col(address_col), r'((\bUNIT\s((NO|NO\.)\s?)?([-A-Z0-9\.]{1,4}|([^A-Z]{1,6})))\s|\bUNIT[-0-9A-RT-Z\.]{1,4}\s|\bUNIT\s)((\s?-\s?|\s?TO\s|\s?&\s?\\\s?)((\bUNIT\s)?[-A-Z0-9\.]{1,4}\b)\s)?', 0))
    df = df.withColumn('block', regexp_extract(col(address_col), r'((\bBLOCK\s((NO|NO\.)\s?)?([-A-Z0-9\.]{1,4}|([^A-Z]{1,6})))\s|\bBLOCK[-0-9A-RT-Z\.]{1,4}\s|\bBLOCK\s)((\s?-\\s?|\s?TO\s|\s?&\s?\\\s?)((\bBLOCK\s)?[-A-Z0-9\.]{1,4}\b)\s)?', 0))
    df = df.withColumn('apartment', regexp_extract(col(address_col), r'((\bAPARTMENT\s((NO|NO\.)\s?)?([-A-Z0-9\.]{1,4}|([^A-Z]{1,6})))\s|\bAPARTMENT[-0-9A-RT-Z\.]{1,4}\s|\bAPARTMENT\s)((\s?-\s?|\s?TO\s|\s?&\s?\\\s?)((\bAPARTMENT\s)?[-A-Z0-9\.]{1,4}\b)\s)?', 0))
    df = df.withColumn('floor', regexp_extract(col(address_col), r'BASEMENT|((GROUND|FIRST|SECOND|THIRD|FOURTH|FIFTH|SIXTH|SEVENTH)\sFLOOR\b)', 0))
    df = df.withColumn('location_units_identified_flag', when(col('flat').isNotNull() | col('room').isNotNull() | col('unit').isNotNull() | col('block').isNotNull() | col('apartment').isNotNull() | col('floor').isNotNull(), lit(1)).otherwise(lit(0)))
    return df
  
#################################################################

def remove_unwanted_characters(df: DataFrame, address_col: str = "final_cleaned_address") -> DataFrame:
    original_col = col(address_col)  # Use col() function to reference the column
    df = df.withColumn(address_col, regexp_replace(original_col, r'\.|-|\bTO\b|\bOVER\b|\bNO\b|\bNO\.\b|\\|/', ' '))
    df = df.withColumn("unwanted_characters_removed_flag", when(col(address_col) != original_col, lit(1)).otherwise(0))
    return df