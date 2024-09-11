import pandas as pd
import re
from collections import Counter
from functools import reduce
from pyspark.sql import SparkSession, DataFrame

import openpyxl
import xlrd
from fuzzywuzzy import fuzz

import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame, udf
from pyspark.sql.functions import udf, regexp_replace, upper, col, when, length, split, regexp_extract, trim
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, IntegerType, StructType, StructField


from address_functions.pre_processing import (
    clean_punctuation, remove_noise_words_with_flag,
    get_process_and_deduplicate_address_udf, deduplicate_postcodes_udf, map_and_check_postcode
)

from address_functions.quality_flags import (
    add_length_flag, just_town_postcode, just_town_postcode_exact,
    just_country_postcode, just_country_postcode_exact,
    just_county_postcode, just_county_postcode_exact,
    keyword_present, all_3_criteria, has_country_and_ZZ99,
    country_in_last_half, is_invalid_postcode)

from address_functions.sac import (
    extract_postcode_town_address, standardise_street_types)



#################################################################################
"""
    Function: filter_and_count_all_flags_zero

    Purpose:
    This function processes the DataFrame by:
    1. Filtering the records based on flags.
    2. Excluding specified columns ("punctuation_cleaned_flag", "words_deduplicated_flag", "postcodes_deduplicated_flag") 
       from the flag-checking process.
    3. Checking for all flags set to 0 (excluding the columns we specify).
    4. Filtering the df to retain only rows where all of the checked flags are set to 0.
    5. Counting and printing the number of such records.

    Parameters:
    - df (dataFrame): The input DataFrame containing address data and associated flags.

    Returns:
    - df: A DataFrame that contains only the rows where all of the flags (excluding specific flags) are set to 0.
    
    Example:

    Suppose you have a dataframe `df` with several flag columns:
    +----------------+----------------------+-----------------------+------------------------+----------------------+
    | address        | punctuation_cleaned_flag | words_deduplicated_flag | postcodes_deduplicated_flag | other_flag |
    +----------------+----------------------+-----------------------+------------------------+----------------------+
    | MAIN ROAD      | 0                       | 0                      | 0                       | 0          |
    | HIGH STREET    | 1                       | 0                      | 0                       | 1          |
    | PARK AVENUE    | 0                       | 1                      | 0                       | 0          |
    +----------------+----------------------+-----------------------+------------------------+----------------------+

    After applying the function:
    df_filtered = filter_and_count_all_flags_zero(df)

    The df will contain:
    +----------------+----------------------+-----------------------+------------------------+----------------------+
    | address        | punctuation_cleaned_flag | words_deduplicated_flag | postcodes_deduplicated_flag | other_flag |
    +----------------+----------------------+-----------------------+------------------------+----------------------+
    | MAIN ROAD      | 0                       | 0                      | 0                       | 0          |
    +----------------+----------------------+-----------------------+------------------------+----------------------+

"""

def filter_and_count_all_flags_zero(df):
    from pyspark.sql.functions import col
    from functools import reduce

    exclusion_list = ["punctuation_cleaned_flag", "words_deduplicated_flag", "postcodes_deduplicated_flag", "postcodes_corrected_flag" , "noise_removed_flag", "changes_flag", "unwanted_characters_removed_flag",\
                     "street_type_standardised_flag", "patterns_identified_flag", "location_units_identified_flag", "unwanted_characters_removed_flag"]
    conditions = [col(column) == 0 for column in df.columns if column.endswith("_flag") and column not in exclusion_list]
    combined_condition = reduce(lambda x, y: x & y, conditions)
    filtered_df = df.filter(combined_condition)
    return filtered_df
  
#####################################################################

"""
    Function: filter_records_with_any_flags_set

    Purpose:
    This function processes the DataFrame by:
    1. Filtering the records based on flags.
    2. Excluding specified columns ("punctuation_cleaned_flag", "words_deduplicated_flag", "postcodes_deduplicated_flag") 
       from the flag-checking process.
    3. Checking for any flags set to 1 (excluding the aforementioned columns).
    4. Filtering the DataFrame to retain only rows where any of the checked flags are set to 1.

    Parameters:
    - df (DataFrame): The input DataFrame containing address data and associated flags.

    Returns:
    - df: A DataFrame that contains only the rows where any of the flags (excluding specific flags) are set to 1.
    
    Example:

    +----------------+----------------------+-----------------------+------------------------+----------------------+
    | address        | punctuation_cleaned_flag | words_deduplicated_flag | postcodes_deduplicated_flag | other_flag |
    +----------------+----------------------+-----------------------+------------------------+----------------------+
    | MAIN ROAD      | 0                       | 0                      | 0                       | 1          |
    | HIGH STREET    | 1                       | 0                      | 0                       | 0          |
    | PARK AVENUE    | 0                       | 1                      | 0                       | 0          |
    +----------------+----------------------+-----------------------+------------------------+----------------------+

    After applying the function:
    df_filtered = filter_records_with_any_flags_set(df)

    The DataFrame will contain:
    +----------------+----------------------+-----------------------+------------------------+----------------------+
    | address        | punctuation_cleaned_flag | words_deduplicated_flag | postcodes_deduplicated_flag | other_flag |
    +----------------+----------------------+-----------------------+------------------------+----------------------+
    | MAIN ROAD      | 0                       | 0                      | 0                       | 1          |
    | HIGH STREET    | 1                       | 0                      | 0                       | 0          |
    | PARK AVENUE    | 0                       | 1                      | 0                       | 0          |
    +----------------+----------------------+-----------------------+------------------------+----------------------+
"""


def filter_records_with_any_flags_set(df):
    from pyspark.sql.functions import col
    from functools import reduce

    exclusion_list = ["punctuation_cleaned_flag", "words_deduplicated_flag", "postcodes_deduplicated_flag", "postcodes_corrected_flag" , "noise_removed_flag", "changes_flag", "unwanted_characters_removed_flag",\
                     "street_type_standardised_flag", "patterns_identified_flag", "location_units_identified_flag", "unwanted_characters_removed_flag"]
    conditions = [col(column) == 1 for column in df.columns if column.endswith("_flag") and column not in exclusion_list]
    combined_condition = reduce(lambda x, y: x | y, conditions)
    flagged_df = df.filter(combined_condition)
    return flagged_df
  
##################################################################################

"""
    Function: process_function

    Purpose:
    this function processes the DataFrame through various cleaning, flagging, and transformation stages, including:
    1. Cleaning punctuation in the address string and flagging records that were cleaned.
    2. Deduplicating words within the address and flagging records that were deduplicated.
    3. Deduplicating UK postcodes within the address and flagging records that were deduplicated.
    4. Adding a flag for addresses based on their length.
    5. Adding a flag for addresses containing specific keywords.
    6. Applying and flagging based on a set of three criteria.
    7. Flagging addresses that have both a country name and a ZZ99 postcode pattern.
    8. Flagging addresses where the country name appears in the latter half of the address.
    9. Flagging addresses that contain invalid postcodes.
    10. Cleaning up the final address by stripping any leading and trailing commas or spaces.

    Parameters:
    - df (dataFrame): The input DataFrame containing address data.
    - address_string (String): The name of the column containing addresses to be processed.

    Returns:
    - df: A processed DataFrame with cleaned addresses, multiple flag columns indicating the transformations, and log prints detailing the number of records affected by each transformation stage.
    
    Example:

    Suppose you have a DataFrame `df` with a column "raw_address" containing various address inconsistencies.
    After applying the function:
    df_processed = process_function(df, "raw_address")

    The resulting df `df_processed` will have the "raw_address" processed and potentially several new flag columns, 
    each indicating if a particular processing step affected the record. Additionally, after each processing step, the function 
    will print the count of records affected by that step.
    
"""



def process_df_precise(df, address_string: str) -> DataFrame:
  
    
    
    # Convert address column to uppercase before processing
    df = df.withColumn(address_string, upper(df[address_string]))
    
    
    # Step 1: applying clean_punctuation
    df = clean_punctuation(df, address_string)
    punctuation_affected_count = df.filter(df.punctuation_cleaned_flag == 1).count()
    print(f"Punctuation cleaned count: {punctuation_affected_count}")

    # Step 2: applying remove_noise_words_with_flag
    df = remove_noise_words_with_flag(df, address_col)
    noise_affected_count = df.filter(df.noise_removed_flag == 1).count()
    print(f"Noise words removed count: {noise_affected_count}")

    # Step 3: applying process_and_deduplicate_address with UDF
    process_udf = get_process_and_deduplicate_address_udf()  # Get the UDF
    df = df.withColumn("output", process_udf(col(address_col)))
    df = df.withColumn(address_col, col("output.cleaned_address")) \
           .withColumn("words_deduplicated_flag", col("output.words_deduplicated_flag")) \
           .drop("output")
    deduplicated_affected_count = df.filter(df.words_deduplicated_flag == 1).count()
    print(f"Address deduplicated count: {deduplicated_affected_count}")

    # Step 4: applying dedupe_uk_postcode
    df = dedupe_uk_postcode(df)
    postcode_deduplicated_count = df.filter(df.postcodes_deduplicated_flag == 1).count()
    print(f"Postcodes deduplicated count: {postcode_deduplicated_count}")

    # Step 5: applying add_length_flag
    df = add_length_flag(df)
    length_affected_count = df.filter(df.length_flag == 1).count()
    print(f"Flagged by length: {length_affected_count}")

    # Step 6: applying just_country_postcode
    df = just_country_postcode(df)
    country_postcode_affected_count = df.filter(df.just_country_postcode_flag == 1).count()
    print(f"Number of addresses flagged for only having country and postcode: {country_postcode_affected_count}")

    # Step 7: applying just_county_postcode
    df = just_county_postcode(df)
    county_postcode_affected_count = df.filter(df.just_county_postcode_flag == 1).count()
    print(f"Number of addresses flagged for only having county and postcode: {county_postcode_affected_count}")

    # Step 8: applying just_town_postcode
    df = just_town_postcode(df)
    town_postcode_affected_count = df.filter(df.just_town_postcode_flag == 1).count()
    print(f"Number of addresses flagged for only having town and postcode: {town_postcode_affected_count}")

    # Step 9: applying keyword_present
    df = keyword_present(df)
    keyword_affected_count = df.filter(df.keyword_flag == 1).count()
    print(f"Flagged by keywords: {keyword_affected_count}")

    # Step 10: applying all_3_criteria
    df = all_3_criteria(df)
    criteria_affected_count = df.filter(df.criteria_flag == 1).count()
    print(f"Number of addresses flagged based on 3 criteria: {criteria_affected_count}")

    # Step 11: applying has_country_and_ZZ99
    df = has_country_and_ZZ99(df)
    country_and_ZZ99_affected_count = df.filter(df.country_postcode_flag == 1).count()
    print(f"Number of addresses flagged for country and ZZ99: {country_and_ZZ99_affected_count}")

    # Step 12: applying country_in_last_half
    df = country_in_last_half(df)
    country_position_affected_count = df.filter(df.country_position_flag == 1).count()
    print(f"Number of addresses with country in last half: {country_position_affected_count}")

    # Step 13: Count and print the number of addresses with invalid postcodes
    df = is_invalid_postcode(df)
    invalid_postcode_affected_count = df.filter(df.invalid_postcode_flag == 1).count()
    print(f"Number of addresses with invalid postcodes: {invalid_postcode_affected_count}")
    
    # Step 14: Extract postcode, town, and clean address lines
    df = extract_postcode_town_address(df, address_col)
    
    # a refresh of cleaning any lingering punctuation that is created by any other function
    df = clean_punctuation(df, input_col="final_cleaned_address")
    
    # dropping unnecessary columns that are created to make other functions work. 
    df = df.drop("address_array", "cleaned_address")
    
    # Step 15: Filter where all flags are zero and print count
    df_all_flags_zero = filter_and_count_all_flags_zero(df)
    count_all_flags_zero = df_all_flags_zero.count()
    print(f"Number of records where all flags (excluding specific flags) are 0: {count_all_flags_zero}")

    # Step 16: Filter where any flag is set and print count
    df_any_flags_set = filter_records_with_any_flags_set(df)
    count_any_flags_set = df_any_flags_set.count()
    print(f"Number of records where any flag (excluding specific flags) is set: {count_any_flags_set}")
    
    return df, df_all_flags_zero, df_any_flags_set

#################################################################################


def process_df_default(df: DataFrame, address_col: str) -> DataFrame:
    """
    Process a df containing address data using exact matching, with unwanted character removal integrated into clean_punctuation.

    Parameters:
    - df (DataFrame): The input DataFrame containing address data.
    - address_col (str): The name of the address column.

    Returns:
    - df: A new DataFrame with additional columns and flags indicating address validity.
    """

    # Step 1: Convert address column to uppercase
    df = df.withColumn(address_col, upper(df[address_col]))

    # Step 2: Clean punctuation (with unwanted character removal integrated)
    df = clean_punctuation(df, address_col)
    punctuation_cleaned_count = df.filter(df.punctuation_cleaned_flag == 1).count()
    print(f"Punctuation cleaned count: {punctuation_cleaned_count}")

    # Step 3: Remove noise words with flag
    df = remove_noise_words_with_flag(df)
    noise_words_removed_count = df.filter(df.noise_removed_flag == 1).count()
    print(f"Noise words removed count: {noise_words_removed_count}")

    # Step 4: Process and deduplicate address
    process_udf = get_process_and_deduplicate_address_udf()
    df = df.withColumn("output", process_udf(col("final_cleaned_address")))
    df = df.withColumn("final_cleaned_address", col("output.cleaned_address")) \
           .withColumn("words_deduplicated_flag", col("output.words_deduplicated_flag")) \
           .drop("output")
    address_deduplicated_count = df.filter(df.words_deduplicated_flag == 1).count()
    print(f"Address deduplicated count: {address_deduplicated_count}")

    # Step 5: Deduplicate UK postcodes
    deduplicate_postcodes = deduplicate_postcodes_udf()
    df = df.withColumn("output", deduplicate_postcodes(col("final_cleaned_address")))
    df = df.withColumn("final_cleaned_address", col("output.final_cleaned_address")) \
           .withColumn("postcodes_deduplicated_flag", col("output.changes_flag")) \
           .drop("output")
    postcodes_deduplicated_count = df.filter(df.postcodes_deduplicated_flag == 1).count()
    print(f"Postcodes deduplicated count: {postcodes_deduplicated_count}")

    # Step 6: Standardise street types
    df = standardise_street_types(df, "final_cleaned_address")
    street_type_standardised_count = df.filter(df.street_type_standardised_flag == 1).count()
    print(f"Street type standardised count: {street_type_standardised_count}")

    # Step 7: Extract postcode, town, and clean address lines
    df = extract_postcode_town_address(df, "final_cleaned_address")

    # Step 8: Add length flag
    df = add_length_flag(df, "final_cleaned_address")
    length_affected_count = df.filter(df.length_flag == 1).count()
    print(f"Flagged by length: {length_affected_count}")

    # Step 9: Just country postcode exact
    df = just_country_postcode_exact(df, "final_cleaned_address")
    country_postcode_affected_count = df.filter(df.just_country_postcode_flag == 1).count()
    print(f"Number of addresses flagged for only having country and postcode: {country_postcode_affected_count}")

    # Step 10: Just county postcode exact
    df = just_county_postcode_exact(df, "final_cleaned_address")
    county_postcode_affected_count = df.filter(df.just_county_postcode_flag == 1).count()
    print(f"Number of addresses flagged for only having county and postcode: {county_postcode_affected_count}")

    # Step 11: Just town postcode exact
    df = just_town_postcode_exact(df, "final_cleaned_address")
    town_postcode_affected_count = df.filter(df.just_town_postcode_flag == 1).count()
    print(f"Number of addresses flagged for only having town and postcode: {town_postcode_affected_count}")

    # Step 12: Keyword present
    df = keyword_present(df, "final_cleaned_address")
    keyword_affected_count = df.filter(df.keyword_flag == 1).count()
    print(f"Flagged by keywords: {keyword_affected_count}")

    # Step 13: All 3 criteria
    df = all_3_criteria(df, "final_cleaned_address")
    criteria_affected_count = df.filter(df.criteria_flag == 1).count()
    print(f"Number of addresses flagged based on 3 criteria: {criteria_affected_count}")

    # Step 14: Has country and ZZ99
    df = has_country_and_ZZ99(df, "final_cleaned_address")
    country_and_ZZ99_affected_count = df.filter(df.country_postcode_flag == 1).count()
    print(f"Number of addresses flagged for country and ZZ99: {country_and_ZZ99_affected_count}")

    # Step 15: Country in last half
    df = country_in_last_half(df, "final_cleaned_address")
    country_position_affected_count = df.filter(df.country_position_flag == 1).count()
    print(f"Number of addresses with country in last half: {country_position_affected_count}")

    # Step 16: Invalid postcodes
    df = is_invalid_postcode(df, "final_cleaned_address")
    invalid_postcode_affected_count = df.filter(df.invalid_postcode_flag == 1).count()
    print(f"Number of addresses with invalid postcodes: {invalid_postcode_affected_count}")

    # Final output
    df_all_flags_zero = filter_and_count_all_flags_zero(df)
    df_any_flags_set = filter_records_with_any_flags_set(df)
    
    all_flags_zero_count = df_all_flags_zero.count()
    any_flags_set_count = df_any_flags_set.count()
    
    print(f"Number of records where all flags (excluding specific flags) are 0: {all_flags_zero_count}")
    print(f"Number of records where any flag (excluding specific flags) is set: {any_flags_set_count}")

    return df, df_all_flags_zero, df_any_flags_set
  
  #########################################################################################################
  
def process_df_default_with_debug(df: DataFrame, address_col: str) -> DataFrame:
    """
    Process a df containing address data using exact matching, with intermediate debug outputs.

    Parameters:
    - df (DataFrame): The input DataFrame containing address data.
    - address_col (str): The name of the address column.

    Returns:
    - df: A new DataFrame with additional columns and flags indicating address validity.
    """

    # Step 1: Convert address column to uppercase
    print("Step 1: Uppercasing Address")
    df = df.withColumn("before_step", col(address_col))
    df = df.withColumn(address_col, upper(df[address_col]))
    df = df.withColumn("after_step", col(address_col))
    df.select("before_step", "after_step").show(30, truncate=False)

    # Step 2: Clean punctuation
    print("Step 2: Clean Punctuation")
    df = df.withColumn("before_step", col(address_col))
    df = clean_punctuation(df, address_col)
    df = df.withColumn("after_step", col(address_col))
    df.select("before_step", "after_step").show(30, truncate=False)

    # Step 3: Remove noise words with flag
    print("Step 3: Remove Noise Words")
    df = df.withColumn("before_step", col(address_col))
    df = remove_noise_words_with_flag(df)
    df = df.withColumn("after_step", col(address_col))
    df.select("before_step", "after_step").show(30, truncate=False)

    # Step 4: Process and deduplicate address
    print("Step 4: Deduplicate Address")
    df = df.withColumn("before_step", col(address_col))
    process_udf = get_process_and_deduplicate_address_udf()
    df = df.withColumn("output", process_udf(col("final_cleaned_address")))
    df = df.withColumn("final_cleaned_address", col("output.cleaned_address")) \
           .withColumn("words_deduplicated_flag", col("output.words_deduplicated_flag")) \
           .drop("output")
    df = df.withColumn("after_step", col("final_cleaned_address"))
    df.select("before_step", "after_step").show(30, truncate=False)

    # Step 5: Deduplicate UK postcodes
    print("Step 5: Deduplicate Postcodes")
    df = df.withColumn("before_step", col("final_cleaned_address"))
    deduplicate_postcodes = deduplicate_postcodes_udf()
    df = df.withColumn("output", deduplicate_postcodes(col("final_cleaned_address")))
    df = df.withColumn("final_cleaned_address", col("output.final_cleaned_address")) \
           .withColumn("postcodes_deduplicated_flag", col("output.changes_flag")) \
           .drop("output")
    df = df.withColumn("after_step", col("final_cleaned_address"))
    df.select("before_step", "after_step").show(30, truncate=False)

    # Step 6: Standardise street types
    print("Step 6: Standardise Street Types")
    df = df.withColumn("before_step", col("final_cleaned_address"))
    df = standardise_street_types(df, "final_cleaned_address")
    df = df.withColumn("after_step", col("final_cleaned_address"))
    df.select("before_step", "after_step").show(30, truncate=False)

    # Step 7: Remove unwanted characters
    print("Step 7: Remove Unwanted Characters")
    df = df.withColumn("before_step", col("final_cleaned_address"))
    df = remove_unwanted_characters(df, "final_cleaned_address")
    df = df.withColumn("after_step", col("final_cleaned_address"))
    df.select("before_step", "after_step").show(30, truncate=False)

    # Step 8: Extract postcode, town, and clean address lines
    print("Step 8: Extract Postcode and Town")
    df = df.withColumn("before_step", col("final_cleaned_address"))
    df = extract_postcode_town_address(df, "final_cleaned_address")
    df = df.withColumn("after_step", col("final_cleaned_address"))
    df.select("before_step", "after_step").show(30, truncate=False)

    # Step 9: Add length flag
    print("Step 9: Add Length Flag")
    df = df.withColumn("before_step", col("final_cleaned_address"))
    df = add_length_flag(df, "final_cleaned_address")
    df = df.withColumn("after_step", col("final_cleaned_address"))
    df.select("before_step", "after_step").show(30, truncate=False)

    # Step 10: Just country postcode
    print("Step 10: Flag Country and Postcode")
    df = df.withColumn("before_step", col("final_cleaned_address"))
    df = just_country_postcode(df, "final_cleaned_address")
    df = df.withColumn("after_step", col("final_cleaned_address"))
    df.select("before_step", "after_step").show(30, truncate=False)

    # Step 11: Just county postcode
    print("Step 11: Flag County and Postcode")
    df = df.withColumn("before_step", col("final_cleaned_address"))
    df = just_county_postcode(df, "final_cleaned_address")
    df = df.withColumn("after_step", col("final_cleaned_address"))
    df.select("before_step", "after_step").show(30, truncate=False)

    # Step 12: Just town postcode
    print("Step 12: Flag Town and Postcode")
    df = df.withColumn("before_step", col("final_cleaned_address"))
    df = just_town_postcode(df, "final_cleaned_address")
    df = df.withColumn("after_step", col("final_cleaned_address"))
    df.select("before_step", "after_step").show(30, truncate=False)

    # Step 13: Keyword present
    print("Step 13: Check Keywords")
    df = df.withColumn("before_step", col("final_cleaned_address"))
    df = keyword_present(df, "final_cleaned_address")
    df = df.withColumn("after_step", col("final_cleaned_address"))
    df.select("before_step", "after_step").show(30, truncate=False)

    # Step 14: All 3 criteria
    print("Step 14: Check All 3 Criteria")
    df = df.withColumn("before_step", col("final_cleaned_address"))
    df = all_3_criteria(df, "final_cleaned_address")
    df = df.withColumn("after_step", col("final_cleaned_address"))
    df.select("before_step", "after_step").show(30, truncate=False)

    # Step 15: Has country and ZZ99
    print("Step 15: Check Country and ZZ99")
    df = df.withColumn("before_step", col("final_cleaned_address"))
    df = has_country_and_ZZ99(df, "final_cleaned_address")
    df = df.withColumn("after_step", col("final_cleaned_address"))
    df.select("before_step", "after_step").show(30, truncate=False)

    # Step 16: Country in last half
    print("Step 16: Country in Last Half")
    df = df.withColumn("before_step", col("final_cleaned_address"))
    df = country_in_last_half(df, "final_cleaned_address")
    df = df.withColumn("after_step", col("final_cleaned_address"))
    df.select("before_step", "after_step").show(30, truncate=False)

    # Step 17: Invalid postcodes
    print("Step 17: Flag Invalid Postcodes")
    df = df.withColumn("before_step", col("final_cleaned_address"))
    df = is_invalid_postcode(df, "final_cleaned_address")
    df = df.withColumn("after_step", col("final_cleaned_address"))
    df.select("before_step", "after_step").show(30, truncate=False)

    return df