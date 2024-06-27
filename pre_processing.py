import pandas as pd
import re
from collections import Counter
from functools import reduce

import fuzzywuzzy
from fuzzywuzzy import fuzz
import openpyxl
import xlrd

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import pyspark.sql.functions as F
from pyspark.sql.functions import udf, regexp_replace, upper, col, when, length, split, regexp_extract, trim
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, IntegerType, StructType, StructField

####################################################################################

"""
    Function: clean_punctuation

    Purpose:
    This function processes and cleans addresses by:
    1. Replacing certain punctuation patterns seperated by comma
    2. Removing unwanted leading/trailing punctuation
    3. Splitting addresses into arrays based on comma
    4. Concatenating the cleaned parts back together
    5. Cleaning up any lingering punctuation
    6. Adding a flag to tell if the punctuation was cleaned or not

    Parameters:
    - df (DataFrame): The input DataFrame containing address data.
    - input_col (String): The name of the column containing addresses to be cleaned.

    Returns:
    - DataFrame: A DataFrame with additional columns: "cleaned_address", "address_array", "final_cleaned_address", and "punctuation_cleaned".
    
    
    Example:

    Suppose you have a DataFrame `df` with a column "raw_address" containing addresses:
    +-----------------------------+
    | raw_address                 |
    +-----------------------------+
    | -MAIN ROAD.-, LONDON,       |
    | .HIGH STREET,-MANCHESTER-.  |
    | ,PARK AVENUE.-.NEW YORK-    |
    | .,QUEEN ST.,,LONDON,-       |
    | -..KING AVENUE,-CHICAGO..-. |
    +-----------------------------+

    After applying the function:
    df = clean_punctuation(df, "raw_address")

    The df will have additional columns:
    +-----------------------------+------------------------+------------------------+-----------------------+
    | raw_address                 | cleaned_address        | address_array          | final_cleaned_address |
    +-----------------------------+------------------------+------------------------+-----------------------+
    | -MAIN ROAD.-, LONDON,       | MAIN ROAD, LONDON      | [MAIN ROAD,LONDON]     | MAIN ROAD, LONDON     |
    | .HIGH STREET,-MANCHESTER-.  | HIGH STREET,MANCHESTER | [HIGH STREET,MANCHESTER]| HIGH STREET, MANCHESTER|
    | ,PARK AVENUE.-.NEW YORK-    | PARK AVENUE, NEW YORK  | [PARK AVENUE,NEW YORK] | PARK AVENUE, NEW YORK |
    | .,QUEEN ST.,,LONDON,-       | QUEEN ST, LONDON       | [QUEEN ST,LONDON]      | QUEEN ST, LONDON      |
    | -..KING AVENUE,-CHICAGO..-. | KING AVENUE, CHICAGO   | [KING AVENUE,CHICAGO]  | KING AVENUE, CHICAGO  |
    +-----------------------------+------------------------+------------------------+-----------------------+

"""

# Function to check and reformat postcode
def clean_punctuation(df, input_col="supplied_query_address"):
    # handling punctuation marks and spaces
    df = df.withColumn("cleaned_address", 
                       regexp_replace(col(input_col), 
                                      r"[\s,.-]*\.,[\s,.-]*|[\s,.-]+\,|,\s*[\s,.-]+", 
                                      ", "))

    # identifying repeated punctuation and unwanted leading/trailing characters
    df = df.withColumn("cleaned_address", 
                       regexp_replace(col("cleaned_address"), 
                                      r",\s*,|(^[\s,.-]+)|([\s,.-]+$)", 
                                      ", "))

    # removing any lingering punctuation (a refresh)
    df = df.withColumn("final_cleaned_address", 
                       regexp_replace(col("cleaned_address"), 
                                      r"^[^a-zA-Z0-9]+|[^a-zA-Z0-9]+$", ""))

    # remove any occurrences of ', ,' in the final cleaned address
    df = df.withColumn("final_cleaned_address", 
                       regexp_replace(col("final_cleaned_address"), 
                                      r",\s*,", ", "))

    # removing extra spaces between words
    df = df.withColumn("final_cleaned_address", 
                       regexp_replace(col("final_cleaned_address"), 
                                      r"\s+", " "))

    # adding a punctuation_cleaned flag column
    df = df.withColumn("punctuation_cleaned_flag",
                       when(col(input_col) == col("final_cleaned_address"), 0).otherwise(1))

    return df

  


'''
# Usage example
# Suppose your column is named "address_to_clean"
cleaned_df = clean_punctuation(df, "address_to_clean")
'''
##############################################################################

def remove_noise_words_with_flag(df, input_col="final_cleaned_address"):
    """
    Removes noise words from the 'final_cleaned_address' column in a DataFrame that match the pattern
    of four or more identical capital letters in a row and adds a flag indicating if noise
    words were removed. This is particularly useful for cleaning up addresses or similar text fields
    where data may contain errors or irrelevant characters that can affect downstream processing or analysis.

    Parameters:
    df (dataframe): The input DataFrame.
    input_col (str): The name of the column to clean up, defaults to 'final_cleaned_address'.

    Returns:
    df: The DataFrame with noise words removed from 'final_cleaned_address' and a flag column indicating
        whether the cleanup was performed.

    Example:
    Given a DataFrame `df` with a column 'final_cleaned_address' that contains the values:
        1. '123 Main St AAAAA'
        2. '456 Pine St'
        3. '789 Elm St BBBB'

    The function changes the strings to:
        1. '123 Main St '
        2. '456 Pine St'
        3. '789 Elm St '

    Additionally, a new column 'noise_removed_flag' would indicate with a 1 or 0 if noise words were removed:
        1. 1 (indicating noise was removed)
        2. 0 (indicating no noise was found and thus not removed)
        3. 1 (indicating noise was removed)

    This helps in cleaning the data for more accurate analysis or processing while also tracking the changes made.
    """
    # Define the noise words pattern for four or more identical capital letters
    noise_pattern = r"\b([A-Z])\1{3,}\b"

    # Create a new column 'cleaned_address' that removes noise words from 'final_cleaned_address'
    df = df.withColumn("cleaned_address", regexp_replace(col(input_col), noise_pattern, ""))

    # Add a flag to indicate whether noise words were removed by comparing the new 'cleaned_address' with 'final_cleaned_address'
    df = df.withColumn("noise_removed_flag",
                       when(col("cleaned_address") != col(input_col), 1).otherwise(0))

    # Now update 'final_cleaned_address' with the contents of 'cleaned_address'
    df = df.withColumn(input_col, col("cleaned_address"))

    # Remove the intermediate 'cleaned_address' column as it's no longer needed
    df = df.drop("cleaned_address")

    return df
  
##############################################################################


def get_process_and_deduplicate_address_udf(column_name="final_cleaned_address"):
    """

    This UDF performs two main tasks:
    1. Merges similar consecutive parts of an address based on a similarity threshold.
    2. Removes exact duplicates from the address parts after merging.

    Returns:
    -  A Spark UDF that can be applied to a DataFrame column.
      The UDF takes a single address string and outputs a tuple:
      (cleaned_address, words_deduplicated_flag)
        - cleaned_address (str): The processed address with merged parts and removed duplicates.
        - words_deduplicated_flag (int): A flag (0 or 1) indicating whether any modifications were made.

    Example Usage:
    ---------------
    Apply the UDF to a DataFrame column named 'address' to clean and deduplicate address data:

    ```python
    # Assuming 'df' is your DataFrame containing an 'address' column
    process_udf = get_process_and_deduplicate_address_udf()
    df = df.withColumn("processed_data", process_udf(col("address")))

    # Split the results into separate columns and drop the original 'address' column if no longer needed
    df = df.withColumn("cleaned_address", col("processed_data.cleaned_address"))
    df = df.withColumn("words_deduplicated_flag", col("processed_data.words_deduplicated_flag"))
    df = df.drop("processed_data")
    ```
    This modifies the DataFrame to include 'cleaned_address' and 'words_deduplicated_flag' columns,
    showing the deduplicated addresses and whether any changes were made.
    """

    def process_and_deduplicate_address(address, threshold=95):
        def contains_numbers(s):
            return bool(re.search(r'\d', s))

        parts = [part.strip() for part in address.split(',')]
        processed = []
        seen = set()
        skip_next = False
        changes_made = False

        for i in range(len(parts)):
            if skip_next:
                skip_next = False
                continue

            current_part = parts[i]
            if i < len(parts) - 1:
                next_part = parts[i + 1]
                if fuzz.ratio(current_part, next_part) >= threshold and abs(len(current_part) - len(next_part)) < 3:
                    if contains_numbers(current_part) or not contains_numbers(next_part):
                        chosen_part = current_part
                    else:
                        chosen_part = next_part
                    processed.append(chosen_part)
                    skip_next = True
                    changes_made = True
                else:
                    processed.append(current_part)
            else:
                processed.append(current_part)

        final_parts = []
        for part in processed:
            if part not in seen:
                final_parts.append(part)
                seen.add(part)
            else:
                changes_made = True

        flag = 1 if changes_made else 0
        return (', '.join(final_parts), flag)

    return udf(process_and_deduplicate_address, StructType([
        StructField("cleaned_address", StringType(), True),
        StructField("words_deduplicated_flag", IntegerType(), True)
    ]))
    


###################################################################################


def dedupe_uk_postcode(df, input_col="final_cleaned_address"):
    """
    Deduplicates UK postcode prefixes from an address column in a DataFrame by normalising postcodes
    for consistent comparison and deduplication. This function specifically targets UK postcodes within
    the addresses, removing duplicate prefixes and normalising postcodes to ensure accuracy in deduplication.
    It also flags records where any form of deduplication has occurred.

    Args:
    - df (DataFrame): The input DataFrame.
    - input_col (str): The name of the address column in the DataFrame to deduplicate. Default is "final_cleaned_address".

    Returns:
    - DataFrame: The DataFrame with deduplicated postcodes and two flags indicating which records were modified for
                 word deduplication and postcode deduplication, respectively.
    """
    
    def normalise_postcode(part):
        """
        Normalises postal codes for comparison by removing spaces and special characters.
        
        Args:
        - part (str): A string part of the address, potentially a postcode.
        
        Returns:
        - str: A normalised version of the postcode.
        """
        return ''.join(filter(str.isalnum, part))  # Remove spaces and special characters

    def dedupe_uk_postcode_prefix(address):
        """
        Deduplicates UK postcode prefixes within an address string by normalising and comparing postcodes.
        Handles general duplicates and specifically targets postcode variations, marking deduplication with flags.
        
        Args:
        - address (str): The full address string from which postcode prefixes are to be deduplicated.
        
        Returns:
        - tuple: A tuple containing the deduplicated address and flags indicating modifications.
        """
        parts = [part.strip() for part in address.split(',')]
        seen_postcodes = {}
        seen_parts = set()
        deduplicated_parts = []
        postcodes_deduplicated = False
        words_deduplicated = False

        for part in parts:
            normalised_part = normalise_postcode(part)
            if part in seen_parts:
                words_deduplicated = True
                continue  # Skip general duplicates

            if normalised_part in seen_postcodes:
                # If we encounter a spaced version of an already seen postcode, replace it
                if ' ' in part and not ' ' in seen_postcodes[normalised_part]:
                    deduplicated_parts.remove(seen_postcodes[normalised_part])
                    deduplicated_parts.append(part)
                    seen_postcodes[normalised_part] = part
                    postcodes_deduplicated = True
                else:
                    words_deduplicated = True
            else:
                seen_postcodes[normalised_part] = part
                deduplicated_parts.append(part)

            seen_parts.add(part)

        # Set the deduplication flags
        words_deduplication_flag = 1 if words_deduplicated else 0
        postcodes_deduplication_flag = 1 if postcodes_deduplicated else 0

        return (', '.join(deduplicated_parts), words_deduplication_flag, postcodes_deduplication_flag)

    dedupe_udf = udf(dedupe_uk_postcode_prefix, StructType([
        StructField("final_cleaned_address", StringType()), 
        StructField("words_deduplicated_flag", IntegerType()), 
        StructField("postcodes_deduplicated_flag", IntegerType())
    ]))
    
    df = df.withColumn("results", dedupe_udf(col(input_col)))
    df = df.withColumn("final_cleaned_address", col("results.final_cleaned_address"))
    df = df.withColumn("words_deduplicated_flag", col("results.words_deduplicated_flag"))
    df = df.withColumn("postcodes_deduplicated_flag", col("results.postcodes_deduplicated_flag"))
    df = df.drop("results")
    
    return df