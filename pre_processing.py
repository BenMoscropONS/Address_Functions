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
from pyspark.sql.functions import udf, regexp_replace, upper, col, when, length, split, regexp_extract, trim, concat_ws
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, IntegerType, StructType, StructField, ArrayType

from address_functions.config.settings import town_list

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

    Suppose you have a DataFrame df with a column "raw_address" containing addresses:
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

def clean_punctuation(df, input_col="supplied_query_address", create_flag=True):
    # Define a function to check if a part is a town
    def is_town(part):
        return part.upper() in town_list

    # Define a function to clean parts
    def clean_part(part):
        if part and not is_town(part):
            part = re.sub(r"^[^a-zA-Z0-9]+|[^a-zA-Z0-9]+$", "", part)
            part = re.sub(r"\s+", " ", part)
        return part

    # Define a UDF that applies the clean_part function to each element in the list
    @udf(ArrayType(StringType()))
    def clean_parts_udf(parts):
        return [clean_part(part) for part in parts]

    # Handling punctuation marks and spaces
    df = df.withColumn("cleaned_address",
                       regexp_replace(col(input_col),
                                      r"[\s,.-]*\.,[\s,.-]*|[\s,.-]+\,|,\s*[\s,.-]+",
                                      ", "))
    df = df.withColumn("cleaned_address",
                       regexp_replace(col("cleaned_address"),
                                      r",\s*,|(^[\s,.-]+)|([\s,.-]+$)",
                                      ", "))

    # Split the address into parts to handle each part separately
    df = df.withColumn("address_parts", split(col("cleaned_address"), ",\\s*"))

    # Clean parts and join back into a single address string
    df = df.withColumn("cleaned_parts", clean_parts_udf(col("address_parts")))
    df = df.withColumn("final_cleaned_address", concat_ws(", ", col("cleaned_parts")))

    if create_flag:
        df = df.withColumn("punctuation_cleaned_flag",
                           when(col(input_col) == col("final_cleaned_address"), 0).otherwise(1))
    else:
        df = df.withColumn("punctuation_cleaned_flag",
                           when(col("punctuation_cleaned_flag").isNotNull(), col("punctuation_cleaned_flag"))
                           .otherwise(when(col(input_col) == col("final_cleaned_address"), 0).otherwise(1)))

    # Drop intermediate columns
    df = df.drop("cleaned_address", "address_parts", "cleaned_parts")

    return df

  


##############################################################################

def remove_noise_words_with_flag(df, input_col="final_cleaned_address"):
    noise_pattern = r"\b([A-Z])\1{3,}\b"
    df = df.withColumn("cleaned_address", regexp_replace(col(input_col), noise_pattern, ""))
    df = df.withColumn("noise_removed_flag",
                       when(col("cleaned_address") != col(input_col), 1).otherwise(0))
    df = df.withColumn(input_col, col("cleaned_address"))
    df = df.drop("cleaned_address")
    return df
  
##############################################################################


def get_process_and_deduplicate_address_udf(column_name="final_cleaned_address"):
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



def deduplicate_postcodes_udf():
    def deduplicate_postcodes(address):
        import re

        postcode_regex = r"([Gg][Ii][Rr] 0[Aa]{2})|((([A-Za-z]\d{1,2})|(([A-Za-z][A-Ha-hJ-Yj-y]\d{1,2})|(([A-Za-z]\d[A-Za-z])|([A-Za-z][A-Ha-hJ-Yj-y]\d[A-Za-z]?))))\s?\d[A-Za-z]{2})"
        exclusion_list = ['STREET', 'ROAD', 'AVENUE', 'BOULEVARD', 'PLACE', 'LANE', 'DRIVE', 'TERRACE']

        def is_valid_uk_postcode(postcode):
            return re.fullmatch(postcode_regex, postcode) is not None

        def extract_postcodes(address):
            return re.findall(postcode_regex, address)

        def ensure_postcode_format(postcode):
            postcode = postcode.replace(" ", "").replace("-", "").upper()
            if len(postcode) > 3:
                return postcode[:-3] + " " + postcode[-3:]
            return postcode

        def remove_prefix_duplicates(address, postcode):
            prefix = postcode.split()[0]
            prefix_pattern = re.compile(r'\b{}\b'.format(re.escape(prefix)), re.IGNORECASE)
            parts = [part.strip() for part in re.split(r',\s*', address)]
            deduplicated_parts = []
            for part in parts:
                if prefix_pattern.fullmatch(part):
                    continue
                deduplicated_parts.append(part)
            return ', '.join(deduplicated_parts)

        def normalise_postcode(postcode):
            return re.sub(r'[\s-]', '', postcode.upper())

        def remove_duplicate_postcodes(address):
            parts = [part.strip() for part in re.split(r',\s*', address)]
            normalised_postcodes = {normalise_postcode(part) for part in parts if is_valid_uk_postcode(normalise_postcode(part))}
            deduplicated_parts = []
            seen_postcodes = set()
            changes_flag = 0

            for part in parts:
                normalised = normalise_postcode(part)
                if normalised in normalised_postcodes:
                    if normalised not in seen_postcodes:
                        seen_postcodes.add(normalised)
                        deduplicated_parts.append(part)
                    else:
                        changes_flag = 1  # Duplicate postcode found
                else:
                    deduplicated_parts.append(part)

            return ', '.join(deduplicated_parts), changes_flag

        # Ensure specific parts like STREET and hyphenated numbers are not altered
        def should_preserve_part(part):
            if any(exclusion in part for exclusion in exclusion_list):
                return True
            if re.search(r'\d+[-]\d+', part) and not is_valid_uk_postcode(part):
                return True
            return False

        postcodes = extract_postcodes(address)
        if not postcodes:
            return address, 0  # No valid postcodes found

        formatted_postcodes = []
        for pc_tuple in postcodes:
            try:
                pc = next(pc for pc in pc_tuple if pc)  # Find the non-empty match
                formatted_postcode = ensure_postcode_format(pc)
                formatted_postcodes.append(formatted_postcode)
            except (IndexError, StopIteration) as e:
                print(f"Error formatting postcode: {pc_tuple}, {e}")

        changes_flag = 0
        new_address = address

        # Step 3: Remove prefix duplicates
        for pc in formatted_postcodes:
            if new_address.lower().count(pc.split()[0].lower()) > 1:
                new_address = remove_prefix_duplicates(new_address, pc)
                changes_flag = 1

        # Step 4: Remove duplicate postcodes
        new_address, check4_flag = remove_duplicate_postcodes(new_address)
        changes_flag = changes_flag or check4_flag

        # Step 5: Ensure specific parts and hyphens are not altered
        parts = [part.strip() for part in re.split(r',\s*', new_address)]
        final_parts = []
        for part in parts:
            if should_preserve_part(part):
                final_parts.append(part)
            else:
                final_parts.append(part)
        new_address = ', '.join(final_parts)

        return new_address, changes_flag

    return udf(deduplicate_postcodes, StructType([
        StructField("final_cleaned_address", StringType()), 
        StructField("changes_flag", IntegerType())
    ]))
  
  
###################################################################################
  
def map_and_check_postcode():
    import re
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType

    def normalise_postcode(postcode):
        return postcode.replace(" ", "").replace("-", "").upper()

    def format_postcode(postcode):
        postcode = normalise_postcode(postcode)
        if len(postcode) > 3:
            return postcode[:-3] + " " + postcode[-3:]
        return postcode

    def is_valid_postcode(postcode):
        postcode_regex = r"\b[A-Za-z]{1,2}\d{1,2}[A-Za-z]?\d[A-Za-z]{2}\b"
        return re.match(postcode_regex, postcode) is not None

    def map_and_check_postcode(address):
        char_map = {
            'I': '1', 'O': '0', 'S': '5', 'Z': '2',
            'B': '8', 'D': '0', 'G': '6', 'J': '1',
            'A': '4', 'E': '3', 'H': '4', 'L': '1',
            'U': '0', 'Y': '4', 'C': '0', 'K': '1', 'M': '1'
        }

        def correct_postcode(postcode):
            return ''.join([char_map.get(char, char) for char in postcode])

        parts = [part.strip() for part in address.split(',')]
        changes_flag = 0
        valid_postcode_found = any(is_valid_postcode(part) for part in parts)

        if not valid_postcode_found:
            for i, part in enumerate(parts):
                corrected = correct_postcode(part)
                if is_valid_postcode(corrected):
                    parts[i] = corrected
                    changes_flag = 1  # Corrected postcode found, set the flag

        final_address = ', '.join(parts)
        return (final_address, changes_flag)

    map_and_check_postcode_udf = udf(map_and_check_postcode, StructType([
        StructField("final_cleaned_address", StringType()), 
        StructField("changes_flag", IntegerType())
    ]))

    return {
        "map_and_check_postcode_udf": map_and_check_postcode_udf
    }