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
from pyspark.sql.functions import udf, regexp_replace, upper, col, when, length, split, regexp_extract, trim, concat_ws, lit
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
def clean_punctuation(df: DataFrame, input_col="supplied_query_address", create_flag=True):
    # Define a function to clean parts
    def clean_part(part):
        if part:
            # Replace hyphen between room numbers with a comma (e.g., "Room 7 - 1"), account for varying spaces
            part = re.sub(r'(Room\s+\d+)\s*-\s*(\d+)', r'\1, \2', part)

            # Preserve hyphens between numbers, even if there are spaces around the hyphen (e.g., "14 - 16")
            part = re.sub(r'(?<=\d)\s*-\s*(?=\d)', ' TEMP_HYPHEN ', part)  # Preserve hyphens between numbers, even with spaces
            
            # Preserve periods (.) between numbers (e.g., "14.16")
            part = re.sub(r'(?<=\d)\s*\.\s*(?=\d)', ' TEMP_DOT ', part)  # Preserve periods between numbers

            # Preserve hyphens in cases like "II-2" or "Gp2-4-B-7"
            part = re.sub(r'(?<=\w)-(?=\w)', ' TEMP_HYPHEN ', part)  # Preserve hyphens between alphanumeric chars
            part = re.sub(r'(?<=BLOCK\s\w)-(?=\d)', ' TEMP_HYPHEN ', part)  # Preserve hyphen in block names
            part = re.sub(r'(?<=\w)-(?=\d\w)', ' TEMP_HYPHEN ', part)  # Preserve hyphens like "C-11E"

            # Remove leading hyphens before numbers, except in preserved cases
            part = re.sub(r'-\s*(?=\d)', '', part)

            # Remove punctuation at the beginning and end
            part = re.sub(r"^[^a-zA-Z0-9]+|[^a-zA-Z0-9]+$", "", part)

            # Normalise whitespace
            part = re.sub(r"\s+", " ", part)

            # Restore preserved hyphens and periods
            part = part.replace(' TEMP_HYPHEN ', '-')
            part = part.replace(' TEMP_DOT ', '.')
        return part.strip()

    # Apply cleaning logic to each address part
    @udf(ArrayType(StringType()))
    def clean_parts_udf(parts):
        return [clean_part(part) for part in parts]

    # Step 1: Temporarily preserve hyphens between numbers or in special cases
    df = df.withColumn("cleaned_address", regexp_replace(col(input_col), r'(?<=\d)-(?=\d)', ' TEMP_HYPHEN '))
    df = df.withColumn("cleaned_address", regexp_replace(col("cleaned_address"), r'(?<=\d)\.\s*(?=\d)', ' TEMP_DOT '))
    df = df.withColumn("cleaned_address", regexp_replace(col("cleaned_address"), r'(?<=\w)-(?=\d\w)', ' TEMP_HYPHEN '))

    # Step 2: Handle punctuation marks and spaces, excluding preserved hyphens and periods
    df = df.withColumn("cleaned_address",
                       regexp_replace(col("cleaned_address"),
                                      r"[\s,.-]*\.,[\s,.-]*|[\s,.-]+\,|,\s*[\s,.-]+",
                                      ", "))
    df = df.withColumn("cleaned_address",
                       regexp_replace(col("cleaned_address"),
                                      r",\s*,|(^[\s,.-]+)|([\s,.-]+$)",
                                      ", "))

    # Step 3: Remove leading punctuation or commas at the start of the string (again after transformations)
    df = df.withColumn("cleaned_address", regexp_replace(col("cleaned_address"), r"^[,.\s]+", ""))

    # Step 4: Split the address into parts to handle each part separately
    df = df.withColumn("address_parts", split(col("cleaned_address"), ",\\s*"))

    # Step 5: Clean each part and join back into a single address string
    df = df.withColumn("cleaned_parts", clean_parts_udf(col("address_parts")))
    df = df.withColumn("final_cleaned_address", concat_ws(", ", col("cleaned_parts")))

    # Step 6: Restore preserved hyphens and periods in the final cleaned address
    df = df.withColumn("final_cleaned_address", regexp_replace(col("final_cleaned_address"), ' TEMP_HYPHEN ', '-'))
    df = df.withColumn("final_cleaned_address", regexp_replace(col("final_cleaned_address"), ' TEMP_DOT ', '.'))

    # Step 7: Remove any trailing commas and spaces in the final cleaned address
    df = df.withColumn("final_cleaned_address", regexp_replace(col("final_cleaned_address"), r",\s*$", ""))

    # Step 8: Create a flag indicating whether punctuation was cleaned
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
    import re
    
    # Define the UK postcode regex pattern
    postcode_regex = r"([Gg][Ii][Rr] 0[Aa]{2})|((([A-Za-z]\d{1,2})|(([A-Za-z][A-Ha-hJ-Yj-y]\d{1,2})|(([A-Za-z]\d[A-Za-z])|([A-Za-z][A-Ha-hJ-Yj-y]\d[A-Za-z]?))))\s?\d[A-Za-z]{2})"
    exclusion_list = ['STREET', 'ROAD', 'AVENUE', 'BOULEVARD', 'PLACE', 'LANE', 'DRIVE', 'TERRACE', 'ST']

    def is_valid_uk_postcode(postcode):
        return re.fullmatch(postcode_regex, postcode) is not None

    def extract_postcodes(address):
        return re.findall(postcode_regex, address)

    def ensure_postcode_format(postcode):
        # Ensure the postcode has the correct format (with space)
        postcode = postcode.replace(" ", "").replace("-", "").upper()
        if len(postcode) > 3:
            return postcode[:-3] + " " + postcode[-3:]
        return postcode

    def remove_prefix_duplicates(address, postcode):
        # Remove the postcode prefix (e.g., "LN96QB" in "LN96QB, 123 MAIN ROAD")
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

    def move_last_postcode_to_end(address):
        # Extract all valid postcodes from the address
        postcodes = extract_postcodes(address)
        if not postcodes:
            return address  # No valid postcodes found

        # Ensure the postcodes are in the correct format (with space)
        formatted_postcodes = [ensure_postcode_format(pc[0]) for pc in postcodes]

        # Remove all postcodes from the address string
        for pc in formatted_postcodes:
            address = re.sub(re.escape(pc), '', address)

        # Trim excess commas or spaces
        address = re.sub(r'\s*,\s*', ', ', address.strip(', '))

        # Append the last formatted postcode to the end of the string without trailing comma
        address = f"{address}, {formatted_postcodes[-1]}".rstrip(", ")

        return address

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

        new_address = ', '.join(deduplicated_parts)
        if new_address == address:
            changes_flag = 0  # Reset flag if no actual change occurred

        return new_address, changes_flag

    def deduplicate_postcodes(address):
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

        # Step 3: Remove prefix duplicates (improved logic)
        for pc in formatted_postcodes:
            if new_address.lower().count(pc.split()[0].lower()) > 1:
                new_address = remove_prefix_duplicates(new_address, pc)
                changes_flag = 1

        # Step 4: Remove duplicate postcodes (improved detection of duplicates)
        new_address, check4_flag = remove_duplicate_postcodes(new_address)
        changes_flag = changes_flag or check4_flag

        # Step 5: Ensure the last valid postcode with a space is placed at the end
        new_address = move_last_postcode_to_end(new_address)

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

    # The preferred postcode regex
    postcode_regex = r"([Gg][Ii][Rr] 0[Aa]{2})|((([A-Za-z]\d{1,2})|(([A-Za-z][A-Ha-hJ-Yj-y]\d{1,2})|(([A-Za-z]\d[A-Za-z])|([A-Za-z][A-Ha-hJ-Yj-y]\d[A-Za-z]?))))\s?\d[A-Za-z]{2})"

    def normalise_postcode(postcode):
        return postcode.replace(" ", "").replace("-", "").upper()

    def format_postcode(postcode):
        postcode = normalise_postcode(postcode)
        if len(postcode) > 3:
            return postcode[:-3] + " " + postcode[-3:]
        return postcode

    def is_valid_postcode(postcode):
        # Use the preferred postcode regex
        return re.fullmatch(postcode_regex, postcode) is not None

    def looks_like_postcode(part):
        # Check if a part looks like a UK postcode (combination of letters and numbers)
        return bool(re.search(r'[A-Za-z0-9]', part)) and not part.isalpha()  # Ensure it's not just a word

    def correct_postcode(postcode):
        # Character mapping for commonly misinterpreted characters
        char_map = {
            'I': '1', 'O': '0', 'S': '5', 'Z': '2',
            'B': '8', 'D': '0', 'G': '6', 'J': '1',
            'A': '4', 'E': '3', 'H': '4', 'L': '1',
            'U': '0', 'Y': '4', 'C': '0', 'K': '1', 'M': '1'
        }
        return ''.join([char_map.get(char, char) for char in postcode])

    def map_and_check_postcode(address):
        parts = [part.strip() for part in address.split(',')]
        changes_flag = 0
        valid_postcode_found = False

        print(f"Processing address: {address}")  # Debugging statement

        # First, check if a valid UK postcode already exists in the string
        for part in parts:
            if is_valid_postcode(part):
                valid_postcode_found = True
                print(f"Valid postcode found: {part}")  # Debugging statement
                break  # Exit if a valid postcode is found

        # If no valid postcode was found, apply mapping and correction
        if not valid_postcode_found:
            for i, part in enumerate(parts):
                print(f"Checking part: {part}")  # Debugging statement
                # Only apply mapping to parts that look like they might be postcodes (but not words)
                if looks_like_postcode(part) and not is_valid_postcode(part):
                    print(f"Applying correction to part: {part}")  # Debugging statement
                    corrected = correct_postcode(part)
                    if is_valid_postcode(corrected):
                        print(f"Corrected postcode: {corrected}")  # Debugging statement
                        parts[i] = format_postcode(corrected)
                        changes_flag = 1  # Set the flag if a valid postcode is corrected

        final_address = ', '.join(parts)
        return (final_address, changes_flag)

    map_and_check_postcode_udf = udf(map_and_check_postcode, StructType([
        StructField("final_cleaned_address", StringType()), 
        StructField("changes_flag", IntegerType())
    ]))

    return {
        "map_and_check_postcode_udf": map_and_check_postcode_udf
    }
  
#############################################################################################

def standardise_street_types(df, address_col="final_cleaned_address"):
    # Save the original column for comparison later
    original_column = col(address_col)

    # Apply standardization rules for street types
    df = df.withColumn(address_col, regexp_replace(col(address_col), r'\bSTR\b|\bSTRT\b', 'STREET'))
    df = df.withColumn(address_col, regexp_replace(col(address_col), r'\bST\b(?!REET\b)', 'STREET'))
    df = df.withColumn(address_col, regexp_replace(col(address_col), r'\bRD\b|RAOD', 'ROAD'))
    df = df.withColumn(address_col, regexp_replace(col(address_col), r'\bAVE\b|\bAVE\.\b|\bAVENEU\b', 'AVENUE'))
    df = df.withColumn(address_col, regexp_replace(col(address_col), r'\bCRT\b|\bCRT\.\b|\bCT\b', 'COURT'))
    df = df.withColumn(address_col, regexp_replace(col(address_col), r'\bCRESENT\b|\bCRSNT\b', 'CRESCENT'))
    df = df.withColumn(address_col, regexp_replace(col(address_col), r'\bDRV\b|\bDR\b', 'DRIVE'))
    df = df.withColumn(address_col, regexp_replace(col(address_col), r'\bGRDN(?=S\b)?\b|\bGDN(?=S\b)?\b', 'GARDEN'))
    df = df.withColumn(address_col, regexp_replace(col(address_col), r'\bPK\b', 'PARK'))
    df = df.withColumn(address_col, regexp_replace(col(address_col), r'\bCL\b', 'CLOSE'))

    # Add a flag to indicate if any standardization has occurred by comparing the original column and the modified one
    df = df.withColumn(
        'street_type_standardised_flag',
        when(col(address_col) != original_column, lit(1)).otherwise(lit(0))
    )

    return df
  