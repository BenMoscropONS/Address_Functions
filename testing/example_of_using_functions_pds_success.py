import pandas as pd
import re
from collections import Counter
from functools import reduce

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.functions import udf, regexp_replace, upper, col, when, length, split, regexp_extract, trim
from pyspark.sql.types import StringType, IntegerType, StructType, StructField


# Import specific functions you plan to use from your custom modules
import address_functions.pre_processing as pre_processing
import address_functions.results as results
import address_functions.sac as sac  
from address_functions.config.settings import town_list






spark = (
  SparkSession.builder.appName("medium-session")
  .config("spark.executor.memory", "8g") # memory of session
  .config("spark.executor.cores", 1)
  .config("soark.dynamicAllocation.enabled", "true")
  .config("spark.dynamicAllocation.maxExecutors", 3)
  .config("spark.sql.shuffle.partitions", "100") # this is number of partitions 
  .config("spark.shuffle.service.enabled", "true")
  .config("spark.ui.showConsoleProgress", "false") # true will show progress lines
  .config("spark.io.compression.codec", "lz4") # this makes your data more compressed
  .config("spark.sql.repl.eagerEval.enabled", "true") # this makes data scrollable
  .enableHiveSupport()
  .getOrCreate()
)

# reading in pandas, read in is simpler in pandas. Hence I have opted for this rather than spark.read
# this csv file is uploaded to cloudera, change this to suit your need. For me this was the area the csv was located. 
df = pd.read_csv('addr_index/data/pds_2022_under_65_conf.csv')

# making the dataframe to have spark utility 
df = utilities.pandas_to_spark(df)

df.limit(40).toPandas()



# quickly cacheing this dataset, as it's only 100k rows... this is no probs
df.cache()

# Assuming the process_df_default function returns three DataFrames: df, df_all_flags_zero, df_any_flags_set
df, df_all_flags_zero, df_any_flags_set = results.process_df_default(df, "supplied_query_address")

df.limit(40).toPandas()

testing_df.limit(40).toPandas()

######################################################################################

# CLEAN_PUNCTUATION

clean_punctuation_ex = pre_processing.clean_punctuation(df, "supplied_query_address")

clean_punctuation_ex.select("supplied_query_address","final_cleaned_address").where\
(clean_punctuation_ex.punctuation_cleaned_flag == 1).show(50, truncate=False)

#####################################################################################

# NOISE_REMOVED_FLAG

# BECAUSE OF HOW I HAVE THE LOGIC, IT IS DIFFICULT TO SPECIFICALLY SHOW THIS ONE.
# this is because it is designed to work on "final_cleaned_address", however in the original df, that
# variable doesn't exist. For testing purposes I can cheat and run the whole process_df and filter
# for the flagged ones

noise = pre_processing.remove_noise_words_with_flag(df, "supplied_query_address")


noise.select("supplied_query_address").where\
(noise.noise_removed_flag == 1).show(50, truncate=False)
#####################################################################################

# WORDS_DEDUPLICATED

# same logic as above

'''also in this function I have a part that deduplicates postcode, it's on the 'to-do' list
to move specifically that part to "dedupe_uk_postcode. This shouldnt make a huge difference, as 
the final output will be the exact same and discrepency would only be found when going through function
by function, rather than the intended process_df function.'''  



df.select("supplied_query_address","final_cleaned_address").where\
(df.words_deduplicated_flag == 1).show(50, truncate=False)

#####################################################################################

DEDUPE_UK_POSTCODE

testing_df.select("supplied_query_address","final_cleaned_address").where\
(testing_df.postcodes_deduplicated_flag == 1).show(50, truncate=False)

######################################################################################

# LENGTH_FLAG


testing_df.select("final_cleaned_address").where\
(testing_df.length_flag == 1).show(50, truncate=False)

######################################################################################

# JUST_TOWN_POSTCODE_EXACT (DEFAULT)

testing_df.select("supplied_query_address","final_cleaned_address").where\
(testing_df.just_town_postcode_flag == 1).show(50, truncate=False)


######################################################################################

# JUST COUNTRY_POSTCODE_EXACT (DEFAULT)

testing_df.select("supplied_query_address","final_cleaned_address").where\
(testing_df.just_country_postcode_flag == 1).show(50, truncate=False)

######################################################################################

#JUST COUNTY_POSTCODE_EXACT (DEFAULT)

testing_df.select("supplied_query_address","final_cleaned_address").where\
(testing_df.just_county_postcode_flag == 1).show(50, truncate=False)

######################################################################################

# KEYWORD PRESENT

testing_df.select("supplied_query_address","final_cleaned_address").where\
(testing_df.keyword_flag == 1).show(50, truncate=False)

######################################################################################

# ALL_3_CRITERIA

testing_df.select("supplied_query_address","final_cleaned_address").where\
(testing_df.criteria_flag == 1).show(50, truncate=False)

######################################################################################

# HAS_COUNTRY_AND_ZZ99

testing_df.select("supplied_query_address","final_cleaned_address").where\
(testing_df.country_postcode_flag == 1).show(50, truncate=False)

######################################################################################

# COUNTRY_IN_LAST_HALF (WORK IN PROGRESS)

testing_df.select("supplied_query_address","final_cleaned_address").where\
(testing_df.country_position_flag == 1).show(50, truncate=False)

######################################################################################

# IS_INVALID_POSTCODE

testing_df.select("supplied_query_address","final_cleaned_address").where\
(testing_df.invalid_postcode_flag == 1).show(50, truncate=False)

######################################################################################

# FILTER_AND_COUNT_ALL_FLAGS_ZERO

all_flags_zero = filter_and_count_all_flags_zero(testing_df)


######################################################################################

# FILTER_WITH_ANY_FLAGS_YES

any_flags_yes = filter_records_with_any_flags_set(testing_df
                                                  
any_flags_yes.limit(40).toPandas()                                                  

######################################################################################

# EXTRACT_TOWN

######################################################################################



######################################################################################

######################################################################################

######################################################################################

######################################################################################

######################################################################################

######################################################################################

# Running the process_df function which runs them all in the order that is most suitable
# for the average dataset.























# Example usage:
processed_df = results.process_df_default(df, "supplied_query_address")

processed_df.columns

#punctuation flag
processed_df.select("supplied_query_address","final_cleaned_address").where(processed_df.punctuation_cleaned_flag == 1).show(50, truncate=False)

# noise words
processed_df.select("supplied_query_address","final_cleaned_address").where(processed_df.noise_removed_flag == 1).show(50, truncate=False)

# address deduplicated
processed_df.select("supplied_query_address","final_cleaned_address").where(processed_df.invalid_postcode_flag == 1).show(50, truncate=False)

# the default dataframe we'd suggest sending to aims
filtered_all_no = filter_and_count_all_flags_zero(processed_df)

# all of the records that have positive flags for one or more of the quality related flags. (look at read_me.txt for more info)
filtered_any_yes = filter_records_with_any_flags_set(processed_df)

filtered_all_no.select("final_cleaned_address").show(1000, truncate=False)
filtered_any_yes.select("final_cleaned_address").show(1000, truncate=False)
