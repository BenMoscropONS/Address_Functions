import pandas as pd
import re
from collections import Counter
from functools import reduce

import dlh_utils
from dlh_utils import utilities
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.functions import udf, regexp_replace, upper, col, when, length, split, regexp_extract, trim
from pyspark.sql.types import StringType, IntegerType, StructType, StructField


# Import specific functions you plan to use from your custom modules
import address_functions.pre_processing as pre_processing
import address_functions.results as results
import address_functions.sac as sac  
from address_functions.config.settings import town_list


spark = dlh_utils.sessions.getOrCreateSparkSession(appName='demo', size='medium')


# reading in pandas, read in is simpler in pandas. Hence I have opted for this rather than spark.read
# this csv file is uploaded to cloudera, change this to suit your need. For me this was the area the csv was located. 
df = pd.read_csv('addr_index/data/pds_2022_under_65_conf.csv')

# making the dataframe to have spark utility 
df = utilities.pandas_to_spark(df)

df.limit(40).toPandas()



# quickly cacheing this dataset
df.cache()

# Assuming the process_df_default function returns three DataFrames: df, df_all_flags_zero, df_any_flags_set
df, df_all_flags_zero, df_any_flags_set = results.process_df_default(df, "supplied_query_address")

df.columns
df.limit(40).toPandas()

# if you'd like to see how each function behaves (isn't available for all):


######################################################################################

# CLEAN_PUNCTUATION

clean_punctuation_ex = pre_processing.clean_punctuation(df, "supplied_query_address")

clean_punctuation_ex.select("supplied_query_address","final_cleaned_address").where\
(clean_punctuation_ex.punctuation_cleaned_flag == 1).show(50, truncate=False)

#####################################################################################

# WORDS_DEDUPLICATED

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

any_flags_yes = filter_records_with_any_flags_set(testing_df)
                                                  
any_flags_yes.limit(40).toPandas()                                                  


