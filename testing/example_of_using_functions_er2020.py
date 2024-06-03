from fuzzywuzzy import fuzz
import pyspark.sql.functions as F
import pandas as pd
import dlh_utils
import openpyxl
import xlrd
import re
import functools

from dlh_utils import utilities
from dlh_utils import dataframes
from dlh_utils import linkage
from dlh_utils import standardisation
from dlh_utils import sessions
from dlh_utils import profiling
from dlh_utils import flags
from pyspark.sql.functions import col, concat, lit
from pyspark.sql.functions import length
from pyspark.sql.functions import udf
from pyspark.sql.types import *
from pyspark.sql.functions import regexp_replace, trim
from pyspark.sql.functions import regexp_replace, split, array_distinct, concat_ws
from pyspark.sql import DataFrame
from pyspark.sql.functions import regexp_extract, col, trim, regexp_replace, when
from pyspark.sql.functions import count
from pyspark.sql.functions import sum as F_sum
from functools import reduce
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, IntegerType, StructType, StructField

from pyspark.sql import *
from pyspark.sql.functions import substring
from pyspark.sql.functions import when
from pyspark.sql.functions import udf
from functools import reduce
import re
from collections import Counter

# import modules with aliases

from address_index.address_functions.config.settings import town_list, alternative_town_list, contextual_keywords
from address_index.address_functions.config.settings import allowed_country_list, disallowed_country_list
from address_index.address_functions.config.settings import county_list

import address_index.address_functions.pre_processing as pre_processing
import address_index.address_functions.quality_flags as quality_flags
import address_index.address_functions.results as results
import address_index.address_functions.standardised_address_columns as standardised_address_columns

from address_index.address_functions.results import filter_and_count_all_flags_zero, filter_records_with_any_flags_set
from address_index.address_functions.standardised_address_columns import extract_postcode_town_address

# Use the aliases to access the modules
spark = pre_processing.sessions.getOrCreateSparkSession(appName='cleaning addresses', size='medium')

# reading in pandas, read in is simpler in pandas. Hence I have opted for this rather than spark.read
# this csv file is uploaded to cloudera, change this to suit your need. For me this was the area the csv was located. 
df = pd.read_excel('addr_index/data/ER2020_over_65_conf.xlsx')

df.columns

# making the dataframe to have spark utility 
df = utilities.pandas_to_spark(df)

# quickly cacheing this dataset, as it's only 100k rows... this is no probs
df.cache()

# making the address column uppercase, to catch the oddities i'm seeing with Regex easier

df = df.withColumn("supplied_query_address", F.upper(df.supplied_query_address))


######################################################################################

# Example usage:
processed_df = results.process_df_default(df, "supplied_query_address")


# currently I have to run all of the standardised_address_columns manually, will fix this at some point
final_df = extract_postcode_town_address(processed_df)

final_df.select("supplied_query_address","address_lines", "town", "postcode").show(1000, truncate=False)
