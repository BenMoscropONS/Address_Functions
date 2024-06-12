# Address Functions

This repository contains a collection of address processing functions, including pre-processing, quality flagging, result handling, and standardization of address columns. 

# Functions: 

# Pre_processing 
- the "pre_processing" module has functions to clean, standardise and flag address data.
  
# Quality_flags 
The "quality_flags" module has functions to flag addresses based on their quality, what each flag specifically means can be found out in the doc strings of quality_flags

# Results  
The "results" module contains an overarching function that runs the pre_processing, quality_flags, and sac functions sequentially. The output is defined here too.

# Sac 
the "sac" module, which stands for Standardised Address Columns, pertains to extracting information from the address string to create "address_lines", "town" and "postcode.

# Example usage can be seen in the testing scripts, however a simple version is:

from results import process_df_default

'example dataframe'
data = {
    "supplied_query_address": [
        "10 Downing St, Westminster, London SW1A 2AA, UK",
        "221B Baker St, Marylebone, London NW1 6XE, UK"
    ]
}
df = pd.DataFrame(data)

'Process the DataFrame'

processed_df = process_df_default(df)

# Testing 
In the testing folder is an example script of implementing the functions on a dataframe




