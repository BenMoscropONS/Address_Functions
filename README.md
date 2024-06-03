'''
to look at the records that are affected by each function, use the flag associated with the
function if it has one. 

So i have added in a function that basically runs them in the correct order for the user. this is the function
"process_df". This is the order of that function

1) clean_punctuation
2) merge_similar_words
3) deduplicate_address_and_flag
4) dedupe_uk_postcode
5) add_length_flag
5) keyword_present
6) all_3_criteria
7) has_country_and_ZZ99
8) country_in_last_half
9) is_invalid_postcode

then extra functions "filter_records_with_any_flags_set", and "filter_and_count_all_flags_zero"
which act on our processed dataframe to give us the dataframe we want to send to AIMS, and also give us
a dataframe to quality check

there is a flag for:
(clean_punctuation, (merge_similar_words + remove_repeats_with_flag),\
dedupe_uk_postcode, add_length_flag, keyword_present, all_3_criteria, has_country_and_ZZ99, \
country_in_last_half, is_invalid_postcode)

The flags that do not discern data quality are the flags associated with: 
(clean_punctuation, (merge_similar_words + remove_repeats_with_flag), dedupe_uk_postcode)

The flags related to data quality, are:
(add_length_flag, keyword_present, all_3_criteria, has_country_and_ZZ99, country_in_last_half, is_invalid_postcode)

as a default I would suggest only to send the "all_no" records as seen in summary_statistics
(the records that aren't flagged by one or more of the quality associated flags)
'''
#################

Important note: 

the functions: just_town_postcode, just_country_postcode, and just_county_postcode take a considerable
amount of time to run in the "process_df_precise" function, this is because they use fuzzy matching. This makes it more accurate, but at the
expense of run time. If it is less than 100k records I would suggest to use the fuzzy matching. But
any more and I'd suggest using the exact matching
