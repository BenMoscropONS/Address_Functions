# Address Functions

This repository contains a collection of address processing functions, including pre-processing, quality flagging, result handling, and standardization of address columns. These functions are designed to help with the efficient handling and processing of address data.

## Table of Contents

- [Project Structure](#project-structure)
- [Installation](#installation)
- [Usage](#usage)
- [Functions](#functions)
- [Streamlined Processing](#streamlined-processing)
- [Testing](#testing)
- [Contributing](#contributing)
- [License](#license)

## Project Structure

Address_Functions/
├── init.py
├── LICENSE.txt
├── README.md
├── config/
│ ├── init.py
│ ├── settings.py
├── pre_processing.py
├── pyproject.toml
├── quality_flags.py
├── results.py
├── sac.py
├── testing/
│ ├── example_of_using_functions_er2020.py
│ ├── example_of_using_functions_land_success.py
│ ├── example_of_using_functions_pds_success.py

bash
Copy code

## Installation

To use the address functions, you need to have Python installed. You can clone the repository and install the required dependencies using the following commands:

```bash
git clone https://github.com/BenMoscropONS/Address_Functions.git
cd Address_Functions
pip install -r requirements.txt
Usage
You can import and use the functions in your Python scripts. Here is an example of how to use the overarching function process_df_default to process a DataFrame with a column named "supplied_query_address":

python
Copy code
import pandas as pd
from results import process_df_default

# Example DataFrame
data = {
    "supplied_query_address": [
        "10 Downing St, Westminster, London SW1A 2AA, UK",
        "221B Baker St, Marylebone, London NW1 6XE, UK"
    ]
}
df = pd.DataFrame(data)

# Process the DataFrame
processed_df = process_df_default(df)
print(processed_df)
Functions
Pre-processing
The pre_processing.py module includes functions to clean and standardize address data.

Quality Flags
The quality_flags.py module includes functions to flag addresses based on their quality.

Results
The results.py module includes functions to handle and process the results of address processing.

SAC
The sac.py module includes functions related to the standardization of address columns.

Streamlined Processing
The results.py module contains an overarching function that runs the pre-processing, quality flagging, and standardization functions sequentially. This streamlines the process of handling address data, ensuring that all necessary steps are completed in the correct order.

Example Usage
Here is an example of how to use the overarching function in the results.py module:

python
Copy code
from results import process_address_data

address_data = "10 Downing St, Westminster, London SW1A 2AA, UK"
processed_data = process_address_data(address_data)
print(processed_data)
Testing
Unit tests are included in the testing directory. You can run the tests using the following command:

bash
Copy code
python -m unittest discover -s testing
Contributing
Contributions are welcome! Please fork the repository and submit a pull request for any changes you'd like to make. Make sure to follow the code style and include tests for any new functionality.

License
This project is licensed under the MIT License. See the LICENSE file for details.
