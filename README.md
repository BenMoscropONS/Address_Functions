' # Address Functions

This repository contains a collection of address processing functions, including pre-processing, quality flagging, result handling, and standardisation of address columns.

## Table of Contents

- [Project Structure](#project-structure)
- [Installation](#installation)
- [Usage](#usage)
- [Functions](#functions)
- [Testing](#testing)
- [Contributing](#contributing)
- [License](#license)

## Installation

To use the address functions, you need to have Python installed. You can clone the repository and install the required dependencies using the following commands:

```bash
git clone https://github.com/BenMoscropONS/Address_Functions.git
cd Address_Functions
pip install -r requirements.txt

## Functions
Pre_processing
The pre_processing.py module includes functions to clean and standardise address data and flag them.

Quality_Flags
The quality_flags.py module includes functions to flag addresses based on their quality, due to the different metrics in every function.

Results
The results.py module includes functions to handle how a dataframe is processed and in what form it is returned. 

SAC
The sac.py module includes functions related to the standardisation of address columns, to be Address lines, Town, and Postcode.

Streamlined Processing
The results.py module contains an overarching function that runs the pre_processing, quality flags, and SAC functions sequentially.
