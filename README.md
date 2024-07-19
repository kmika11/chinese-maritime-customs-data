# chinese-maritime-customs-data
 Curation scripts used for processing datasets from: https://dataverse.harvard.edu/dataverse/chinese_trade

# Documentation for Using the Curation Scripts

This documentation file explains how to use the provided scripts to process the Chinese Maritime Trade Statistics Collection datasets as described in the README file. The scripts help in preprocessing, cleaning, and organizing the datasets for analysis.

## Table of Contents
1. [Introduction](#introduction)
2. [Requirements](#requirements)
3. [Scripts Overview](#scripts-overview)
4. [Using the Jupyter Notebooks](#using-the-jupyter-notebooks)
5. [Common Issues and Troubleshooting](#common-issues-and-troubleshooting)
6. [Contact Information](#contact-information)

## Introduction
The Chinese Maritime Trade Statistics Collection includes datasets from various titles containing trade data from the 19th and early 20th centuries. The curation scripts are designed to preprocess, clean, and organize these datasets for research purposes.

## Requirements
Ensure you have the following software and packages installed before using the scripts:
- Python 3.8 or later
- Jupyter Notebook
- Pandas
- Numpy
- Spacy (for Named Entity Recognition)
- pyDataverse: (https://pydataverse.readthedocs.io/en/latest/)
- dvuploader: (https://github.com/gdcc/python-dvuploader)


## Scripts Overview
### Python Script: `curate.py`
This script handles the bulk processing of the datasets. It includes functions for reading, cleaning, labeling, and saving the data.

### Jupyter Notebooks
1. `curation_script_Chinese_Maritime_Trade_Data.ipynb`: Main notebook for processing the trade data.
2. `annual_trade_metadata_titles.ipynb`: Notebook for handling metadata related to annual trade reports.
3. `returns_trade_ports_titles.ipynb`: Notebook for processing returns of trade data by port.
4. `shanghai_returns_titles.ipynb`: Notebook specific to the Shanghai trade returns.
5. `trade_statistics_treaty_ports_titles.ipynb`: Notebook for processing trade statistics of treaty ports.

"..._titles.ipynb" notebooks are designed to preprocess the data files with labels, titles, and NER tags before curating and depositing into a Dataverse collection. 

### Using the Jupyter Notebooks
1. **Launch Jupyter Notebook**:
    ```bash
    jupyter notebook
    ```
    
2. **Open a Notebook**: Navigate to the directory containing the notebooks and open the desired notebook (e.g., `curation_script_Chinese_Maritime_Trade_Data.ipynb`).

4. **Run Cells**: Execute the cells in the notebook sequentially to process the data. Each notebook contains specific instructions and code cells designed to handle different aspects of the data.

5. **Notebook Functions**: for `curation_script_Chinese_Maritime_Trade_Data.ipynb`
    - **Prepare inventory data for curation**: Dataset metadata should be formatted at the file level. See inventory spreadsheet (chinese_maritime_customs_metadata_inventory.tab link) for an example on required headers for this curation script. Edit functions in curate.py to reflect your actual metadata fields. 
    - **Create Series Batches**: Cells that chunk files for upload into batches according to dataset/series. 
    - **Initialize pyDataverse API**: Initialize pyDataverse api wiht your API key to connect to Dataverse repository.
    - **Create all datasets**: Cells that create datasets from metadata spreadsheet. For each series, create a dataset and retain status information.
    - **Upload dataset datafiles**: Upload the datafiles associated with each dataset in a batch **MAKE SURE DIRECT UPLOAD IS ENABLED IN COLLECTION**. There is a known problem with the "registering files" process in the dvuploader python library. Usually throws 500 error, but works with small number of files (eg. fewer than 40). This code (in cureate.py) is designed to  loop batches with exceptions for 500 errors. It probably isn't a great idea to just ignore 500 errors and skip to the next series, but that's part of why batching is useful here.
    - **Publish datasets**: Cells publish all datasets in a collection (or sub-collection).
    - **Unlock datasets that were locked during publish process**: It is common for datasets to get held in the "lock" process for some reason after "publish" is triggered. The fix is to run the unlock request for all files in the collection and then republish. You can either republish the whole collection with the existing code, anticipating errors for datasets that have already been successfully published. Or you can manually publish in the UI.
    - **Create Inventories**: Combines all metadata files into one long dataframe for use as an inventory file. 

## Common Issues and Troubleshooting
- **File formatting**: For `..._titles.ipynb` notebooks, csv files must be properly formatted. If the encoding is funny, or there's column of values that are not aligned correctly, the `spreadsheet_maker()` will fail. It usually works to test whether a file can be read into a dataframe. If it can't then you have to manually inspect it, dpending on the error message, and go from there.
- **Missing Dependencies**: Ensure all required packages are installed. 
- **File Paths**: Verify that the file paths in the scripts and notebooks are correct.

## Contact Information
For any questions or further assistance, please contact:
- Katie Mika, Data Services Librarian, Harvard Library
- Email: katherine_mika@harvard.edu

Refer to the README file for additional context and detailed information about the datasets and their usage.
