# ETL process

This project is an ETL (Extract, Transform, Load) process that is used for extracting data from various sources, transforming it into a format that can be used for analysis and loading it into a destination database or storage.

## Installation

To install the required packages, run the following command:
```
pip install -r requirements.txt
```
## Usage
To run the example command, use the following command:

```
python main.py --data-source kontrakty_godzinowe --date-from DATE_FROM --date-to DATE_TO
```
or

```
python main.py --data-source wielkosci_podstawowe --date-from DATE_FROM --date-to DATE_TO
```
or

```
python main.py --data-source generacja_mocy_jednostek_wytworczych --date-from DATE_FROM --date-to DATE_TO
```
Note: The above command assumes that the current directory is the root directory of the project.

This project can be used to extract data, transform it into a format that can be used for analysis (.csv), and load it into a destination file. The example command above extracts data from "kontrakty_godzinowe", "wielkosci_podstawowe" and "generacja_mocy_jednostek_wytworczych" data sources for the date range of DATE_FROM to DATE_TO. Please replace DATE_FROM and DATE_TO with the desired date in format YYYYMMDD

The date-to parameter is optional, if not provided the data for only DATE_FROM will be extracted.

The data source "kontrakty_godzinowe" uses the package BeautifulSoup4 (bs4) to scrape data from a website, while the "wielkosci_podstawowe" and "generacja_mocy_jednostek_wytworczych" data sources use direct links to download the data.
