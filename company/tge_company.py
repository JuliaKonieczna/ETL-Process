import pandas as pd
import re
import requests
import logging
from bs4 import BeautifulSoup
from datetime import datetime
from typing import List, Optional
from unidecode import unidecode
from company.company_blueprint import Company
from constants.constants import DATE_FORMAT


class TGE(Company):
    def __init__(
        self,
        base_url: str,
        data_source: str,
        date_from: datetime,
        date_to: Optional[datetime] = None,
    ) -> None:
        super().__init__(base_url, data_source, date_from, date_to)

    def get_report_file_name(
        self,
    ) -> str:
        report_file_name = super().get_report_file_name()
        return report_file_name

    def manage_process(self) -> None:
        data = self.get_data()
        data = self.clean_data(data)
        self.save_data(data)

    def get_endpoint(self, date) -> str:
        return f"{self.base_url}{date.strftime(DATE_FORMAT)}"

    def get_data(
        self,
    ) -> List[List[str]]:
        logging.info("Scraping data...")
        data = []
        for date in pd.date_range(self.date_from, self.date_to):
            endpoint = self.get_endpoint(date)
            response = requests.get(endpoint)
            html_content = response.text

            soup = BeautifulSoup(html_content, features="html.parser")

            table = soup.find_all(
                "div", class_="table-responsive wyniki-table-kontrakty-godzinowe"
            )
            assert len(table) >= 1
            for (
                columns
            ) in (
                table
            ):  # these are not really columns, these are 2 columns and all headers
                elements = columns.find_all("tr")
                scrapped_data = [
                    (re.split("[\n\t]+", element.text.strip())) for element in elements
                ]
                if not data:
                    data += scrapped_data
                    data[1].insert(0, "Data")
                    for row in data[2:]:
                        row.insert(0, date)
                else:
                    new_data = scrapped_data[2:]
                    for row in new_data:
                        row.insert(0, date)
                    data += new_data

        logging.info("Data scraped")
        return data

    def clean_data(self, data: List[List[str]]) -> pd.DataFrame:
        logging.info("Cleaning data...")
        start_index = 2  # it's needed to ignore first two columns because they are independent
        labels = data[0]

        for i, e in enumerate(data[1][start_index:], start=start_index):
            data[1][i] = labels[(i - start_index) // 2] + " " + e

        data = data[1:]
        df = pd.DataFrame(data[1:], columns=data[0])
        df.columns = df.columns.str.replace(" ", "_")
        df.rename(columns=unidecode, inplace=True)
        df.rename(columns={"Czas": "Godzina"}, inplace=True)
        df["Godzina"] = df["Godzina"].str.split("-").str[0]
        df.loc[df["Godzina"] == "Suma", "FIXING_I_Kurs_(PLN/MWh)"] = ""
        df.loc[df["Godzina"] == "Suma", "FIXING_II_Kurs_(PLN/MWh)"] = ""
        df.loc[df["Godzina"] == "Suma", "Notowania_ciagle_Kurs_(PLN/MWh)"] = ""
        logging.info("Data cleaned")
        return df

    def save_data(self, data: pd.DataFrame) -> None:
        super().save_data(data)
