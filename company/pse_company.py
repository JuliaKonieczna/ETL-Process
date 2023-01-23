import requests
import logging
import pandas as pd
from datetime import datetime
from typing import Optional
from unidecode import unidecode
from enums.param import Param
from company.company_blueprint import Company
from constants.constants import DATE_FORMAT


class PSE(Company):
    endpoint_when_date_range = (
        f"data_od/{Param.DATE_FROM.value}/data_do/{Param.DATE_TO.value}"
    )
    endpoint_when_single_date = f"data/{Param.DATE.value}/unit/all"

    def __init__(
        self,
        base_url: str,
        data_source: str,
        date_from: datetime,
        date_to: Optional[datetime] = None,
    ) -> None:
        super().__init__(base_url, data_source, date_from, date_to)

    def get_report_file_name(self) -> str:
        report_file_name = super().get_report_file_name()
        return report_file_name

    def manage_process(self) -> None:
        self.get_data()
        data = self.clean_data()
        self.save_data(data=data)

    def get_endpoint(self) -> str:
        if self.date_to is not None:
            endpoint = self.base_url + PSE.endpoint_when_date_range
            endpoint = endpoint.replace(
                Param.DATE_FROM.value, self.date_from.strftime(DATE_FORMAT)
            )
            endpoint = endpoint.replace(
                Param.DATE_TO.value, self.date_to.strftime(DATE_FORMAT)
            )
        else:
            endpoint = self.base_url + PSE.endpoint_when_single_date
            endpoint = endpoint.replace(
                Param.DATE.value, self.date_from.strftime(DATE_FORMAT)
            )
        return endpoint

    def get_data(self) -> None:
        endpoint = self.get_endpoint()
        logging.info("Downloading file...")
        response = requests.get(endpoint)
        with open(self.report_file_name, "wb") as f:
            f.write(response.content)
        logging.info("File downloaded")

    def clean_data(self) -> pd.DataFrame:
        logging.info("Cleaning data...")
        df = pd.read_csv(self.report_file_name, encoding="Windows-1250", sep=";")
        df.columns = df.columns.str.replace(" ", "_")
        df.rename(columns=unidecode, inplace=True)
        logging.info("Data cleaned")
        return df

    def save_data(self, data: pd.DataFrame) -> None:
        super().save_data(data)
