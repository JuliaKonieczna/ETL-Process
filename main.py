import pandas as pd
import click
import requests
import re
from bs4 import BeautifulSoup
from typing import Optional, List
from datetime import datetime
from abc import ABC, abstractmethod
from enum import Enum


class DataSources(Enum):
    GEN_MOC_JW = "generacja_mocy_jednostek_wytworczych"
    WYK_KSE = "wielkosci_podstawowe"
    KONTRAKTY_GODZINOWE = "kontrakty_godzinowe"

    def get_url(self):
        if self == DataSources.GEN_MOC_JW:
            return "https://www.pse.pl/getcsv/-/export/csv/PL_GEN_MOC_JW_EPS/"
        elif self == DataSources.WYK_KSE:
            return "https://www.pse.pl/getcsv/-/export/csv/PL_WYK_KSE/"
        elif self == DataSources.KONTRAKTY_GODZINOWE:
            return "https://tge.pl/energia-elektryczna-rdn?dateShow="
        else:
            raise ValueError(f"Unknown data source: {self}")


class Param(Enum):
    DATE_FROM = "$date_from"
    DATE_TO = "$date_to"
    DATE = "$date"


class Company(ABC):
    def __init__(self,
                 base_url: str,
                 data_source: DataSources,  # TODO: check if correct type
                 date_from: datetime,
                 date_to: Optional[datetime] = None,
                 ) -> None:
        self.base_url = base_url
        self.data_source = data_source
        self.date_from = date_from
        self.date_to = date_to
        self.report_file_name = self.get_report_file_name()

    @abstractmethod
    def get_report_file_name(self) -> str:
        if self.date_from == self.date_to:
            report_file_name = f"{self.data_source}_{self.date_from.strftime('%Y%m%d')}.csv"
        else:
            report_file_name = f"{self.data_source}_{self.date_from.strftime('%Y%m%d')}-{self.date_to.strftime('%Y%m%d')}.csv"
        return report_file_name

    @abstractmethod
    def get_endpoint(self, **kwargs) -> None:
        pass

    @abstractmethod
    def get_data(self, **kwargs) -> None:
        pass


class PSE(Company):
    endpoint_when_date_range = f"data_od/{Param.DATE_FROM.value}/data_do/{Param.DATE_TO.value}"
    endpoint_when_single_date = f"data/{Param.DATE.value}/unit/all"

    def __init__(self,
                 base_url: str,
                 data_source: DataSources,  # TODO: check if correct type
                 date_from: datetime,
                 date_to: Optional[datetime] = None,
                 ) -> None:
        super().__init__(base_url, data_source, date_from, date_to)

    def get_report_file_name(self) -> str:
        report_file_name = super().get_report_file_name()
        return report_file_name

    def get_endpoint(self) -> str:
        if self.date_to is not None:
            endpoint = self.base_url + PSE.endpoint_when_date_range
            endpoint = endpoint.replace(Param.DATE_FROM.value, self.date_from.strftime("%Y%m%d"))
            endpoint = endpoint.replace(Param.DATE_TO.value, self.date_to.strftime("%Y%m%d"))
        else:
            endpoint = self.base_url + PSE.endpoint_when_single_date
            endpoint = endpoint.replace(Param.DATE.value, self.date_from.strftime("%Y%m%d"))
        return endpoint

    def get_data(
            self
    ) -> None:
        endpoint = self.get_endpoint()
        click.echo("Downloading file...")
        response = requests.get(endpoint)
        with open(self.report_file_name, "wb") as f:
            f.write(response.content)
        click.echo("File downloaded")


class TGE(Company):
    def __init__(self,
                 base_url: str,
                 data_source: DataSources,  # TODO: check if correct type
                 date_from: datetime,
                 date_to: Optional[datetime] = None,
                 ) -> None:
        super().__init__(base_url, data_source, date_from, date_to)

    def get_endpoint(self, date) -> str:
        return f"{self.base_url}{date.strftime('%Y%m%d')}"

    def get_report_file_name(
            self,
    ) -> str:
        report_file_name = super().get_report_file_name()
        return report_file_name

    def get_data(
            self,
    ) -> List[List[str]]:
        click.echo("Scraping data...")
        for date in pd.date_range(self.date_from, self.date_to):
            endpoint = self.get_endpoint(date)
            response = requests.get(endpoint)
            html_content = response.text

            soup = BeautifulSoup(html_content, features="html.parser")

            table = soup.find_all("div", class_="table-responsive wyniki-table-kontrakty-godzinowe")
            for columns in table:  # TODO these are not really columns, these are 2 columns and all headers
                elements = columns.find_all("tr")
                data = [(re.split("[\n\t]+", element.text.strip())) for element in elements]
        click.echo("Data scraped")
        return data


def data_cleaning(
    report_file_name: str
):
    with open(report_file_name, "r") as f:
        pass


def save_to_csv(

):
    pass


def map_data_source_to_company(data_source: str) -> Company:
    if data_source == DataSources.WYK_KSE.value:
        return PSE(base_url=DataSources.WYK_KSE.get_url())
    elif data_source == DataSources.GEN_MOC_JW.value:
        return TGE(base_url=DataSources.GEN_MOC_JW.get_url())
    elif data_source == DataSources.KONTRAKTY_GODZINOWE.value:
        return PSE(base_url=DataSources.KONTRAKTY_GODZINOWE.get_url())


def etl_process(  # TODO: change name
    data_source: str,
    date_from: datetime,
    date_to: Optional[datetime],
    output_file: str
) -> None:
    company = map_data_source_to_company(data_source=data_source)
    company.get_data()


@click.command()
@click.option('--data-source', type=click.Choice([data_source.value for data_source in DataSources]), required=True)  # TODO: add help
@click.option('--date-from', type=click.DateTime(formats=["%Y%m%d"]), required=True, help="Date format: YYYYMMDD")  # TODO: maybe add default as today's date
@click.option('--date-to', type=click.DateTime(formats=["%Y%m%d"]), required=False, help="Date format: YYYYMMDD")  # TODO: add speification, if not provided, report will be downloaded only from one day
@click.option('--output-file', type=str, default="output.csv")  # TODO: add help
def get_cli_params(
    data_source: str,
    date_from: datetime,
    date_to: Optional[datetime],
    output_file: str
) -> None:
    assert date_from < datetime.today(), "Date from must be less than today's date"
    if date_to is not None:
        assert date_from <= date_to, "Date from must be less than date to"
        assert date_to < datetime.today(), "Date must be less than today's date"  # TODO: ugly, try to merge with same for date_from
    if date_from == date_to:
        date_to = None
    etl_process(data_source=data_source, date_from=date_from, date_to=date_to, output_file=output_file)


if __name__ == '__main__':
    get_cli_params()
