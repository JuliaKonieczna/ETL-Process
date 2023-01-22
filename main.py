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
    def __init__(self, base_url: str):
        self.base_url = base_url

    @abstractmethod
    def get_endpoint(self, date_from: datetime, date_to: Optional[datetime] = None) -> None:
        pass

    @abstractmethod
    def get_report_file_name(self, data_source: str, date_from: datetime, date_to: Optional[datetime]) -> str:
        if date_from == date_to:
            report_file_name =  f"{data_source}_{date_from.strftime('%Y%m%d')}.csv"
        else:
            report_file_name = f"{data_source}_{date_from.strftime('%Y%m%d')}-{date_to.strftime('%Y%m%d')}.csv"
        return report_file_name


class PSE(Company):
    endpoint_when_date_range = f"data_od/{Param.DATE_FROM.value}/data_do/{Param.DATE_TO.value}"
    endpoint_when_single_date = f"data/{Param.DATE.value}/unit/all"

    def __init__(self, base_url: str):
        super().__init__(base_url)

    def get_endpoint(
            self,
            date_from: datetime,
            date_to: Optional[datetime] = None,
    ) -> str:
        if date_to is not None:
            endpoint = self.base_url + PSE.endpoint_when_date_range
            endpoint = endpoint.replace(Param.DATE_FROM.value, date_from.strftime("%Y%m%d"))
            endpoint = endpoint.replace(Param.DATE_TO.value, date_to.strftime("%Y%m%d"))
        else:
            endpoint = self.base_url + PSE.endpoint_when_single_date
            endpoint = endpoint.replace(Param.DATE.value, date_from.strftime("%Y%m%d"))
        return endpoint

    def get_report_file_name(
            self,
            data_source: str,
            date_from: datetime,
            date_to: Optional[datetime]
    ) -> str:
        report_file_name = super().get_report_file_name(data_source, date_from, date_to)
        return report_file_name


class TGE(Company):
    def __init__(self, base_url: str):
        super().__init__(base_url)
        
    def get_endpoint(
            self,
            date_from: datetime,
            date_to: Optional[datetime] = None
    ) -> List[str]:
        return [f"{self.base_url}{date.strftime('%Y%m%d')}" for date in pd.date_range(date_from, date_to)]  # TODO: check if it works when date_to is none

    def get_report_file_name(
            self,
            data_source: str,
            date_from: datetime,
            date_to: Optional[datetime]
    ) -> str:
        report_file_name = super().get_report_file_name(data_source, date_from, date_to)
        return report_file_name


def download_csv_file(
    report_file_name: str,
    endpoint: str,
    date_from: datetime,
    date_to: Optional[datetime]
) -> str:
    click.echo("Downloading file...")
    response = requests.get(endpoint)
    with open(report_file_name, "wb") as f:
        f.write(response.content)
    click.echo("File downloaded")
    return report_file_name


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
    report_file_name = company.get_report_file_name(data_source=data_source, date_from=date_from, date_to=date_to)
    endpoint = company.get_endpoint(date_from=date_from, date_to=date_to)

    # TODO implement if underneath to classes
    if isinstance(company, PSE):
        download_csv_file(report_file_name=report_file_name, endpoint=endpoint, date_from=date_from, date_to=date_to)
    elif isinstance(company, TGE):
        scrape_data(report_file_name=report_file_name, endpoints=endpoint, date_from=date_from, date_to=date_to)
    # data_cleaning(report_file_name=report_file_name)


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
