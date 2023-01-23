import pandas as pd
import click
import requests
import re
from bs4 import BeautifulSoup
from typing import Optional, List
from datetime import datetime
from abc import ABC, abstractmethod
from enum import Enum
from unidecode import unidecode


class DataSources(Enum):
    GEN_MOC_JW = "generacja_mocy_jednostek_wytworczych"
    WYK_KSE = "wielkosci_podstawowe"
    KONTRAKTY_GODZINOWE = "kontrakty_godzinowe"  # TODO maybe change name

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
                 data_source: str,
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
    def manage_process(self) -> None:
        pass

    @abstractmethod
    def get_endpoint(self, **kwargs) -> None:
        pass

    @abstractmethod
    def get_data(self, **kwargs) -> None:
        pass

    @abstractmethod
    def clean_data(self, **kwargs) -> None:
        pass

    @abstractmethod
    def save_data(self, **kwargs) -> None:
        pass


class PSE(Company):
    endpoint_when_date_range = f"data_od/{Param.DATE_FROM.value}/data_do/{Param.DATE_TO.value}"
    endpoint_when_single_date = f"data/{Param.DATE.value}/unit/all"

    def __init__(self,
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
        self..save_data(data=data)

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

    def clean_data(self) -> pd.DataFrame:
        click.echo("Cleaning data...")
        df = pd.read_csv(self.report_file_name, encoding="Windows-1250", sep=";")
        df.columns = df.columns.str.replace(" ", "_")
        df.rename(columns=unidecode, inplace=True)
        click.echo("Data cleaned")
        return df

    def save_data(self, data: pd.DataFrame) -> None:
        click.echo("Saving data...")
        data.to_csv(self.report_file_name, sep=";", index=False)
        click.echo(f"Data saved to {self.report_file_name}")


class TGE(Company):
    def __init__(self,
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
        return f"{self.base_url}{date.strftime('%Y%m%d')}"

    def get_data(
            self,
    ) -> List[List[str]]:
        click.echo("Scraping data...")
        data = []
        for date in pd.date_range(self.date_from, self.date_to):
            endpoint = self.get_endpoint(date)
            response = requests.get(endpoint)
            html_content = response.text

            soup = BeautifulSoup(html_content, features="html.parser")

            table = soup.find_all("div", class_="table-responsive wyniki-table-kontrakty-godzinowe")
            assert len(table) >= 1
            for columns in table:  # TODO these are not really columns, these are 2 columns and all headers
                elements = columns.find_all("tr")
                scrapped_data = [(re.split("[\n\t]+", element.text.strip())) for element in elements]
                if not data:
                    data += scrapped_data
                    data[1].insert(0, "Data")
                    for row in data[2:]:
                        row.insert(0, endpoint.split("=")[1])
                else:
                    new_data = scrapped_data[2:]
                    for row in new_data:
                        row.insert(0, endpoint.split("=")[1])
                    data += new_data

        click.echo("Data scraped")
        return data

    def clean_data(self, data: List[List[str]]) -> pd.DataFrame:
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
        return df


    def save_data(self, data: pd.DataFrame) -> None:
        data.to_csv(self.report_file_name, sep=";", index=False)


def map_data_source_to_company(
        data_source: str,
        date_from: datetime,
        date_to: Optional[datetime]
        ) -> Company:
    if data_source == DataSources.WYK_KSE.value:
        return PSE(base_url=DataSources.WYK_KSE.get_url(), data_source=data_source, date_from=date_from, date_to=date_to)
    elif data_source == DataSources.GEN_MOC_JW.value:
        return TGE(base_url=DataSources.GEN_MOC_JW.get_url(), data_source=data_source, date_from=date_from, date_to=date_to)
    elif data_source == DataSources.KONTRAKTY_GODZINOWE.value:
        return PSE(base_url=DataSources.KONTRAKTY_GODZINOWE.get_url(), data_source=data_source, date_from=date_from, date_to=date_to)


def run_etl_process(
    data_source: str,
    date_from: datetime,
    date_to: Optional[datetime]
) -> None:
    company = map_data_source_to_company(data_source=data_source, date_from=date_from, date_to=date_to)
    company.manage_process()


@click.command()
@click.option('--data-source', type=click.Choice([data_source.value for data_source in DataSources]), required=True,
              help="Data source to download")
@click.option('--date-from', type=click.DateTime(formats=["%Y%m%d"]), required=True, default=datetime.today,
              help="Date format: YYYYMMDD, default is today's date")
@click.option('--date-to', type=click.DateTime(formats=["%Y%m%d"]), required=False
    , help="Date format: YYYYMMDD, if not provided report will be downloaded only from one day")
def get_cli_params(
    data_source: str,
    date_from: datetime,
    date_to: Optional[datetime]
) -> None:
    assert date_from < datetime.today(), "Date from must be less than today's date"
    if date_to is not None:
        assert date_from <= date_to, "Date from must be less than date to"
        assert date_to < datetime.today(), "Dates must be less than today's date"
        if data_source == DataSources.KONTRAKTY_GODZINOWE.value:
            assert (date_to - date_from).days <= 90, "Maximum date range is 3 months"
    if date_from == date_to:
        date_to = None
    run_etl_process(data_source=data_source, date_from=date_from, date_to=date_to)


if __name__ == '__main__':
    get_cli_params()
