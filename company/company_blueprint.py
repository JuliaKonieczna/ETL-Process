import logging
import os
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional
from constants.constants import OUTPUT_FILE_LOCATION, DATE_FORMAT


class Company(ABC):
    def __init__(
        self,
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
            report_file_name = (
                f"{self.data_source}_{self.date_from.strftime(DATE_FORMAT)}.csv"
            )
        else:
            report_file_name = \
                f"{self.data_source}_{self.date_from.strftime(DATE_FORMAT)}-{self.date_to.strftime(DATE_FORMAT)}.csv"
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

    def save_data(self, data) -> None:
        logging.info("Saving data...")
        if os.path.exists(OUTPUT_FILE_LOCATION):
            data.to_csv(OUTPUT_FILE_LOCATION + self.report_file_name, sep=";", index=False)
        else:
            os.mkdir(OUTPUT_FILE_LOCATION)
            data.to_csv(OUTPUT_FILE_LOCATION + self.report_file_name, sep=";", index=False)
        logging.info(f"Data saved to {OUTPUT_FILE_LOCATION + self.report_file_name}")
