import logging
import click
from typing import Optional
from datetime import datetime
from mapper.data_source_mapper import map_data_source_to_company
from enums.data_source import DataSource
from constants.constants import DATE_FORMAT

logging.basicConfig(level=logging.INFO, filename='app.log', filemode='w')
logging.getLogger().addHandler(logging.StreamHandler())


def run_etl_process(
    data_source: str, date_from: datetime, date_to: Optional[datetime]
) -> None:
    company = map_data_source_to_company(
        data_source=data_source, date_from=date_from, date_to=date_to
    )
    company.manage_process()


@click.command()
@click.option(
    "--data-source",
    type=click.Choice([data_source.value for data_source in DataSource]),
    required=True,
    help="Data source to download",
)
@click.option(
    "--date-from",
    type=click.DateTime(formats=[DATE_FORMAT]),
    required=True,
    default=datetime.today,
    help="Date format: YYYYMMDD, default is today's date",
)
@click.option(
    "--date-to",
    type=click.DateTime(formats=[DATE_FORMAT]),
    required=False,
    help="Date format: YYYYMMDD, if not provided report will be downloaded only from one day",
)
def get_cli_params(
    data_source: str, date_from: datetime, date_to: Optional[datetime]
) -> None:
    assert date_from < datetime.today(), "Date from must be less than today's date"
    if date_to is not None:
        assert date_from <= date_to, "Date from must be less than date to"
        assert date_to < datetime.today(), "Dates must be less than today's date"
        if data_source == DataSource.HOUR_CONTRACTS.value:
            assert (datetime.today() - date_from).days <= 90, "Maximum date range is 3 months in the past"
    if date_from == date_to:
        date_to = None
    run_etl_process(data_source=data_source, date_from=date_from, date_to=date_to)


if __name__ == "__main__":
    get_cli_params()
