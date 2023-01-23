from typing import Optional
from datetime import datetime
from company.company_blueprint import Company
from company.pse_company import PSE
from company.tge_company import TGE
from enums.data_source import DataSource


def map_data_source_to_company(
    data_source: str, date_from: datetime, date_to: Optional[datetime]
) -> Company:
    if data_source == DataSource.BASIC_VOLUMES.value:
        return PSE(
            base_url=DataSource.BASIC_VOLUMES.get_url(),
            data_source=data_source,
            date_from=date_from,
            date_to=date_to,
        )
    elif data_source == DataSource.POWER_GENERATION.value:
        return PSE(
            base_url=DataSource.POWER_GENERATION.get_url(),
            data_source=data_source,
            date_from=date_from,
            date_to=date_to,
        )
    elif data_source == DataSource.HOUR_CONTRACTS.value:
        return TGE(
            base_url=DataSource.HOUR_CONTRACTS.get_url(),
            data_source=data_source,
            date_from=date_from,
            date_to=date_to,
        )
    else:
        raise ValueError(f"Unknown data source: {data_source}")
