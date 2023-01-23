from enum import Enum


class DataSource(Enum):
    POWER_GENERATION = "generacja_mocy_jednostek_wytworczych"
    BASIC_VOLUMES = "wielkosci_podstawowe"
    HOUR_CONTRACTS = "kontrakty_godzinowe"

    def get_url(self):
        if self == DataSource.POWER_GENERATION:
            return "https://www.pse.pl/getcsv/-/export/csv/PL_GEN_MOC_JW_EPS/"
        elif self == DataSource.BASIC_VOLUMES:
            return "https://www.pse.pl/getcsv/-/export/csv/PL_WYK_KSE/"
        elif self == DataSource.HOUR_CONTRACTS:
            return "https://tge.pl/energia-elektryczna-rdn?dateShow="
        else:
            raise ValueError(f"Unknown data source: {self}")
