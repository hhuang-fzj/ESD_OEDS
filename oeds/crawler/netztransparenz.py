# SPDX-FileCopyrightText: Marvin Lorber
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""
Crawler for data from https://www.netztransparenz.de.
The resulting data is not available under an open-source license and should not be reshared but is available for crawling yourself.

Using this crawler requires setting up an Account and client in the
Netztransparenz extranet (see: https://www.netztransparenz.de/de-de/Web-API).

The client expects that the environment contains the variables 'IPNT_CLIENT_ID' and 'IPNT_CLIENT_SECRET'
with the credentials from the previous step.
"""

import datetime as dt
import io
import logging
import sys
from datetime import datetime, timedelta

import pandas as pd
import requests
import sqlalchemy as sql

from oeds.base_crawler import ContinuousCrawler, load_config

log = logging.getLogger("netztransparenz")
log.setLevel(logging.INFO)
api_date_format = "%Y-%m-%dT%H:%M:%S"
csv_date_format = "%Y-%m-%d %H:%M %Z"

metadata_info = {
    "schema_name": "netztransparenz",
    "data_date": "2015-02-10",
    "data_source": "https://ds.netztransparenz.de/api/",
    "license": "https://www.netztransparenz.de/en/About-us/Information-platforms/Disclosure-obligations-in-accordance-with-the-EU-Transparency-Regulation",
    "description": "German Energy Network Operations. Activated minimum and secondary reserve levels, forecasted solar and wind energy outputs, net reserve power balance, and redispatch measures.",
    "contact": "",
    "temporal_start": "2011-03-31 00:00:00",
    "temporal_end": "2024-05-22 16:00:00",
    "concave_hull_geometry": None,
}


def database_friendly(string):
    return string.lower().replace("(", "").replace(")", "").replace(" ", "_")


TEMPORAL_START = datetime(2011, 3, 31)


class NetztransparenzCrawler(ContinuousCrawler):
    TIMEDELTA = timedelta(days=2)

    def get_latest_data(self) -> datetime:
        return TEMPORAL_START

    def get_first_data(self) -> datetime:
        return TEMPORAL_START

    def initialize_token(self):
        # add your Client-ID and Client-secret from the API Client configuration GUI to
        # your environment variable first

        IPNT_CLIENT_ID = self.config.get("ipnt_client_id")
        IPNT_CLIENT_SECRET = self.config.get("ipnt_client_secret")
        ACCESS_TOKEN_URL = "https://identity.netztransparenz.de/users/connect/token"

        # Ask for the token providing above authorization data
        response = requests.post(
            ACCESS_TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": IPNT_CLIENT_ID,
                "client_secret": IPNT_CLIENT_SECRET,
            },
        )

        # Parse the token from the response if the response was OK
        if response.ok:
            self.token = response.json()["access_token"]
        else:
            message = (
                f"Error retrieving token\n{response.status_code}:{response.reason}"
            )
            log.error(message)
            raise Exception(f"Login failed. {message}")

    def check_health(self):
        url = "https://ds.netztransparenz.de/api/v1/health"
        response = requests.get(url, headers={"Authorization": f"Bearer {self.token}"})
        print(response.text, file=sys.stdout)

    def forecast_solar(self):
        # Prognose Solar contains historical data, relevant data is only found in the timeframe below
        start_of_data = "2011-03-31T22:00:00"
        end_of_data = "2022-12-14T23:00:00"
        url = f"https://ds.netztransparenz.de/api/v1/data/prognose/Solar/{start_of_data}/{end_of_data}"
        response = requests.get(url, headers={"Authorization": f"Bearer {self.token}"})
        df = pd.read_csv(
            io.StringIO(response.text),
            sep=";",
            header=0,
            decimal=",",
            thousands=".",
            na_values=["N.A."],
        )
        df.rename(mapper=lambda x: database_friendly(x), axis="columns", inplace=True)
        df["von"] = pd.to_datetime(
            df["datum"] + " " + df["von"] + " " + df["zeitzone_von"],
            format=csv_date_format,
            utc=True,
        ).dt.tz_localize(None)
        df["bis"] = pd.to_datetime(
            df["datum"] + " " + df["bis"] + " " + df["zeitzone_bis"],
            format=csv_date_format,
            utc=True,
        ).dt.tz_localize(None)
        df = df.drop(["datum", "zeitzone_von", "zeitzone_bis"], axis=1).set_index("von")
        with self.engine.begin() as conn:
            df.to_sql("prognose_solar", conn, if_exists="replace")

    def forecast_wind(self):
        # Prognose Solar contains historical data, relevant data is only found in the timeframe below
        start_of_data = "2011-03-31T22:00:00"
        end_of_data = "2022-12-14T23:00:00"
        url = f"https://ds.netztransparenz.de/api/v1/data/prognose/Wind/{start_of_data}/{end_of_data}"
        response = requests.get(url, headers={"Authorization": f"Bearer {self.token}"})
        df = pd.read_csv(
            io.StringIO(response.text),
            sep=";",
            header=0,
            decimal=",",
            thousands=".",
            na_values=["N.A."],
        )
        df.rename(mapper=lambda x: database_friendly(x), axis="columns", inplace=True)
        df["von"] = pd.to_datetime(
            df["datum"] + " " + df["von"] + " " + df["zeitzone_von"],
            format=csv_date_format,
            utc=True,
        ).dt.tz_localize(None)
        df["bis"] = pd.to_datetime(
            df["datum"] + " " + df["bis"] + " " + df["zeitzone_bis"],
            format=csv_date_format,
            utc=True,
        ).dt.tz_localize(None)
        df = df.drop(["datum", "zeitzone_von", "zeitzone_bis"], axis=1).set_index("von")
        with self.engine.begin() as conn:
            df.to_sql("prognose_wind", conn, if_exists="replace")

    def extrapolation_solar(self):
        tablename = "hochrechnung_solar"
        start_of_data = "2011-03-31T22:00:00"
        start_of_data = self.find_latest(tablename, "bis", start_of_data)
        end_of_data = dt.date.today() - dt.timedelta(days=1)
        end_of_data = dt.datetime.combine(end_of_data, dt.datetime.min.time()).strftime(
            api_date_format
        )
        if start_of_data < end_of_data:
            url = f"https://ds.netztransparenz.de/api/v1/data/hochrechnung/Solar/{start_of_data}/{end_of_data}"
            response = requests.get(
                url, headers={"Authorization": f"Bearer {self.token}"}
            )
            df = pd.read_csv(
                io.StringIO(response.text),
                sep=";",
                header=0,
                decimal=",",
                thousands=".",
                na_values=["N.A."],
            )
            df.rename(
                mapper=lambda x: database_friendly(x),
                axis="columns",
                inplace=True,
            )
            df["von"] = pd.to_datetime(
                df["datum"] + " " + df["von"] + " " + df["zeitzone_von"],
                format=csv_date_format,
                utc=True,
            ).dt.tz_localize(None)
            df["bis"] = pd.to_datetime(
                df["datum"] + " " + df["bis"] + " " + df["zeitzone_bis"],
                format=csv_date_format,
                utc=True,
            ).dt.tz_localize(None)
            df = df.drop(["datum", "zeitzone_von", "zeitzone_bis"], axis=1).set_index(
                "von"
            )
            with self.engine.begin() as conn:
                df.to_sql(tablename, conn, if_exists="append")

    def extrapolation_wind(self):
        tablename = "hochrechnung_wind"
        start_of_data = "2011-03-31T22:00:00"
        start_of_data = self.find_latest(tablename, "bis", start_of_data)
        end_of_data = dt.date.today() - dt.timedelta(days=1)
        end_of_data = dt.datetime.combine(end_of_data, dt.datetime.min.time()).strftime(
            api_date_format
        )
        if start_of_data < end_of_data:
            url = f"https://ds.netztransparenz.de/api/v1/data/hochrechnung/Wind/{start_of_data}/{end_of_data}"
            response = requests.get(
                url, headers={"Authorization": f"Bearer {self.token}"}
            )
            df = pd.read_csv(
                io.StringIO(response.text),
                sep=";",
                header=0,
                decimal=",",
                thousands=".",
                na_values=["N.A."],
            )
            df.rename(
                mapper=lambda x: database_friendly(x),
                axis="columns",
                inplace=True,
            )
            df["von"] = pd.to_datetime(
                df["datum"] + " " + df["von"] + " " + df["zeitzone_von"],
                format=csv_date_format,
                utc=True,
            ).dt.tz_localize(None)
            df["bis"] = pd.to_datetime(
                df["datum"] + " " + df["bis"] + " " + df["zeitzone_bis"],
                format=csv_date_format,
                utc=True,
            ).dt.tz_localize(None)
            df = df.drop(["datum", "zeitzone_von", "zeitzone_bis"], axis=1).set_index(
                "von"
            )
            with self.engine.begin() as conn:
                df.to_sql(tablename, conn, if_exists="append")

    def utilization_balancing_energy(self):
        tablename = "vermarktung_inanspruchnahme_ausgleichsenergie"
        start_of_data = "2011-03-31T22:00:00"
        start_of_data = self.find_latest(tablename, "bis", start_of_data)
        end_of_data = dt.date.today() - dt.timedelta(days=1)
        end_of_data = dt.datetime.combine(end_of_data, dt.datetime.min.time()).strftime(
            api_date_format
        )
        if start_of_data < end_of_data:
            url = f"https://ds.netztransparenz.de/api/v1/data/vermarktung/InanspruchnahmeAusgleichsenergie/{start_of_data}/{end_of_data}"
            response = requests.get(
                url, headers={"Authorization": f"Bearer {self.token}"}
            )
            df = pd.read_csv(
                io.StringIO(response.text),
                sep=";",
                header=0,
                decimal=",",
                thousands=".",
                na_values=["N.A."],
            )
            df.rename(
                mapper=lambda x: database_friendly(x),
                axis="columns",
                inplace=True,
            )
            df["von"] = pd.to_datetime(
                df["datum"] + " " + df["von"] + " " + df["zeitzone_von"],
                format=csv_date_format,
                utc=True,
            ).dt.tz_localize(None)
            df["bis"] = pd.to_datetime(
                df["datum"] + " " + df["bis"] + " " + df["zeitzone_bis"],
                format=csv_date_format,
                utc=True,
            ).dt.tz_localize(None)
            df = df.drop(["datum", "zeitzone_von", "zeitzone_bis"], axis=1).set_index(
                "von"
            )
            with self.engine.begin() as conn:
                df.to_sql(tablename, conn, if_exists="append")

    def redispatch(self):
        tablename = "redispatch"
        start_of_data = "2013-01-01T00:00:00"
        start_of_data = self.find_latest(tablename, "bis", start_of_data)
        end_of_data = dt.date.today() - dt.timedelta(days=1)
        end_of_data = dt.datetime.combine(end_of_data, dt.datetime.min.time()).strftime(
            api_date_format
        )
        if start_of_data < end_of_data:
            url = f"https://ds.netztransparenz.de/api/v1/data/redispatch/{start_of_data}/{end_of_data}"
            response = requests.get(
                url, headers={"Authorization": f"Bearer {self.token}"}
            )
            df = pd.read_csv(
                io.StringIO(response.text),
                sep=";",
                header=0,
                decimal=",",
                na_values=["N.A."],
            )
            df.rename(mapper=str.lower, axis="columns", inplace=True)
            df["beginn"] = pd.to_datetime(
                df["beginn_datum"] + " " + df["beginn_uhrzeit"],
                format="%d.%m.%Y %H:%M",
                utc=True,
            ).dt.tz_localize(None)
            df["ende"] = pd.to_datetime(
                df["ende_datum"] + " " + df["ende_uhrzeit"],
                format="%d.%m.%Y %H:%M",
                utc=True,
            ).dt.tz_localize(None)
            df = df.drop(
                [
                    "beginn_datum",
                    "beginn_uhrzeit",
                    "ende_datum",
                    "ende_uhrzeit",
                    "zeitzone_von",
                    "zeitzone_bis",
                ],
                axis=1,
            ).set_index("beginn")
            with self.engine.begin() as conn:
                df.to_sql(tablename, conn, if_exists="append")

    def gcc_balance(self):
        tablename = "nrv_saldo"
        start_of_data = "2014-01-01T00:00:00"
        start_of_data = self.find_latest(tablename, "bis", start_of_data)
        # Data might be subject to change for 20 Work days, so we wait 30 calendar days to crawl
        end_of_data = dt.date.today() - dt.timedelta(days=30)
        end_of_data = dt.datetime.combine(end_of_data, dt.datetime.min.time()).strftime(
            api_date_format
        )
        if start_of_data < end_of_data:
            url = f"https://ds.netztransparenz.de/api/v1/data/nrvsaldo/NRVSaldo/Qualitaetsgesichert/{start_of_data}/{end_of_data}"
            response = requests.get(
                url, headers={"Authorization": f"Bearer {self.token}"}
            )
            df = pd.read_csv(
                io.StringIO(response.text),
                sep=";",
                header=0,
                decimal=",",
                na_values=["N.A."],
            )
            df.rename(mapper=str.lower, axis="columns", inplace=True)
            df["von"] = pd.to_datetime(
                df["datum"] + " " + df["von"] + " " + df["zeitzone"],
                format="%d.%m.%Y %H:%M %Z",
                utc=True,
            ).dt.tz_localize(None)
            df["bis"] = pd.to_datetime(
                df["datum"] + " " + df["bis"] + " " + df["zeitzone"],
                format="%d.%m.%Y %H:%M %Z",
                utc=True,
            ).dt.tz_localize(None)
            df = df.drop(["datum", "zeitzone"], axis=1).set_index("von")
            with self.engine.begin() as conn:
                df.to_sql(tablename, conn, if_exists="append")

    def lfc_area_balance(self):
        tablename = "rz_saldo"
        start_of_data = "2014-01-01T00:00:00"
        start_of_data = self.find_latest(tablename, "bis", start_of_data)
        # Data might be subject to change for 20 Work days, so we wait 30 calendar days to crawl
        end_of_data = dt.date.today() - dt.timedelta(days=30)
        end_of_data = dt.datetime.combine(end_of_data, dt.datetime.min.time()).strftime(
            api_date_format
        )
        if start_of_data < end_of_data:
            url = f"https://ds.netztransparenz.de/api/v1/data/nrvsaldo/RZSaldo/Qualitaetsgesichert/{start_of_data}/{end_of_data}"
            response = requests.get(
                url, headers={"Authorization": f"Bearer {self.token}"}
            )
            df = pd.read_csv(
                io.StringIO(response.text),
                sep=";",
                header=0,
                decimal=",",
                na_values=["N.A."],
            )
            df.rename(mapper=str.lower, axis="columns", inplace=True)
            df["von"] = pd.to_datetime(
                df["datum"] + " " + df["von"] + " " + df["zeitzone"],
                format="%d.%m.%Y %H:%M %Z",
                utc=True,
            ).dt.tz_localize(None)
            df["bis"] = pd.to_datetime(
                df["datum"] + " " + df["bis"] + " " + df["zeitzone"],
                format="%d.%m.%Y %H:%M %Z",
                utc=True,
            ).dt.tz_localize(None)
            df = df.drop(["datum", "zeitzone"], axis=1).set_index("von")
            with self.engine.begin() as conn:
                df.to_sql(tablename, conn, if_exists="append")

    def activated_automatic_balancing_capacity(self):
        tablename = "aktivierte_srl"
        start_of_data = "2013-01-01T00:00:00"
        start_of_data = self.find_latest(tablename, "bis", start_of_data)
        end_of_data = dt.date.today() - dt.timedelta(days=30)
        end_of_data = dt.datetime.combine(end_of_data, dt.datetime.min.time()).strftime(
            api_date_format
        )
        if start_of_data < end_of_data:
            url = f"https://ds.netztransparenz.de/api/v1/data/nrvsaldo/AktivierteSRL/Qualitaetsgesichert/{start_of_data}/{end_of_data}"
            response = requests.get(
                url, headers={"Authorization": f"Bearer {self.token}"}
            )
            df = pd.read_csv(
                io.StringIO(response.text),
                sep=";",
                header=0,
                decimal=",",
                na_values=["N.A."],
            )
            df.rename(mapper=str.lower, axis="columns", inplace=True)
            df["von"] = pd.to_datetime(
                df["datum"] + " " + df["von"] + " " + df["zeitzone"],
                format="%d.%m.%Y %H:%M %Z",
                utc=True,
            ).dt.tz_localize(None)
            df["bis"] = pd.to_datetime(
                df["datum"] + " " + df["bis"] + " " + df["zeitzone"],
                format="%d.%m.%Y %H:%M %Z",
                utc=True,
            ).dt.tz_localize(None)
            df = df.drop(["datum", "zeitzone"], axis=1).set_index("von")
            with self.engine.begin() as conn:
                df.to_sql(tablename, conn, if_exists="append")

    def activated_manual_balancing_capacity(self):
        tablename = "aktivierte_mrl"
        start_of_data = "2013-01-01T00:00:00"
        start_of_data = self.find_latest(tablename, "bis", start_of_data)
        end_of_data = dt.date.today() - dt.timedelta(days=30)
        end_of_data = dt.datetime.combine(end_of_data, dt.datetime.min.time()).strftime(
            api_date_format
        )
        if start_of_data < end_of_data:
            url = f"https://ds.netztransparenz.de/api/v1/data/nrvsaldo/AktivierteMRL/Qualitaetsgesichert/{start_of_data}/{end_of_data}"
            response = requests.get(
                url, headers={"Authorization": f"Bearer {self.token}"}
            )
            df = pd.read_csv(
                io.StringIO(response.text),
                sep=";",
                header=0,
                decimal=",",
                na_values=["N.A."],
            )
            df.rename(mapper=str.lower, axis="columns", inplace=True)
            df["von"] = pd.to_datetime(
                df["datum"] + " " + df["von"] + " " + df["zeitzone"],
                format="%d.%m.%Y %H:%M %Z",
                utc=True,
            ).dt.tz_localize(None)
            df["bis"] = pd.to_datetime(
                df["datum"] + " " + df["bis"] + " " + df["zeitzone"],
                format="%d.%m.%Y %H:%M %Z",
                utc=True,
            ).dt.tz_localize(None)
            df = df.drop(["datum", "zeitzone"], axis=1).set_index("von")
            with self.engine.begin() as conn:
                df.to_sql(tablename, conn, if_exists="append")

    def value_of_avoided_activation(self):
        tablename = "value_of_avoided_activation"
        start_of_data = "2023-11-01T00:00:00"
        start_of_data = self.find_latest(tablename, "bis", start_of_data)
        end_of_data = dt.date.today() - dt.timedelta(days=30)
        end_of_data = dt.datetime.combine(end_of_data, dt.datetime.min.time()).strftime(
            api_date_format
        )
        if start_of_data < end_of_data:
            url = f"https://ds.netztransparenz.de/api/v1/data/nrvsaldo/VoAA/Qualitaetsgesichert/{start_of_data}/{end_of_data}"
            response = requests.get(
                url, headers={"Authorization": f"Bearer {self.token}"}
            )
            df = pd.read_csv(
                io.StringIO(response.text),
                sep=";",
                header=0,
                decimal=",",
                na_values=["N.A."],
            )
            df.rename(mapper=str.lower, axis="columns", inplace=True)
            df["von"] = pd.to_datetime(
                df["datum"] + " " + df["von"] + " " + df["zeitzone"],
                format="%d.%m.%Y %H:%M %Z",
                utc=True,
            ).dt.tz_localize(None)
            df["bis"] = pd.to_datetime(
                df["datum"] + " " + df["bis"] + " " + df["zeitzone"],
                format="%d.%m.%Y %H:%M %Z",
                utc=True,
            ).dt.tz_localize(None)
            df = df.drop(["datum", "zeitzone"], axis=1).set_index("von")
            with self.engine.begin() as conn:
                df.to_sql(tablename, conn, if_exists="append")

    def find_latest(self, tablename: str, column_name: str, default):
        try:
            with self.engine.begin() as conn:
                query = sql.text(f"SELECT max({column_name}) FROM {tablename}")
                result = conn.execute(query).scalar() or default
                return result.strftime(api_date_format)
        except Exception:
            return default

    def create_hypertable_if_not_exists(self) -> None:
        for tablename in [
            "prognose_solar",
            "prognose_wind",
            "hochrechnung_solar",
            "hochrechnung_wind",
            "vermarktung_inanspruchnahme_ausgleichsenergie",
            "redispatch",
            "nrv_saldo",
            "rz_saldo",
            "aktivierte_srl",
            "aktivierte_mrl",
            "value_of_avoided_activation",
        ]:
            self.create_single_hypertable_if_not_exists(tablename, "von")

    def check_table_exists(self, tablename):
        return sql.inspect(self.engine).has_table(tablename)

    def crawl_temporal(
        self, begin: datetime | None = None, end: datetime | None = None
    ):
        # TODO begin and end is not respected
        log.error("BEGIN AND END IS CURRENTLY NOT RESPECTED")
        # crawler.check_health()
        if not self.check_table_exists("prognose_solar"):
            log.info("No Solar")
            self.forecast_solar()
        if not self.check_table_exists("prognose_wind"):
            log.info("No Wind")
            self.forecast_wind()
        self.extrapolation_solar()
        self.extrapolation_wind()
        self.utilization_balancing_energy()
        self.redispatch()
        self.gcc_balance()
        self.lfc_area_balance()
        self.activated_automatic_balancing_capacity()
        self.activated_manual_balancing_capacity()
        self.value_of_avoided_activation()


if __name__ == "__main__":
    logging.basicConfig()
    from pathlib import Path

    config = load_config(Path(__file__).parent.parent / "config.yml")
    craw = NetztransparenzCrawler("netztransparenz", config=config)
    craw.crawl_temporal()
    craw.set_metadata(metadata_info)
