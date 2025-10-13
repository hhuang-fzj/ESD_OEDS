# SPDX-FileCopyrightText: Marvin Lorber
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""
This crawler downloads all the data of the GIE transparency platform. (https://agsi.gie.eu/, https://alsi.gie.eu/)
The resulting data is not available under an open-source license and should not be reshared but is available for crawling yourself.

license Information from Websire:
Data usage
    It is mandatory to credit or mention to GIE (Gas Infrastructure Europe), AGSI or ALSI as data source when using or repackaging this data.
Contact
    For data inquiries, please contact us via transparency@gie.eu

API and data documentation: https://www.gie.eu/transparency-platform/GIE_API_documentation_v007.pdf

This crawler uses the roiti-gie client: https://github.com/ROITI-Ltd/roiti-gie
"""

import asyncio
import logging
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from roiti.gie import GiePandasClient

from oeds.base_crawler import ContinuousCrawler, CrawlerConfig, load_config

log = logging.getLogger("gie")
log.setLevel(logging.INFO)
# silence roiti logger
logging.getLogger("GiePandasClient").setLevel(logging.WARNING)

metadata_info = {
    "schema_name": "gie",
    "data_date": "2024-06-12",
    "data_source": "https://agsi.gie.eu/",
    "license": "https://www.gie.eu/privacy-policy/",
    "description": "Gas Inventory Transparency. Time and country indexed capacity and consumption of gas.",
    "contact": "",
    "temporal_start": "2012-01-01 00:00:00",
    "temporal_end": "2024-06-11 01:13:10",
}
TEMPORAL_START = datetime(2012, 1, 1)
DATA_HIERARCHY = ["country", "company", "location"]


async def collect_gie_date(date, pandas_client: GiePandasClient, engine):
    df_agsi_europe = await pandas_client.query_country_agsi_storage(date=date)
    df_alsi_europe = await pandas_client.query_country_alsi_storage(date=date)

    with engine.begin() as conn:
        recursiveWrite(df_agsi_europe, "agsi", conn, pandas_client, 0)
        recursiveWrite(df_alsi_europe, "alsi", conn, pandas_client, 0)


def extract(df, client: GiePandasClient):
    result = [0] * len(df)
    for i in range(len(df)):
        result[i] = client._pandas_df_format(
            df.loc[i, "children"], client._FLOATING_COLS, client._DATE_COLS
        )
        result[i] = result[i].assign(parent=df.loc[i, "name"])
    return result


def recursiveWrite(
    df, data_identifier: str, conn, client: GiePandasClient, level: int = 0
):
    df_children = extract(df, client)

    for df_child in df_children:
        if level < 2:
            recursiveWrite(df_child, data_identifier, conn, client, level + 1)
            df_child.drop(columns="children", inplace=True)
        # rename columns to lowercase titles
        df_child.rename(mapper=str.lower, axis="columns", inplace=True)

        df_child.to_sql(
            f"gie_{data_identifier}_{DATA_HIERARCHY[level]}",
            conn,
            if_exists="append",
        )


class GieCrawler(ContinuousCrawler):
    def __init__(self, schema_name: str, config: CrawlerConfig):
        super().__init__(schema_name, config)
        self.API_KEY = config["gie_api_key"]
        if not self.API_KEY or self.API_KEY == "YOUR_GIE_API_KEY":
            raise Exception("GIE_API_KEY is not defined")

    def get_latest_data(self) -> datetime:
        sql = "SELECT max(gasdaystart) FROM gie_agsi_country AS gie"
        try:
            with self.engine.begin() as conn:
                return pd.read_sql(sql, conn, parse_dates=["datetime"]).values[0][0]
        except Exception as e:
            log.error(
                f"Could not read start date - using default: {TEMPORAL_START} - {e}"
            )
            return TEMPORAL_START

    def get_first_data(self) -> datetime:
        sql = "SELECT min(gasdaystart) FROM gie_agsi_country AS gie"
        try:
            with self.engine.begin() as conn:
                return pd.read_sql(sql, conn, parse_dates=["datetime"]).values[0][0]
        except Exception as e:
            log.error(
                f"Could not read start date - using default: {TEMPORAL_START} - {e}"
            )
            return TEMPORAL_START

    async def crawl_data(self, begin: datetime, end: datetime):
        gie_pandas = GiePandasClient(api_key=self.API_KEY)
        try:
            log.info(f"fetching from {begin} until {end}")
            api_call_count = 0
            for fetch_date in pd.date_range(begin, end):
                log.info(f"Handling {fetch_date}")
                api_call_count += 1
                if api_call_count > 30:
                    # The api limits clients to 60 Requests per second
                    # So we have to make sure to stay below that
                    time.sleep(1)
                    api_call_count = 0
                await collect_gie_date(
                    datetime.strftime(fetch_date, "%Y-%m-%d"), gie_pandas, self.engine
                )
        finally:
            await gie_pandas.close_session()

    def crawl_from_to(self, begin: datetime, end: datetime):
        """Crawls data from begin (inclusive) until end (exclusive)

        Args:
            begin (datetime): included begin datetime from which to crawl
            end (datetime): exclusive end datetime until which to crawl
        """
        end = end - timedelta(days=1)
        asyncio.run(self.crawl_data(begin, end))

    def create_hypertable_if_not_exists(self) -> None:
        for tablename in [
            "gie_agsi_country",
            "gie_agsi_company",
            "gie_agsi_location",
            "gie_alsi_country",
            "gie_alsi_company",
            "gie_alsi_location",
        ]:
            self.create_single_hypertable_if_not_exists(tablename, "gasdaystart")


if __name__ == "__main__":
    logging.basicConfig()
    config = load_config(Path(__file__).parent.parent / "config.yml")
    ec = GieCrawler("gie", config)
    # ec.crawl_temporal()
    ec.create_hypertable_if_not_exists()
