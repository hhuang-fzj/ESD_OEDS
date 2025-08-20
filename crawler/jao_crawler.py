# SPDX-FileCopyrightText: Simon Hesselmann, Florian Maurer
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""
Downloads latest data from the Joint Allocation Office (JAO).

Currently, this only includes market results and their bids.
Data from the https://publicationtool.jao.eu/ is not yet included.

Good analysis of this data is included in https://boerman.dev/ and
https://data.boerman.dev/d/5CYxW2JVz/flows-scheduled-commercial-exchanges-day-ahead
"""

import logging
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests
from dateutil.relativedelta import relativedelta
from jao import JaoAPIClient

# pip install git+https://github.com/maurerle/jao-py@improve_horizon_support
from sqlalchemy import MetaData, text
from sqlalchemy.exc import OperationalError

from .common.base_crawler import ContinuousCrawler, load_config

log = logging.getLogger("jao")

metadata_info = {
    "schema_name": "jao",
    "data_date": "2024-04-30",
    "data_source": "https://www.jao.eu/auctions#/",
    "license": "https://www.jao.eu/terms-conditions",
    "description": "JAO energy auction. Energy bids by country and timestamp.",
    "contact": "",
    "temporal_start": "2019-01-01 00:00:00",
    "temporal_end": "2024-04-30 00:00:00",
    "concave_hull_geometry": None,
}

MIN_WEEKLY_DATE = datetime(2023, 1, 1)

DELTAS = {
    "seasonal": relativedelta(years=1),
    "yearly": relativedelta(years=1),
    "monthly": relativedelta(month=1),
    "weekly": relativedelta(weeks=1, weekday=0),
    "daily": relativedelta(days=1),
    "intraday": relativedelta(days=1),
}

TEMPORAL_START = datetime(2019, 1, 1)


def string_to_timestamp(*dates):
    timestamps = []
    date_formats = [
        "%Y-%m-%d-%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d",
    ]
    for date_string in dates:
        if isinstance(date_string, str):
            timestamp = None

            for date_format in date_formats:
                try:
                    timestamp = datetime.strptime(date_string, date_format)
                    timestamps.append(timestamp)
                    break
                except ValueError:
                    pass

            if timestamp is None:
                timestamps.append(None)
        else:
            timestamps.append(date_string)

    return timestamps if len(timestamps) > 1 else timestamps[0]


class JaoClientWrapper:
    def __init__(self, api_key: str | None):
        if not api_key:
            raise Exception("No JAO API key provided")
        self.client = JaoAPIClient(api_key)

    def get_bids(self, auction_id: str):
        try:
            return self.client.query_auction_bids_by_id(auction_id)
        except requests.exceptions.HTTPError as e:
            log.error(f"Error fetching bids for auction {auction_id}: {e}")
            return pd.DataFrame()

    def get_auctions(
        self,
        corridor: str,
        from_date: datetime,
        to_date: datetime,
        horizon="Monthly",
    ) -> pd.DataFrame:
        from_date, to_date = string_to_timestamp(from_date, to_date)
        try:
            return self.client.query_auction_stats(
                from_date, to_date, corridor, horizon
            )
        except requests.exceptions.HTTPError as e:
            if e.response.status_code != 400:
                log.error(
                    f"Error fetching auctions for corridor {corridor} from {from_date} to {to_date}: {e}"
                )
            return pd.DataFrame()

    def get_horizons(self):
        return self.client.query_auction_horizons()

    def get_corridors(self):
        return self.client.query_auction_corridors()


class JaoCrawler(ContinuousCrawler):
    TIMEDELTA = timedelta(days=2)

    def get_latest_data(self) -> datetime:
        query = text("SELECT MAX(date) FROM eu_ets")
        try:
            with self.engine.connect() as conn:
                return conn.execute(query).scalar() + timedelta(hours=12)
        except Exception:
            log.error("No instrat_pl data found")
            return TEMPORAL_START

    def get_first_data(self) -> datetime:
        query = text("SELECT MIN(datum_von) FROM generation")
        try:
            with self.engine.connect() as conn:
                return conn.execute(query).scalar() or TEMPORAL_START
        except Exception:
            log.error("No intrat_pl data found")
            return TEMPORAL_START

    def get_tables(self):
        metadata = MetaData()
        metadata.reflect(bind=self.engine)
        return metadata.tables.keys()

    def create_hypertable_if_not_exists(self):
        for table_name in self.get_tables():
            self.create_single_hypertable_if_not_exists(table_name, "date")

    def calculate_min_max(
        self, corridor, horizon="Yearly"
    ) -> tuple[datetime, datetime]:
        table_name = "auctions"
        try:
            query = text(
                f"SELECT MIN(date), MAX(date) FROM auctions where corridor='{corridor}' and horizon='{horizon}'"
            )
            with self.engine.connect() as conn:
                scalar = conn.execute(query).scalar()
            min_date = scalar[0]
            max_date = scalar[1]
            return string_to_timestamp(min_date), string_to_timestamp(max_date)
        except Exception as e:
            log.error(f"error crawling {e}")
            log.info(
                f"The table '{table_name}' did not exist or was empty. Crawling whole interval"
            )
            return None, None

    def crawl_single_horizon(
        self,
        jao_client,
        from_date,
        to_date,
        corridor,
        horizon,
    ):
        table_name = f"bids_{horizon.lower()}"
        table_name = table_name.replace("-", "_").replace(" ", "_")

        try:
            auctions_data = jao_client.get_auctions(
                corridor, from_date, to_date, horizon
            )
        except Exception as e:
            log.error(f"Did not get Auctions for {corridor} - {horizon}: {e}")
            return
        if auctions_data.empty:
            return

        log.info(
            f"started crawling bids of {corridor} - {horizon} for {len(auctions_data)} auctions"
        )
        auctions_data["horizon"] = horizon

        try:
            with self.engine.begin() as connection:
                auctions_data.to_sql(
                    "auctions",
                    connection,
                    if_exists="append",
                    index=False,
                    method="multi",
                    chunksize=10_000,
                )
        except OperationalError:
            log.exception(
                f"database error writing {len(auctions_data)} entries - trying again"
            )
            import time

            time.sleep(5)
            with self.engine.begin() as connection:
                auctions_data.to_sql(
                    "auctions", connection, if_exists="append", index=False
                )

        for auction_id, auction_date in auctions_data.loc[:, ["id", "date"]].values:
            bids_data = jao_client.get_bids(auction_id)

            if bids_data.empty:
                continue

            bids_data["auctionId"] = auction_id
            bids_data["date"] = auction_date
            try:
                with self.engine.begin() as connection:
                    bids_data.to_sql(
                        table_name,
                        connection,
                        if_exists="append",
                        index=False,
                        method="multi",
                        chunksize=10_000,
                    )
            except OperationalError:
                log.error(
                    f"database error writing {len(bids_data)} entries - trying again"
                )
                import time

                time.sleep(5)
                with self.engine.begin() as connection:
                    bids_data.to_sql(
                        table_name,
                        connection,
                        if_exists="append",
                        index=False,
                        method="multi",
                        chunksize=10_000,
                    )

    def crawl_from_to(self, begin: datetime, end: datetime):
        jao_client = JaoClientWrapper(self.config.get("jao_api_key"))
        log.info(f"starting run_data_crawling from {begin} to {end}")
        for horizon in jao_client.get_horizons():
            for corridor in jao_client.get_corridors():
                if "intraday" == horizon.lower():
                    continue

                if horizon.lower() == "weekly":
                    begin = max(MIN_WEEKLY_DATE, begin)

                first_date, last_date = self.calculate_min_max(corridor, horizon)
                log.info(f"crawl {horizon}, {corridor} - {begin} - {end}")
                if not first_date:
                    first_date = end
                if begin < first_date:
                    log.info(f"crawling before {begin} until {first_date}")
                    self.crawl_single_horizon(
                        jao_client,
                        begin,
                        first_date,
                        corridor,
                        horizon,
                    )
                delta = DELTAS.get(horizon.lower(), timedelta(days=1))
                if last_date and end - delta > last_date:
                    # must be at least one horizon ahead, otherwise we are crawling duplicates
                    log.info(f"crawling before {last_date} until {end}")
                    self.crawl_single_horizon(
                        jao_client,
                        last_date,
                        end,
                        corridor,
                        horizon,
                    )
                log.info(f"finished crawling bids of {corridor} - {horizon}")
        log.info(f"finished run_data_crawling from {begin} to {end}")


if __name__ == "__main__":
    logging.basicConfig(level="INFO")
    config = load_config(Path(__file__).parent.parent / "config.yml")
    craw = JaoCrawler("jao", config)

    now = datetime.now()
    from_date = TEMPORAL_START
    # not sure if this is exclusive, so substract a ms to be safe
    to_date = datetime(now.year, now.month, 1) - timedelta(microseconds=1)

    craw.crawl_from_to(from_date, to_date)
    craw.set_metadata(metadata_info)
