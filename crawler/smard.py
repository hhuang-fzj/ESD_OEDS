# SPDX-FileCopyrightText: Florian Maurer, Jonathan Sejdija
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""
This crawler downloads all the generation data of germany from the smard portal of the Bundesnetzagentur at smard.de.
It contains mostly data for Germany which is also availble in the ENTSO-E transparency platform but under a CC open license.
"""

import io
import logging
from datetime import datetime, timedelta

import pandas as pd
import requests
from sqlalchemy import text

from common.base_crawler import ContinuousCrawler, load_config

log = logging.getLogger("smard")
default_start_date = "2024-06-02 22:00:00"  # "2023-11-26 22:45:00"


metadata_info = {
    "schema_name": "smard",
    "data_date": "2024-06-12",
    "data_source": "https://www.smard.de/",
    "license": "CC-BY-4.0",
    "description": "Open access ENTSOE  Germany. Production of energy by good and timestamp",
    "contact": "",
    "temporal_start": "2015-01-01 00:00:00",
    "temporal_end": "2024-06-09 21:45:00",
    "concave_hull_geometry": None,
}

MODULE_IDS = {}
MODULE_IDS["generation"] = [
    1001224,
    1004066,
    1004067,
    1004068,
    1001223,
    1004069,
    1004071,
    1004070,
    1001226,
    1001228,
    1001227,
    1001225,
]
MODULE_IDS["market"] = [
    8004169,
    8004170,
    8000251,
    8005078,
    8000252,
    8000253,
    8000254,
    8000255,
    8000256,
    8000257,
    8000258,
    8000259,
    8000260,
    8000261,
    8000262,
    8004996,
    8004997,
]
MODULE_IDS["power_flow"] = [
    31004963,
    31004736,
    31004737,
    31004740,
    31004741,
    31004988,
    31004990,
    31004992,
    31004994,
    31004738,
    31004742,
    31004743,
    31004744,
    31004880,
    31004881,
    31004882,
    31004883,
    31004884,
    31004885,
    31004886,
    31004887,
    31004888,
    31004739,
]
MODULE_IDS["allocation"] = [
    22004629,
    22004722,
    22004724,
    22004404,
    22004409,
    22004545,
    22004546,
    22004548,
    22004550,
    22004551,
    22004552,
    22004405,
    22004547,
    22004403,
    22004406,
    22004407,
    22004408,
    22004410,
    22004412,
    22004549,
    22004553,
    22004998,
    22004712,
]
MODULE_IDS["forecast_day_ahead"] = [
    2000122,
    2005097,
    2000715,
    2003791,
    2000123,
    2000125,
]
MODULE_IDS["consumption"] = [5000410, 5004387, 5005140, 5004359]
MODULE_IDS["frequency_reserve"] = [15004383, 15004384, 15004382, 15004390]

TEMPORAL_START = datetime(2015, 1, 1)
MAX_DELTA = timedelta(weeks=52)
OFFSET_FROM_NOW = timedelta(hours=-6)
SMARD_URL = "https://www.smard.de/nip-download-manager/nip/download/market-data"


class SmardCrawler(ContinuousCrawler):
    def get_latest_data(self) -> datetime:
        query = text("SELECT MAX(datum_von) FROM generation")
        try:
            with self.engine.connect() as conn:
                return conn.execute(query).scalar() or TEMPORAL_START
        except Exception:
            log.error("No smard data found")
            return TEMPORAL_START

    def get_first_data(self) -> datetime:
        query = text("SELECT MIN(datum_von) FROM generation")
        try:
            with self.engine.connect() as conn:
                return conn.execute(query).scalar() or TEMPORAL_START
        except Exception:
            log.error("No smard data found")
            return TEMPORAL_START

    def create_hypertable_if_not_exists(self) -> None:
        for key in MODULE_IDS.keys():
            self.create_single_hypertable_if_not_exists(key, "datum_von")

    def crawl_from_to(self, begin: datetime, end: datetime):
        """Crawls data from begin (inclusive) until end (exclusive)

        Args:
            begin (datetime): included begin datetime from which to crawl
            end (datetime): exclusive end datetime until which to crawl
        """
        if begin < TEMPORAL_START:
            begin = TEMPORAL_START

        if end > datetime.now() + OFFSET_FROM_NOW:
            end = datetime.now() + OFFSET_FROM_NOW

        sliced_begin = begin
        sliced_end = sliced_begin + MAX_DELTA
        while end > sliced_end:
            self._crawl_single_period(sliced_begin, sliced_end)
            sliced_begin = sliced_end
            sliced_end += MAX_DELTA
        self._crawl_single_period(sliced_begin, end)

    def _crawl_single_period(self, begin: datetime, end: datetime):
        """Gets data for a single period from begin to end and stores it in the database.

        Args:
            begin (datetime): starting point of the period to crawl
            end (datetime): ending point of the period to crawl
        """
        log.info("Crawling smard data from %s to %s", begin, end)

        timestamp_from = int(begin.timestamp() * 1000)
        timestamp_to = int(end.timestamp() * 1000)

        with self.engine.begin() as conn:
            for table_name, modul_ids in MODULE_IDS.items():
                log.debug(
                    "Downloading %s data from smard from %s to %s",
                    table_name,
                    begin,
                    end,
                )
                post_json = {
                    "request_form": [
                        {
                            "format": "CSV",
                            "moduleIds": modul_ids,
                            "region": "DE",
                            "timestamp_from": timestamp_from,
                            "timestamp_to": timestamp_to,
                            "type": "discrete",
                            "language": "de",
                            "resolution": "hour",
                        }
                    ]
                }
                try:
                    result = requests.post(SMARD_URL, json=post_json)
                    # result is csv read it
                    result.raise_for_status()
                    df = pd.read_csv(
                        io.StringIO(result.text),
                        sep=";",
                        index_col="Datum von",
                        date_format="%d.%m.%Y %H:%M",
                        parse_dates=["Datum von", "Datum bis"],
                    )
                    df.index.name = "datum_von"
                    df.columns = [col.lower().replace(" ", "_") for col in df.columns]

                    df.to_sql(table_name, conn, if_exists="append")
                except Exception as e:
                    log.error(
                        "Error while downloading %s data with %s from smard: %s",
                        table_name,
                        post_json,
                        e,
                    )
                    raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    from pathlib import Path

    config = load_config(Path(__file__).parent.parent / "config.yml")
    smard = SmardCrawler("smard", config=config)
    smard.crawl_temporal()
