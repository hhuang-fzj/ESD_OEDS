# SPDX-FileCopyrightText: Vassily Aliseyko, Florian Maurer, Christian Rieke

# SPDX-License-Identifier: AGPL-3.0-or-later

import logging
from io import StringIO

import pandas as pd
import requests
from oeds.base_crawler import DEFAULT_CONFIG_LOCATION, DownloadOnceCrawler, load_config
from sqlalchemy import text

log = logging.getLogger("opsd")
log.setLevel(logging.INFO)
logging.basicConfig()

metadata_info = {
    "schema_name": "opsd_national_capacity",
    "data_date": "2020-10-01",
    "data_source": "https://data.open-power-system-data.org/national_generation_capacity/2020-10-01/national_generation_capacity_stacked.csv",
    "license": "Attribution required",
    "description": "National generation capacity from opsd. European nation generation by energy source in MW.",
    "contact": "",
    "temporal_start": "1990-01-01",
    "temporal_end": "2020-10-01",
    "concave_hull_geometry": None,
}


national_generation_capacity_url = "https://data.open-power-system-data.org/national_generation_capacity/2020-10-01/national_generation_capacity_stacked.csv"


class OpsdCrawler(DownloadOnceCrawler):
    def structure_exists(self) -> bool:
        try:
            query = text("SELECT 1 from iwu_typgebäude limit 1")
            with self.engine.connect() as conn:
                return conn.execute(query).scalar() == 1
        except Exception:
            return False

    def crawl_structural(self, recreate: bool = False):
        if not self.structure_exists() or recreate:
            log.info("Fetching data from %s", national_generation_capacity_url)
            response = requests.get(national_generation_capacity_url)
            response.raise_for_status()

            log.info("Loading data into DataFrame")
            data = pd.read_csv(StringIO(response.text))

            log.info("Writing data to the database")
            with self.engine.begin() as conn:
                data.to_sql(
                    "national_generation_capacity",
                    conn,
                    if_exists="replace",
                )
            log.info("Data written successfully")
        self.create_hypertable_if_not_exists()


if __name__ == "__main__":
    logging.basicConfig()

    config = load_config(DEFAULT_CONFIG_LOCATION)
    craw = OpsdCrawler("opsd", config)
    craw.crawl_structural(recreate=False)
