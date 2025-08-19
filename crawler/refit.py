# SPDX-FileCopyrightText: Florian Maurer
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""
Data from REFIT paper
https://www.nature.com/articles/sdata2016122

REFIT (An electrical load measurements dataset of United Kingdom households from a two-year longitudinal study)

This dataset is typically used for NILM applications (non-intrusive load monitoring).
"""

import io
import logging

import cloudscraper
import pandas as pd
import py7zr
from sqlalchemy import text

from common.base_crawler import DownloadOnceCrawler, load_config

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)


metadata_info = {
    "schema_name": "refit",
    "data_date": "2024-06-12",
    "data_source": "https://pure.strath.ac.uk/ws/portalfiles/portal/52873459/Processed_Data_CSV.7z",
    "license": "CC-BY-4.0",
    "description": "University of Strathclyde household energy usage. Time-stamped data on various household appliances' energy consumption, detailing usage patterns across different homes.",
    "contact": "",
    "temporal_start": "2013-10-09 13:06:17",
    "temporal_end": "2015-07-10 11:56:32",
}


REFIT_URL = (
    "https://pure.strath.ac.uk/ws/portalfiles/portal/52873459/Processed_Data_CSV.7z"
)
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
}


class RefitCrawler(DownloadOnceCrawler):
    def structure_exists(self) -> bool:
        try:
            query = text("SELECT 1 from refit limit 1")
            with self.engine.connect() as conn:
                return conn.execute(query).scalar() == 1
        except Exception:
            return False

    def crawl_structural(self, recreate: bool = False):
        if not self.structure_exists() or recreate:
            log.info("Download refit dataset")
            self.download_refit_data()
            log.info("Finished writing REFIT to Database")
        self.create_hypertable_if_not_exists()

    def create_hypertable_if_not_exists(self):
        self.create_single_hypertable_if_not_exists("refit", "Time")

    def download_refit_data(self):
        # 2025-08-19 this only works with cloudflare circumvention
        scraper = cloudscraper.create_scraper()
        response = scraper.get(REFIT_URL, headers=headers)
        response.raise_for_status()
        log.info("Write refit to database")
        with py7zr.SevenZipFile(io.BytesIO(response.content), mode="r") as z:
            names = z.getnames()
            for name in names:
                file = z.read([name])[name]
                df = pd.read_csv(file, index_col="Time", parse_dates=["Time"])
                del df["Unix"]
                df["house"] = name
                log.info(f"writing {name}")

                with self.engine.begin() as conn:
                    df.to_sql("refit", conn, if_exists="append", chunksize=10000)


if __name__ == "__main__":
    logging.basicConfig()
    from pathlib import Path

    config = load_config(Path(__file__).parent.parent / "config.yml")
    craw = RefitCrawler("refit", config)
    craw.crawl_structural()
