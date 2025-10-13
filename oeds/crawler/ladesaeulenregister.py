# SPDX-FileCopyrightText: Vassily Aliseyko
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""
The Charging station map is available at:
https://www.bundesnetzagentur.de/DE/Fachthemen/ElektrizitaetundGas/E-Mobilitaet/Ladesaeulenkarte/Karte/Ladesaeulenkarte.html
One can download the raw file as CSV from this link:
https://www.bundesnetzagentur.de/SharedDocs/Downloads/DE/Sachgebiete/Energie/Unternehmen_Institutionen/E_Mobilitaet/Ladesaeulenregister_CSV.csv?__blob=publicationFile&v=42
"""

import logging

import pandas as pd
from sqlalchemy import text

from oeds.base_crawler import DEFAULT_CONFIG_LOCATION, DownloadOnceCrawler, load_config

log = logging.getLogger("ladesaeulenregister")
log.setLevel(logging.INFO)

metadata_info = {
    "schema_name": "ladesaeulenregister",
    "data_date": "2025-07-18",
    "data_source": "https://www.bundesnetzagentur.de/DE/Fachthemen/ElektrizitaetundGas/E-Mobilitaet/Ladesaeulenkarte/start.html",
    "license": "CC-BY-4.0",
    "description": "Charging stations for EV. Coordinate referenced power usage of individual chargers.",
    "contact": "",
    "temporal_start": None,
    "temporal_end": None,
}


class LadesaeulenregisterCrawler(DownloadOnceCrawler):
    def structure_exists(self) -> bool:
        try:
            query = text("SELECT 1 from ladesaeulenregister limit 1")
            with self.engine.connect() as conn:
                return conn.execute(query).scalar() == 1
        except Exception:
            return False

    def crawl_structural(self, recreate: bool = False):
        if not self.structure_exists() or recreate:
            log.info("Crawling Ladesäulenregister")
            url = "https://data.bundesnetzagentur.de/Bundesnetzagentur/DE/Fachthemen/ElektrizitaetundGas/E-Mobilitaet/Ladesaeulenregister_BNetzA_2025-07-18.csv"
            df = pd.read_csv(
                url,
                skiprows=10,
                delimiter=";",
                encoding="iso-8859-1",
                index_col=0,
                decimal=",",
                low_memory=False,
            )

            with self.engine.begin() as conn:
                df.to_sql("ladesaeulenregister", conn, if_exists="replace")
            log.info("Finished writing Ladesäulenregister to Database")


if __name__ == "__main__":
    logging.basicConfig()

    config = load_config(DEFAULT_CONFIG_LOCATION)
    mastr = LadesaeulenregisterCrawler("ladesaeulenregister", config=config)
    mastr.crawl_structural(recreate=False)
