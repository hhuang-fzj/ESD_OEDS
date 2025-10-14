# SPDX-FileCopyrightText: Vassily Aliseyko
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import io
import logging
import os
import zipfile
from pathlib import Path

import geopandas
import requests
from sqlalchemy import text

from oeds.base_crawler import DEFAULT_CONFIG_LOCATION, DownloadOnceCrawler, load_config

log = logging.getLogger("iwu")
log.setLevel(logging.INFO)


metadata_info = {
    "schema_name": "nrw_kwp_waermedichte",
    "data_date": "2024-01-23",
    "data_source": "https://www.opengeodata.nrw.de/produkte/umwelt_klima/klima/kwp/",
    "license": "DL-DE-ZERO-2.0",
    "description": "NRW Building stats. Building specific information regarding buildings and heating, modelled",
    "contact": "",
    "temporal_start": None,
    "temporal_end": None,
}

# This file can not be found currently on the internet.


class KwpCrawler(DownloadOnceCrawler):
    def structure_exists(self) -> bool:
        try:
            query = text("SELECT 1 from nrw_kwp_waermedichte limit 1")
            with self.engine.connect() as conn:
                return conn.execute(query).scalar() == 1
        except Exception:
            return False

    def crawl_structural(self, recreate: bool = False):
        if not self.structure_exists() or recreate:
            if self.download_kwp_data():
                self.save_to_database()
                self.clean()

    def download_kwp_data(self):
        url = "https://www.opengeodata.nrw.de/produkte/umwelt_klima/klima/kwp/KWP-NRW-Waermebedarf_EPSG25832_Geodatabase.zip"
        response = requests.get(url)
        if response.status_code == 200:
            z = zipfile.ZipFile(io.BytesIO(response.content))
            logging.log(logging.INFO, "Downloaded the KWP NRW ZIP file")

            base_path = Path(__file__).parent.parent / "data" / "kwp_nrw"
            if not base_path.exists():
                z.extractall(base_path)
                logging.log(logging.INFO, "Extracted KWP NRW GDB")
            else:
                logging.log(logging.INFO, "KWP NRW GDB already exists")

            return True
        else:
            log.info("Failed to download the ZIP file")
            return False

    def save_to_database(self):
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    "DROP TABLE IF EXISTS public.waermedichte CASCADE; DROP SCHEMA IF EXISTS nrw_kwp_waermedichte CASCADE;"
                )
            )
        start_i = 0
        end_i = 1000
        while end_i < 12710309:
            data = geopandas.read_file(
                os.path.join(os.path.dirname(__file__))
                + r"\data\kwp_nrw\Waermebedarf_NRW.gdb",
                rows=slice(start_i, end_i, None),
            )

            start_i = end_i
            end_i += 1000
            if end_i > 12710308:
                end_i = 12710308
            with self.engine.begin() as conn:
                data.to_postgis("waermedichte", conn, if_exists="append")

        with self.engine.begin() as conn:
            conn.execute(
                text(
                    "CREATE SCHEMA IF NOT EXISTS nrw_kwp_waermedichte; ALTER TABLE public.waermedichte SET SCHEMA nrw_kwp_waermedichte;"
                )
            )

    def clean(self):
        base_path = Path(__file__).parent.parent / "data" / "kwp_nrw"
        file_list = os.listdir(base_path / "Waermebedarf_NRW.gdb")
        for file_name in file_list:
            file_path = base_path / "Waermebedarf_NRW.gdb" / file_name
            if file_path.is_file():
                file_path.unlink()
        (base_path / "Waermebedarf_NRW.gdb").rmdir()

        file_list = os.listdir(base_path)
        for file_name in file_list:
            file_path = base_path / file_name
            if file_path.is_file():
                file_path.unlink()
        base_path.rmdir()


if __name__ == "__main__":
    logging.basicConfig()
    from pathlib import Path

    config = load_config(DEFAULT_CONFIG_LOCATION)
    crawler = KwpCrawler("nrw_kwp_waermedichte", config)
    crawler.crawl_structural()
    crawler.set_metadata(metadata_info)
