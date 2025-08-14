# SPDX-FileCopyrightText: Florian Maurer, Christian Rieke
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import logging

from open_mastr import Mastr
from sqlalchemy import text

from common.base_crawler import (
    DownloadOnceCrawler,
    load_config,
)

logging.basicConfig()
log = logging.getLogger("MaStR")
log.setLevel(logging.INFO)

metadata_info = {
    "schema_name": "mastr",
    "data_source": "https://download.marktstammdatenregister.de/Gesamtdatenexport",
    "license": "DL-DE/BY-2-0",
    "description": "Marktstammdatenregistrer. Registration data for energy users in germany by energy type and usage.",
    "contact": "",
    "temporal_start": "2019-01-31",
}


class MastrDownloader(DownloadOnceCrawler):
    def structure_exists(self) -> bool:
        query = text("SELECT 1 from balancing_area limit 1")
        try:
            with self.engine.connect() as conn:
                return conn.execute(query).scalar() == 1
        except Exception:
            return False

    def crawl_structural(self, recreate: bool = False):
        if not self.structure_exists() or recreate:
            mastr_downloader = Mastr(engine=self.engine)
            mastr_downloader.download()


if __name__ == "__main__":
    logging.basicConfig()
    from pathlib import Path

    config = load_config(Path(__file__).parent.parent / "config.yml")
    mastr = MastrDownloader("mastr", config=config)
    mastr.crawl_structural(recreate=False)
