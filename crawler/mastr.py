# SPDX-FileCopyrightText: Florian Maurer, Christian Rieke
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import logging

from sqlalchemy import create_engine, text
from common.base_crawler import DownloadOnceCrawler, create_schema_only, set_metadata_only, load_config
from open_mastr import Mastr

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
        try:
            with self.engine.connect() as conn:
                return conn.execute(text("SELECT 1 from balancing_area limit 1")).scalar() == 1
        except Exception as e:
            return False
    
    def crawl_structural(self, recreate: bool=False):
        if not self.structure_exists() or recreate:
            mastr_downloader = Mastr(engine=self.engine)
            mastr_downloader.download()

if __name__ == "__main__":
    logging.basicConfig()
    from pathlib import Path
    config = load_config(Path(__file__).parent.parent / "config.yml")
    mastr = MastrDownloader("mastr", config=config)
    mastr.crawl_structural(recreate=False)
