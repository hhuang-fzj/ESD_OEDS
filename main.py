#!/usr/bin/env python3
# SPDX-FileCopyrightText: Florian Maurer
#
# SPDX-License-Identifier: AGPL-3.0-or-later
import logging
from pathlib import Path

from crawler import crawlers
from crawler.common.base_crawler import (
    BaseCrawler,
    ContinuousCrawler,
    DownloadOnceCrawler,
    load_config,
)

log = logging.getLogger("OEDS")
log.setLevel(logging.INFO)


def start_crawler(crawler: BaseCrawler):
    if isinstance(crawler, DownloadOnceCrawler):
        crawler.crawl_structural()
    if isinstance(crawler, ContinuousCrawler):
        crawler.crawl_temporal()


if __name__ == "__main__":
    logging.basicConfig()
    config = load_config(Path(__file__).parent / "config.yml")
    for schema_name, crawler_class in crawlers.items():
        crawler = crawler_class(schema_name, config)
        start_crawler(crawler)
