#!/usr/bin/env python3
# SPDX-FileCopyrightText: Florian Maurer
#
# SPDX-License-Identifier: AGPL-3.0-or-later
import argparse
import logging
from pathlib import Path

from oeds.base_crawler import (
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

def cli(args=None):
    parser = argparse.ArgumentParser(description="Open-Energy-Data-Server CLI")
    parser.add_argument(
        "--db", type=str,
        help="set the DB URI",
        default='postgresql://opendata:opendata@localhost:6432/opendata?options=--search_path={DBNAME}'
    )
    parser.add_argument(
        "--crawler-list", nargs='*',
        help="List of crawlers to run (default: all)"
    )
    parsed_args = parser.parse_args(args)

    logging.basicConfig()
    from oeds.crawler import crawlers
    selected_crawlers = set(parsed_args.crawler_list) if parsed_args.crawler_list else set(crawlers.keys())
    config = {
        "db_uri": parsed_args.db
    }

    for crawler_name in selected_crawlers:
        log.info("Starting crawler: %s", crawler_name)
        crawler_class = crawlers[crawler_name]
        crawler = crawler_class(crawler_name, config)
        start_crawler(crawler)

if __name__ == "__main__":
    logging.basicConfig()
    from oeds.crawler import crawlers
    config = load_config(Path(__file__).parent / "config.yml")
    for schema_name, crawler_class in crawlers.items():
        crawler = crawler_class(schema_name, config)
        start_crawler(crawler)
