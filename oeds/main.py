#!/usr/bin/env python3
# SPDX-FileCopyrightText: Florian Maurer
#
# SPDX-License-Identifier: AGPL-3.0-or-later
import argparse
import logging
import pandas as pd
import sys
from pathlib import Path
from typing import TypedDict
from datetime import date, datetime, timedelta

from oeds.base_crawler import (
    BaseCrawler,
    ContinuousCrawler,
    CrawlerConfig,
    DownloadOnceCrawler,
    load_config,
    empty_config,
)

log = logging.getLogger("OEDS")
log.setLevel(logging.INFO)


def start_crawler(crawler: BaseCrawler, begin: datetime = None, end: datetime = None):
    if isinstance(crawler, DownloadOnceCrawler):
        crawler.crawl_structural()
    if isinstance(crawler, ContinuousCrawler):
        crawler.crawl_temporal(begin, end)


def cli(args=None):
    parser = argparse.ArgumentParser(description="Open-Energy-Data-Server CLI")
    parser.add_argument(
        "--db",
        type=str,
        help="set the DB URI",
    )
    parser.add_argument(
        "--crawler-list", nargs="*", help="List of crawlers to run (default: all)"
    )
    parser.add_argument(
        "-l",
        "--loglevel",
        help="logging level used for file log",
        default="INFO",
        type=str,
        metavar="LOGLEVEL",
        choices=set(logging._nameToLevel.keys()),
    )
    parser.add_argument("--config", type=str, help="Path to config.yaml")
    parsed_args = parser.parse_args(args)

    logging.basicConfig(level=parsed_args.loglevel)
    from oeds.crawler import crawlers

    selected_crawlers = (
        parsed_args.crawler_list if parsed_args.crawler_list else list(crawlers.keys())
    )
    if parsed_args.config:
        config = load_config(parsed_args.config)
    else:
        config = empty_config()

    if parsed_args.db:
        config["db_uri"] = parsed_args.db
    print(config["db_uri"])

    for crawler_name in selected_crawlers:
        log.info("Starting crawler: %s", crawler_name)
        try:
            crawler_class = crawlers[crawler_name]
        except Exception:
            raise ValueError(f"crawler {crawler_name} does not exist")

        crawler = crawler_class(crawler_name, config)
        start_crawler(crawler)


if __name__ == "__main__":
    logging.basicConfig()
    from oeds.crawler import crawlers

    #Set a time range for continues crawler, which does not have own crawl_temporal()
    begin_continues_crawl = pd.Timestamp("20240101")
    end_continues_crawl = pd.Timestamp("20241231")

    #Check if the time range is time zone-naive for the continuous crawler, which does not have its own crawl_temporal()
    if begin_continues_crawl.tz is not None :
        sys.exit(f"❌ Error: Timestamp {begin_continues_crawl} is timezone-aware ({begin_continues_crawl.tz}). Please use a naive timestamp instead.")
    elif end_continues_crawl.tz is not None:
        sys.exit(f"❌ Error: Timestamp {end_continues_crawl} is timezone-aware ({end_continues_crawl.tz}). Please use a naive timestamp instead.")
    else:
        pass


    config = load_config(Path(__file__).parent.parent / "config.yml")
    for schema_name, crawler_class in crawlers.items():
        crawler = crawler_class(schema_name, config)
        start_crawler(crawler, begin=begin_continues_crawl, end=end_continues_crawl)
