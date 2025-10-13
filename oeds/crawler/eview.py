# SPDX-FileCopyrightText: Florian Maurer, Jonathan Sejdija
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import logging
import re
from datetime import datetime, timedelta

import pandas as pd
import requests
from sqlalchemy import text

from oeds.base_crawler import DEFAULT_CONFIG_LOCATION, ContinuousCrawler, load_config

log = logging.getLogger("eview")
log.setLevel(logging.INFO)

metadata_info = {
    "schema_name": "eview",
    "data_date": "2024-06-12",
    "data_source": "http://www.eview.de/solarstromdaten/login.php",
    "license": "https://www.eview.de/solarstromdaten/anb.html",
    "description": "Eview solar energy. Plan specific time indexed performance.",
    "contact": "",
    "temporal_start": "2009-01-02 08:15:00",
    "temporal_end": "2024-06-10 21:45:00",
    "concave_hull_geometry": None,
}

TEMPORAL_START = datetime(2022, 11, 1)
# using http instead of https to be faster


class EViewCrawler(ContinuousCrawler):
    OFFSET_FROM_NOW = timedelta(days=1)

    def get_solar_units(self):
        # crawl available pv units
        data = requests.get("http://www.eview.de/solarstromdaten/login.php")
        return re.findall(r"login\.php\?p=;(\w{2});", data.text)

    def crawl_unit_date(self, unit, fetch_date):
        day = datetime.strftime(fetch_date, "%d.%m.%Y")
        url = f"http://www.eview.de/solarstromdaten/export.php?p=;{unit};z;dg1;f0;t{day}/1;km250"
        try:
            df = pd.read_csv(
                url,
                decimal=",",
                index_col=0,
                encoding="iso-8859-1",
                skiprows=4,
                parse_dates=True,
                dayfirst=True,
            )
            log.info("fetched %s records at %s for %s ", df.size, fetch_date, unit)
        except Exception:
            log.info("invalid at %s for %s ", fetch_date, unit)
            return
        if df.size < 2:
            log.info("no data at %s for %s ", fetch_date, unit)
            return
        ddf = df.unstack()
        ddf = ddf.reset_index()
        ddf.index = ddf["Datum und Uhrzeit"]
        ddf.index.name = "datetime"
        del ddf["Datum und Uhrzeit"]
        ddf.columns = ["plant", "value"]
        ddf["plant_id"] = unit
        with self.engine.begin() as conn:
            ddf.to_sql("eview", con=conn, if_exists="append")

    def crawl_unit(self, unit: str, begin: datetime, end: datetime):
        log.info("fetching %s from %s until %s", unit, begin, end)
        for fetch_date in pd.date_range(begin, end):
            self.crawl_unit_date(unit, fetch_date)

    def select_latest_per_unit(self, unit: str):
        day = datetime.strftime(TEMPORAL_START, "%Y-%m-%d")
        today = datetime.strftime(datetime.today(), "%Y-%m-%d")
        sql = f"select datetime from eview where plant_id='{unit}' and datetime > '{day}' and datetime < '{today}' order by datetime desc limit 1"
        try:
            with self.engine.begin() as conn:
                return pd.read_sql(sql, conn, parse_dates=["datetime"]).values[0][0]
        except Exception as e:
            log.error(e)
            return TEMPORAL_START

    def create_hypertable_if_not_exists(self) -> None:
        self.create_single_hypertable_if_not_exists("eview", "datetime")

    def get_latest_data(self) -> datetime:
        sql = text("select max(datetime) as datetime from eview")
        try:
            with self.engine.begin() as conn:
                return conn.execute(sql).scalar() or TEMPORAL_START
        except Exception as e:
            log.error(e)
            return TEMPORAL_START

    def get_first_data(self) -> datetime:
        sql = text("select min(datetime) as datetime from eview")
        try:
            with self.engine.begin() as conn:
                return conn.execute(sql).scalar() or TEMPORAL_START
        except Exception as e:
            log.error(e)
            return TEMPORAL_START

    def crawl_from_to(self, begin: datetime | None = None, end: datetime | None = None):
        """Crawls data from begin (inclusive) until end (exclusive)

        Args:
            begin (datetime): included begin datetime from which to crawl
            end (datetime): exclusive end datetime until which to crawl
        """
        solar_plants = self.get_solar_units()
        for plant in solar_plants:
            try:
                if not begin:
                    begin_date = self.select_latest_per_unit(plant)
                    begin = pd.to_datetime(begin_date) + timedelta(days=1)
                if not end or end > datetime.today() - timedelta(days=1):
                    end = datetime.today() - timedelta(days=1)
                self.crawl_unit(plant, begin, end)
            except Exception:
                log.exception(f"Error with {plant}")

    def crawl_temporal(
        self, begin: datetime | None = None, end: datetime | None = None
    ):
        latest = self.get_latest_data()

        if begin:
            first = self.get_first_data()
            if begin < first:
                self.crawl_from_to(begin, first)
        if not end:
            end = datetime.now()
        print(latest)
        if latest < end - self.__class__.get_minimum_offset():
            # leave begin none to crawl lost data, if it was not available for some parts
            # for this crawler, this selects the latest data per plant automatically
            self.crawl_from_to(begin=None, end=end)
        self.create_hypertable_if_not_exists()


if __name__ == "__main__":
    logging.basicConfig()
    config = load_config(DEFAULT_CONFIG_LOCATION)
    ec = EViewCrawler("eview", config)
    plant = "FI"
    ec.crawl_temporal()
