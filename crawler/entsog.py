#!/usr/bin/env python3
# SPDX-FileCopyrightText: Florian Maurer
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""
This crawler downloads all the data of the ENTSO-G transparency platform.
The resulting data is not available under an open-source license and should not be reshared but is available for crawling yourself.
"""

import logging
import time
import urllib
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import requests
from sqlalchemy import text
from tqdm import tqdm

from common.base_crawler import ContinuousCrawler, load_config

log = logging.getLogger("entsog")
log.setLevel(logging.INFO)

metadata_info = {
    "schema_name": "entsog",
    "data_date": "2024-06-12",
    "data_source": "https://transparency.entsog.eu/api/v1/",
    "license": "https://www.entsog.eu/privacy-policy-and-terms-use",
    "description": "ENTSOG transparency energy. Country specific flows of energy sources.",
    "contact": "",
    "temporal_start": "2017-07-10 02:00:00",
    "temporal_end": "2024-06-10 09:00:00",
    "concave_hull_geometry": None,
}

api_endpoint = "https://transparency.entsog.eu/api/v1/"

fr = date(2020, 5, 18)
to = date.today()

"""
data = pd.read_csv(
    f'{api_endpoint}operationaldata.csv?limit=1000&indicator=Allocation&from={fr}&to={to}')
response = requests.get(api_endpoint+'operationaldatas')
data = pd.read_csv(api_endpoint+'AggregatedData.csv?limit=1000')
response = requests.get(api_endpoint+'AggregatedData?limit=1000')
data = pd.DataFrame(response.json()['AggregatedData'])
"""


def getDataFrame(name, params=["limit=10000"], useJson=False):
    params_str = ""
    if len(params) > 0:
        params_str = "?"
    for param in params[:-1]:
        params_str = params_str + param + "&"
    params_str += params[-1]

    i = 0
    data = pd.DataFrame()
    success = False
    while i < 10 and not success:
        try:
            i += 1
            if useJson:
                url = f"{api_endpoint}{name}.json{params_str}"
                response = requests.get(url)
                data = pd.DataFrame(response.json()[name])
                # replace empty string with None
                data = data.replace([""], [None])
            else:
                url = f"{api_endpoint}{name}.csv{params_str}"
                data = pd.read_csv(url, index_col=False)
            success = True
        except requests.exceptions.InvalidURL:
            raise
        except requests.exceptions.HTTPError as e:
            log.error("Error getting Dataframe")
            if e.response.status_code >= 500:
                log.info(f"{e.response.reason} - waiting 30 seconds..")
                time.sleep(30)
        except urllib.error.HTTPError as e:
            log.error("Error getting Dataframe")
            if e.code >= 500:
                log.info(f"{e.msg} - waiting 30 seconds..")
                time.sleep(30)

    if data.empty:
        raise Exception("could not get any data for params:", params_str)
    data.columns = [x.lower() for x in data.columns]
    return data


class EntsogCrawler(ContinuousCrawler):
    def pullData(self, names):
        pbar = tqdm(names)
        for name in pbar:
            try:
                pbar.set_description(name)
                # use Json as connectionpoints have weird csv
                # TODO Json somehow has different data
                # connectionpoints count differ
                # and tpTSO column are named tSO in connpointdirections
                data = getDataFrame(name, useJson=True)

                with self.engine.begin() as conn:
                    tbl_name = name.lower().replace(" ", "_")
                    data.to_sql(tbl_name, conn, if_exists="replace")

            except Exception:
                log.exception("error pulling data")

        if "operatorpointdirections" in names:
            with self.engine.begin() as conn:
                query = text(
                    'CREATE INDEX IF NOT EXISTS "idx_opd" ON operatorpointdirections (operatorKey, pointKey,directionkey);'
                )
                conn.execute(query)

    def findNewBegin(self, table_name):
        try:
            with self.engine.begin() as conn:
                query = text(f"select max(periodfrom) from {table_name}")
                d = conn.execute(query).fetchone()[0]
            begin = pd.to_datetime(d).date()
        except Exception as e:
            begin = date(2017, 7, 10)
            log.error(f"table does not exist yet - using default start {begin} ({e})")
        return begin

    def pullOperationalData(self, indicators, initial_begin=None, end=None):
        log.info("getting values from operationaldata")
        if not end:
            end = date.today()

        for indicator in indicators:
            tbl_name = indicator.lower().replace(" ", "_")
            if initial_begin:
                begin = initial_begin
            else:
                begin = self.findNewBegin(tbl_name)

            bulks = (end - begin).days
            log.info(
                f"start: {begin}, end: {end}, days: {bulks}, indicator: {indicator}"
            )

            if bulks < 1:
                return
            delta = timedelta(days=1)

            pbar = tqdm(range(int(bulks)))
            for i in pbar:
                beg1 = begin + i * delta
                end1 = begin + (i + 1) * delta
                pbar.set_description(f"op {beg1} to {end1}")

                params = [
                    "limit=-1",
                    "indicator=" + urllib.parse.quote(indicator),
                    "from=" + str(beg1),
                    "to=" + str(end1),
                    "periodType=hour",
                ]
                time.sleep(5)
                # impact of sleeping here is quite small in comparison to 50s query length
                # rate limiting Gateway Timeouts
                df = getDataFrame("operationaldata", params)
                df["periodfrom"] = pd.to_datetime(df["periodfrom"])
                df["periodto"] = pd.to_datetime(df["periodto"])

                try:
                    with self.engine.begin() as conn:
                        df.to_sql(tbl_name, conn, if_exists="append")
                except Exception as e:
                    # allow adding a new column or converting type
                    with self.engine.begin() as conn:
                        log.info(f"handling {repr(e)} by concat")
                        # merge old data with new data
                        prev = pd.read_sql_query(f"select * from {tbl_name}", conn)
                        dat = pd.concat([prev, df])
                        # convert type as pandas needs it
                        dat.to_sql(tbl_name, conn, if_exists="replace")
                        log.info(f"replaced table {tbl_name}")

        # sqlite will only use one index. EXPLAIN QUERY PLAIN shows if index is used
        # ref: https://www.sqlite.org/optoverview.html#or_optimizations
        # reference https://stackoverflow.com/questions/31031561/sqlite-query-to-get-the-closest-datetime
        if "Allocation" in indicators:
            with self.engine.begin() as conn:
                query = text(
                    'CREATE INDEX IF NOT EXISTS "idx_opdata" ON Allocation (operatorKey,periodfrom);'
                )
                conn.execute(query)

                query = text(
                    'CREATE INDEX IF NOT EXISTS "idx_pointKey" ON Allocation (pointKey,periodfrom);'
                )
                conn.execute(query)
        if "Physical Flow" in indicators:
            with self.engine.begin() as conn:
                query = text(
                    'CREATE INDEX IF NOT EXISTS "idx_phys_operator" ON Physical_Flow (operatorKey,periodfrom);'
                )
                conn.execute(query)

                query = text(
                    'CREATE INDEX IF NOT EXISTS "idx_phys_point" ON Physical_Flow (pointKey,periodfrom);'
                )
                conn.execute(query)

        if "Firm Technical" in indicators:
            with self.engine.begin() as conn:
                query = text(
                    'CREATE INDEX IF NOT EXISTS "idx_ft_opdata" ON Firm_Technical (operatorKey,periodfrom);'
                )
                conn.execute(query)

                query = text(
                    'CREATE INDEX IF NOT EXISTS "idx_ft_pointKey" ON Firm_Technical (pointKey,periodfrom);'
                )
                conn.execute(query)

    def crawl_temporal(
        self, begin: datetime | None = None, end: datetime | None = None
    ):
        # TODO begin and end is not respected
        log.error("BEGIN AND END IS CURRENTLY NOT RESPECTED")
        names = [
            "cmpUnsuccessfulRequests",
            "connectionpoints",
            "operators",
            "balancingzones",
            "operatorpointdirections",
            "Interconnections",
            "aggregateInterconnections",
            # 'operationaldata',
            # 'cmpUnavailables',
            # 'cmpAuctions',
            # 'AggregatedData', # operationaldata aggregated for each zone
            # 'tariffssimulations',
            # 'tariffsfulls',
            # 'urgentmarketmessages',
        ]

        self.pullData(names)

        indicators = ["Physical Flow", "Allocation", "Firm Technical"]
        self.pullOperationalData(indicators)

        self.create_hypertable_if_not_exists()

    def create_hypertable_if_not_exists(self) -> None:
        for table_name in ["Physical Flow", "Allocation", "Firm Technical"]:
            self.create_single_hypertable_if_not_exists(table_name, "periodfrom")


if __name__ == "__main__":
    logging.basicConfig(level="INFO")
    config = load_config(Path(__file__).parent.parent / "config.yml")

    craw = EntsogCrawler("entsog", config)
    craw.crawl_temporal()
    craw.set_metadata(metadata_info)
