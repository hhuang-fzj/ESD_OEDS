# SPDX-FileCopyrightText: OEDS Contributors
#
# SPDX-License-Identifier: AGPL-3.0-or-later

from crawler.chargepoint import ChargepointDownloader
from crawler.e2watch import E2WatchCrawler
from crawler.eon_grid_fees import EonGridFeeCrawler
from crawler.eview import EViewCrawler
from crawler.fernwaerme_preisuebersicht import FWCrawler
from crawler.frequency import FrequencyCrawler
from crawler.gie_crawler import GieCrawler
from crawler.instrat_pl import InstratPlCrawler
from crawler.jrc_idees import JrcIdeesCrawler
from crawler.ladesaeulenregister import LadesaeulenregisterCrawler
from crawler.londondatastore import LondonLoadData
from crawler.mastr import MastrDownloader
from crawler.ninja import NinjaCrawler
from crawler.nuts_mapper import NutsCrawler
from crawler.opec import OpecDownloader
from crawler.opsd import OpsdCrawler
from crawler.smard import SmardCrawler
from crawler.vea_industrial_load_profiles import IndustrialLoadProfileCrawler
from crawler.windmodel import WindTurbineCrawler

crawlers = {
    "chargepoint": ChargepointDownloader,
    "e2watch": E2WatchCrawler,
    "eon_grid_fees": EonGridFeeCrawler,
    "eview": EViewCrawler,
    "fernwaerme_preisuebersicht": FWCrawler,
    "frequency": FrequencyCrawler,
    "gie": GieCrawler,
    "instrat_pl": InstratPlCrawler,
    "jrc_idees": JrcIdeesCrawler,
    "ladesaeulenregister": LadesaeulenregisterCrawler,
    "londondatastore": LondonLoadData,
    "mastr": MastrDownloader,
    "ninja": NinjaCrawler,
    "opec": OpecDownloader,
    "opsd": OpsdCrawler,
    "public": NutsCrawler,
    "smard": SmardCrawler,
    "vea_industrial_load_profiles": IndustrialLoadProfileCrawler,
    "windmodel": WindTurbineCrawler,
}
