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
from sqlalchemy import create_engine

from common.base_crawler import create_schema_only, set_metadata_only
from common.config import db_uri

log = logging.getLogger("ladesaeulenregister")
log.setLevel(logging.INFO)

metadata_info = {
    "schema_name": "ladesaeulenregister",
    "data_date": "2014-02-28",
    "data_source": "https://www.bundesnetzagentur.de/DE/Fachthemen/ElektrizitaetundGas/E-Mobilitaet/Ladesaeulenkarte/start.html",
    "license": "CC-BY-4.0",
    "description": "Charging stations for EV. Coordinate referenced power usage of individual chargers.",
    "contact": "",
    "temporal_start": None,
    "temporal_end": None,
}


def main(db_uri):
    engine = create_engine(db_uri)

    create_schema_only(engine, "ladesaeulenregister")

    url = "https://data.bundesnetzagentur.de/Bundesnetzagentur/DE/Fachthemen/ElektrizitaetundGas/E-Mobilitaet/Ladesaeulenregister_BNetzA_2025-07-18.csv"
    df = pd.read_csv(url, skiprows=10, delimiter=";")
    # there were two empty lines at the end
    df = df.dropna(how="all")
    # the PLZ should not be interpreted as a float but be integer
    df["Postleitzahl"] = pd.to_numeric(df["Postleitzahl"], downcast="integer")
    # some entries have whitespace before and after
    df["Längengrad"] = df["Längengrad"].str.replace(",", ".").str.strip()
    df["Längengrad"] = pd.to_numeric(df["Längengrad"])
    # some entries also have an extra delimiter at the end
    df["Breitengrad"] = df["Breitengrad"].str.replace(",", ".").str.strip(" .")
    df["Breitengrad"] = pd.to_numeric(df["Breitengrad"])
    # now conversion works fine

    with engine.begin() as conn:
        df.to_sql("ladesaeulenregister", conn, if_exists="replace")
    log.info("Finished writing Ladesäulenregister to Database")

    set_metadata_only(engine, metadata_info)


if __name__ == "__main__":
    logging.basicConfig()
    main(db_uri("ladesaeulenregister"))
