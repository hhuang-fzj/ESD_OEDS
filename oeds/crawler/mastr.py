# SPDX-FileCopyrightText: Florian Maurer, Christian Rieke
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import io
import logging
import zipfile

import pandas as pd
import requests
from sqlalchemy import text

from oeds.base_crawler import DEFAULT_CONFIG_LOCATION, DownloadOnceCrawler, load_config

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


def get_mastr_url():
    # taken from https://www.marktstammdatenregister.de/MaStR/Datendownload
    # Objektmodell:
    # https://www.marktstammdatenregister.de/MaStRHilfe/files/webdienst/Objektmodell%20-%20Fachliche%20Ansicht%20V1.2.0.pdf
    # Dokumentation statische Katalogwerte:
    # https://www.marktstammdatenregister.de/MaStRHilfe/files/webdienst/Funktionen_MaStR_Webdienste_V23.2.112.html
    # Dynamische Katalogwerte sind in Tabelle "Katalogkategorien" und "Katalogwerte"
    base_url = "https://download.marktstammdatenregister.de/Gesamtdatenexport"

    response = requests.get(
        "https://www.marktstammdatenregister.de/MaStR/Datendownload"
    )
    html_site = response.content.decode("utf-8")
    begin = html_site.find(base_url)
    if begin == -1:
        raise Exception("Error while collecting data from MaStR")

    end = html_site.find('"', begin)
    return html_site[begin:end]


def get_data_from_mastr(data_url):
    response = requests.get(data_url)

    with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
        for info in zip_file.infolist():
            with zip_file.open(info) as file:
                yield file, info


id_fields = [
    "MastrNummer",
    "EinheitMastrNummer",
    "EegMastrNummer",
    "KwkMastrNummer",
    "NetzanschlusspunktMastrNummer",
    "Id",
    "GenMastrNummer",
    "MarktateurMastrNummer",  # GeloeschteUndDeaktivierteMarktakteure
]


def set_index(data_):
    # Mastr should always be lowercase to avoid confusion
    new_cols = list(data_.columns.copy())
    for i in range(len(new_cols)):
        new_cols[i] = new_cols[i].replace("MaStR", "Mastr")
    data_.columns = new_cols

    for field in id_fields:
        if field in data_.columns:
            # only one field can be index
            data_.set_index(field)
            return field


def create_db_from_export(connection):
    tables = {}

    data_url = get_mastr_url()
    log.info(f"get data from MaStR with url {data_url}")
    for file, info in get_data_from_mastr(data_url):
        log.info(f"read file {info.filename}")
        if info.filename.endswith(".xml"):
            table_name = info.filename[0:-4].split("_")[0]
            df = pd.read_xml(file.read(), encoding="utf-16le")
            pk = set_index(df)

            # parse date if possible
            for column in df.columns:
                if "Datum" in column:
                    df[column] = pd.to_datetime(df[column], errors="coerce")

            try:
                # this will fail if there is a new column
                with connection.begin() as conn:
                    df.to_sql(table_name, conn, if_exists="append", index=False)
            except Exception as e:
                log.info(repr(e))
                with connection.begin() as conn:
                    data = pd.read_sql(f'SELECT * FROM "{table_name}"', conn)
                if "level_0" in data.columns:
                    del data["level_0"]
                if "index" in data.columns:
                    del data["index"]
                pk = set_index(data)
                df2 = pd.concat([data, df])
                with connection.begin() as conn:
                    df2.to_sql(
                        name=table_name,
                        con=connection,
                        if_exists="replace",
                        index=False,
                    )

            if table_name not in tables.keys():
                tables[table_name] = pk

    for table_name, pk in tables.items():
        if str(connection.url).startswith("sqlite:/"):
            query = f"CREATE UNIQUE INDEX idx_{table_name}_{pk} ON {table_name}({pk});"
        else:
            query = f'ALTER TABLE "{table_name}" ADD PRIMARY KEY ("{pk}");'
        try:
            with connection.begin() as conn:
                conn.execute(text(query))
        except Exception:
            log.exception("Error adding pk")
    return tables


class MastrDownloader(DownloadOnceCrawler):
    def crawl_structural(self, recreate: bool = False):
        if not self.structure_exists() or recreate:
            create_db_from_export(self.engine)


if __name__ == "__main__":
    logging.basicConfig()

    config = load_config(DEFAULT_CONFIG_LOCATION)
    mastr = MastrDownloader("mastr", config=config)
    mastr.crawl_structural(recreate=False)
