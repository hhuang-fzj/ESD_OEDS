# SPDX-FileCopyrightText: Vassily Aliseyko
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import io
import logging
import zipfile

import pandas as pd
import requests
from sqlalchemy import text

from common.base_crawler import DownloadOnceCrawler, load_config

log = logging.getLogger("iwu")
log.setLevel(logging.INFO)


metadata_info = {
    "schema_name": "iwugebaeudetypen",
    "data_date": "2015-02-10",
    "data_source": "https://www.iwu.de/fileadmin/tools/tabula/TABULA-Analyses_DE-Typology_DataTables.zip",
    "license": "third party usage allowed",
    "description": """IWU German building types. Building types with energy and sanitation metrics attached.
"The usage of the TABULA approach, data and tools in research projects, theses and software applications by third parties is intended and desirable. Only non-exclusive utilisations are possible. A condition for usages of any kind (files, datasets, pictures, ...) is that 'IEE Projects TABULA + EPISCOPE (www.episcope.eu)' is visibly mentioned as the source."
https://www.iwu.de/forschung/gebaeudebestand/tabula/?mkt=&cHash=3d0c076745af29f744b9b8455ea95dee
    """,
    "contact": "",
    "temporal_start": "1800-01-01 00:00:00",
    "temporal_end": "2023-01-01 00:00:00",
    "concave_hull_geometry": None,
}

TABULA_URL = "https://www.iwu.de/fileadmin/tools/tabula/TABULA-Analyses_DE-Typology_DataTables.zip"

COLUMN_NAMES = [
    "Rechenverfahren",
    "Gebäude_variante_klasse",
    "Gebäude_typ_klasse",
    "Gebäude_typ",
    "Kombination_ID",
    "Baualtersklasse",
    "Gebäude_variante",
    "Heiz_klasse",
    "Tabula_EBZ_m2",
    "Wohnfläche_m2",
    "Wärmetransferkoeffizient_Hüllfläche_W_div_(m2K)",
    "Wärmetransferkoeffizient_Wohnfläche_W_div_(m2K)",
    "Nutzwärme_Nettoheizwärmebedarf_kWh_div_(m2a)",
    "Nutzwärme_Warmwasser_kWh_div_(m2a)",
    "Warmwassererzeugung_Heizung_kWh_div_(m2a)",
    "Warmwassererzeugung_Warmwasser_kWh_div_(m2a)",
    "Endenergiebedarf_fossil_kWh_div_(m2a)",
    "Endenergiebedarf_holz_bio_kWh_div_(m2a)",
    "Endenergiebedarf_strom_kWh_div_(m2a)",
    "Endenergiebedarf_strom_erzeugung_kWh_div_(m2a)",
    "Primärenergiebedarf_gesamt_kWh_div_(m2a)",
    "Primärenergiebedarf_nicht_erneuerbar_kWh_div_(m2a)",
    "Co2_Heizung_ww_kg_div_(m2a)",
    "Energiekosten_Heizung_ww_€_div_(m2a)",
]


def set_sanierungsstand(row):
    variante = row["Gebäude_variante"]
    sanierungsstand = variante[2]
    if sanierungsstand == "1":
        sanierungsstand = "Unsaniert"
    elif sanierungsstand == "2":
        sanierungsstand = "Saniert"
    else:
        sanierungsstand = "Modern"
    return sanierungsstand


def set_heizmittel(row):
    variante = row["Gebäude_variante"]
    heizmittel = variante[1]
    if heizmittel == "0":
        heizmittel = "Gas"
    elif heizmittel == "1":
        heizmittel = "Bio"
    else:
        heizmittel = "Strom"
    return heizmittel


def create_identifier(row):
    baualater = row["Baualtersklasse"]
    verfahren = row["Rechenverfahren"]

    baualater = baualater.replace(" ... ", "-")
    baualater = baualater.replace("- ...", "")
    baualater = baualater.replace("... -", "")

    verfahren = verfahren.replace(
        "TABULA Berechnungsverfahren / Standardrandbedingungen", "A"
    )
    verfahren = verfahren.replace(
        "TABULA Berechnungsverfahren / korrigiert auf Niveau von Verbrauchswerten",
        "B",
    )

    # Construct identifier value
    identifier = (
        row["Gebäude_typ_klasse"]
        + "_"
        + baualater
        + "_"
        + row["Sanierungsstand"]
        + "_"
        + row["Heizklasse"]
        + "_"
        + verfahren
    )

    return identifier


def handle_dates(iwu_data):
    datecol = iwu_data[iwu_data.columns[5]]
    # Extract starting year
    fromcol = datecol.str.extract(r"(\d{4})", expand=False)
    fromcol = fromcol.replace("1859", "1800")
    fromcol = pd.to_datetime(fromcol, format="%Y")
    # Extract ending year
    untilcol = datecol.str.extract(r"(\d{4})$", expand=False)
    untilcol = untilcol.fillna("2023")
    untilcol = pd.to_datetime(untilcol, format="%Y")
    # split date into 2 columns by ...
    iwu_data.insert(5, "Baualtersklasse_von", fromcol)
    iwu_data.insert(6, "Baualtersklasse_bis", untilcol)


class IwuCrawler(DownloadOnceCrawler):
    def structure_exists(self) -> bool:
        try:
            query = text("SELECT 1 from iwu_typgebäude limit 1")
            with self.engine.connect() as conn:
                return conn.execute(query).scalar() == 1
        except Exception:
            return False

    def crawl_structural(self, recreate: bool = False):
        if not self.structure_exists() or recreate:
            log.info("Crawling IWU")

            data = self.pull_data()
            with self.engine.begin() as conn:
                data.to_sql("iwu_typgebäude", conn, if_exists="replace")

            log.info("Finished writing IWU to Database")

    def pull_data(self):
        response = requests.get(TABULA_URL)
        response.raise_for_status()
        # load, read and close the zip file
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            with z.open("TABULA-Analyses_DE-Typology_ResultData.xlsx") as f:
                iwu_data = pd.read_excel(f, sheet_name="DE Tables & Charts")
        # Drop unrelated columns, rows and assign column names
        iwu_data.drop(columns=iwu_data.columns[75:], inplace=True)
        iwu_data.drop(columns=iwu_data.columns[0:51], inplace=True)
        iwu_data.drop(range(13), inplace=True)
        iwu_data.ffill(inplace=True)
        iwu_data.bfill(inplace=True)

        iwu_data.columns = COLUMN_NAMES

        iwu_data["Sanierungsstand"] = iwu_data.apply(set_sanierungsstand, axis=1)
        iwu_data["Heizklasse"] = iwu_data.apply(set_heizmittel, axis=1)
        iwu_data["IWU_ID"] = iwu_data.apply(create_identifier, axis=1)

        handle_dates(iwu_data)

        # fill nan & reset index
        iwu_data.reset_index(drop=True, inplace=True)
        return iwu_data


if __name__ == "__main__":
    logging.basicConfig()
    from pathlib import Path

    config = load_config(Path(__file__).parent.parent / "config.yml")
    craw = IwuCrawler("iwu", config)
    craw.crawl_structural(recreate=False)
