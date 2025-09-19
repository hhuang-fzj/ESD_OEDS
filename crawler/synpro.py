# SPDX-FileCopyrightText: Christoph Komanns
#
# SPDX-License-Identifier: AGPL-3.0-or-later

from common.base_crawler import DownloadOnceCrawler, load_config

from sqlalchemy import text

metadata_info = {
    "schema_name": "synpro_free_load_profiles",
    "data_date": "",
    "data_source": "",
    "license": "CDLA-permissive 2.0",
    "description": """15 minütlich aufgelöste Zeitreihen für einen Zeitraum von einem Jahr für eine Auswahl an 4 verschiedenen Haushalten.
    In diesem Beispieldatensatz sind drei verschiedene Ordner zu finden aus denen verschiedene Gebäudekonstellationen erstellt werden können. Es gibt 4 elektrische Lastprofile mit den folgenden sozioökonomischen Faktoren:
        * Eine Familie mit zwei Eltern und zwei Kindern
        * Zwei Vollzeitarbeitende
        * Zwei Personen über 65 (Rentner*innen)
        * Eine Person unter 30
    Dazu gibt es Heizlastprofile sowie Trinkwarmwasserprofile für zwei Gebäude mit jeweils zwei verschiedenen Isolationsstandards:
        * Einfamilienhaus im Passivhausstandard
        * Einfamilienhaus mit Baujahr zwischen 1979 und 2001
        * Mehrfamilienhaus im Passivhausstandard
        * Mehrfamilienhaus mit Baujahr zwischen 1979 und 2001
    Die Profile können entweder einzeln verwendet oder kombiniert werden. Die empfohlenen Kombinationen sind:
        * Das elektrische Lastprofil für die Familie in Kombinationen mit dem Heiz- und Trinkwarmwasserprofil für ein beliebiges Einfamilienhaus
        * Alle elektrischen Lastprofile zusammen mit dem Heiz- und Trinkwarmwasserprofil für ein beliebiges Mehrfamilienhaus""",
    "contact": "komanns@fh-aachen.de",
    "temporal_start": "2021-01-01 00:00:00",
    "temporal_end": "2021-12-31 23:45:00",
    "concave_hull_geometry": None,
}

class SynproLoadProfileCrawler(DownloadOnceCrawler):
    def structure_exists(self):
        try:
            query = text("SELECT 1 FROM synpro_load_profiles.family LIMIT 1")
            with self.engine.connect() as conn:
                return conn.execute(query).scalar() == 1
        except Exception:
            return False
        
    def crawl_structural(self, recreate = False):
        if not self.structure_exists() or recreate:
            pass