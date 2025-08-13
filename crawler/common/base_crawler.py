from datetime import date

from sqlalchemy import create_engine, text, Engine
from datetime import datetime, timedelta
from typing import TypedDict
from pathlib import Path

import yaml


class CrawlerConfig(TypedDict):
    db_uri: str
    entsoe_api_key: str
    regelleistung_api_key: str
    gie_api_key: str
    ipnt_client_id: str
    ipnt_client_secret: str


def load_config(config_path: Path | str = "config.yml") -> CrawlerConfig:
    with Path(config_path).open("r") as f:
        config = yaml.safe_load(f)
    return config

class BaseCrawler:
    def __init__(self, schema_name: str, config: CrawlerConfig):
        self.config = config
        if "db_uri" not in config.keys():
            raise ValueError("Please provide a 'db_uri' in the config")
    
        self.config["db_uri"] = config["db_uri"].format(DBNAME=schema_name)
        self.engine = create_engine(self.config["db_uri"]) # type: Engine
        self.create_schema(schema_name)
        

    def create_schema(self, schema_name: str) -> str:
        create_schema_only(self.engine, schema_name)

    def set_metadata(self, metadata_info: dict[str, str]) -> None:
        set_metadata_only(self.engine, metadata_info)


class DownloadOnceCrawler(BaseCrawler):
    def structure_exists(self) -> bool:
        return False

    def crawl_structural(self, recreate: bool=False):
        if not self.structure_exists() or recreate:
            raise NotImplementedError()


class ContinuousCrawler(BaseCrawler):
    """Ideomatic Crawler for temporal data, served for continuous execution of the crawler.

    The idea is to take care of conditional constraints (like end date must be at hour 0) in the crawler.
    All crawlers should fit to this interface, to handle the start and end date well, and also handle download of data prior to the existing data, if exists.
    
    All temporal data should be in UTC.


    Args:
        BaseCrawler (_type_): _description_
    """
    FIRST_DATA=datetime(2019,1,1)
    TIMEDELTA=timedelta(days=-7)
    URL="https://google.de"

    def get_latest_data(self) -> datetime:
        raise NotImplementedError()

    def get_first_data(self) -> datetime:
        raise NotImplementedError()

    def create_hypertable_if_not_exists(self) -> None:
        pass

    def crawl_from_to(self, begin: datetime, end: datetime):
        """Crawls data from begin (inclusive) until end (exclusive)

        Args:
            begin (datetime): included begin datetime from which to crawl
            end (datetime): exclusive end datetime until which to crawl
        """
        pass

    def crawl_temporal(self, begin: datetime | None = None, end: datetime | None = None):
        latest = self.get_latest_data()

        if begin:
            first = self.get_first_data()
            if begin < first:
                self.crawl_from_to(begin, first)
        if not end:
            end = datetime.now()
        
        if latest < end:
            self.crawl_from_to(latest, end)
        self.create_hypertable_if_not_exists()

# ich habe bis 13 Uhr (exklusive)

# ich komme um 13:30 -> ich runde ab auf 13 Uhr und crawle nichts
# ich komme um  14 Uhr -> ich crawle eine Stunde (bis 14 Uhr (exklusive))

#  13 Uhr ------------- 14 Uhr --------------- 15 Uhr


# Daten von 14 bis 15 Uhr

# Ich komme und möchte von 10-17 Uhr:

# -> first data = 14, last_data = 15 Uhr
# -> crawle von 10-14 Uhr (exklusive Ende)
# -> crawle ich von 15 bis 17 Uhr (exklusive Ende)

def create_schema_only(engine: Engine, schema_name: str) -> None:
    if engine.url.drivername.startswith("postgresql"):
        with engine.begin() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))


def set_metadata_only(engine, metadata_info: dict[str, str]):
    for key in ["concave_hull_geometry", "temporal_start", "temporal_end", "contact"]:
        if key not in metadata_info.keys():
            metadata_info[key] = None
    if "data_date" not in metadata_info.keys():
        metadata_info["data_date"] = date.today()
    with engine.begin() as conn:
        conn.execute(
            text("""
            INSERT INTO public.metadata
            (schema_name, data_date, data_source, license, description, contact, concave_hull_geometry, temporal_start, temporal_end)
            VALUES
            (:schema_name, :data_date, :data_source, :license, :description, :contact, :concave_hull_geometry, :temporal_start, :temporal_end)
            ON CONFLICT (schema_name) DO UPDATE SET
                data_date = EXCLUDED.data_date,
                data_source = EXCLUDED.data_source,
                license = EXCLUDED.license,
                description = EXCLUDED.description,
                contact = EXCLUDED.contact,
                concave_hull_geometry = EXCLUDED.concave_hull_geometry,
                temporal_start = EXCLUDED.temporal_start,
                temporal_end = EXCLUDED.temporal_end
            """),
            metadata_info,
        )
        conn.execute(
            text("""
            UPDATE public.metadata
            SET tables = (SELECT COUNT(*) FROM pg_class JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace WHERE nspname = :schema_name AND pg_class.relkind = 'r'),
                size = (SELECT SUM(pg_total_relation_size(pg_class.oid)) FROM pg_class JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace WHERE nspname = :schema_name AND pg_class.relkind = 'r'),
                crawl_date = NOW()
            WHERE schema_name = :schema_name
            """),
            {"schema_name": metadata_info["schema_name"]},
        )
        conn.execute(
            text("""
            NOTIFY pgrst, 'reload schema';
            """)
        )
