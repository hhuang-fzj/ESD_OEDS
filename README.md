<!--
SPDX-FileCopyrightText: Florian Maurer, Christian Rieke

SPDX-License-Identifier: AGPL-3.0-or-later
-->

# Open Energy Data Server

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.10607894.svg)](https://doi.org/10.5281/zenodo.10607894)

This is a repository that contains python web-crawler scripts to download various available data, which is useful for simulation or analysis of Energy Systems.

The main target is to create an institute-wide available database that can be set up once and then be used by multiple researchers.

Allowing native access through PostgreSQL allows any easy integration of different software which can access data from a SQL database.

For an interactive Documentation, please visit the [Read the Docs Page](https://open-energy-data-server.readthedocs.io/en/latest/index.html).

![Basic outline of the architecture and included services](docs/source/media/oeds-architecture.png)


## Getting started

To set up your institutes new open-data server, you can [install docker](https://docs.docker.com/engine/install/) or [podman](https://podman.io/).
And start the `compose.yml` with `docker compose up -d`.

Then you have a running TimescaleDB server listening on postgresql default port `5432`.

![Visualization of OEDS Usage Workflow](docs/source/media/oeds-workflow.png)

As seen in the above workflow outline, the data is inserted by scripts which retrieve the data from a source API.
This is the core part, afterwards, everything is basically usable.

To execute the scripts, you need a python environment.
As of June 2024 - this works with Python versions 3.9 up to 3.12
You can install all python dependencies:

`pip install -r requirements.txt`

And finally run the main crawling script `python crawl_all.py` to download all available sources into the database.

## Using the ECMWF crawler

If you want to use the ECMWF crawler you need to create an account at [copernicus](https://cds.climate.copernicus.eu) to get an API key which allows you to query the API of copernicus. Follow the [instructions](https://cds.climate.copernicus.eu/api-how-to) of copernicus for that.

## TimeScaleDB

The used database technology for the database server is [TimescaleDB](https://timescale.com/) which is an extension for PostgreSQL (just like PostGIS but for timeseries databases).

### What is a time-series database?
Normal SQL tables can get quite slow if millions of entries are stored in them.

Luckily, timeseries data has the property of always having a separation at the time column.
This can be used for sharding of the database table.

Popular systems like InfluxDB are using this to improve queries with data aggregation or long-time history analysis.
Unfortunately, such databases do not allow storing data without a time column.
For example metadata or lists of existing power plants.

To be able to use both, TimeScaleDB seemed to be the best candidate.
The Grafana integration works also very well and clients can work with it, just like with every PostgreSQL server, without having a new query language to learn (like Flux for example).

### Replication
TimescaleDB allows having replication across multiple servers for load balancing and improvements for reading (and sometimes writing) timeseries data.
This works by using [Distributed Hypertables](https://docs.timescale.com/timescaledb/latest/how-to-guides/distributed-hypertables).

On a high level this can be imagined that for a query spanning a year, each of the three nodes calculates and aggregates the query result for 4 months - resulting in a higher performance.
This only works for timeseries tables and is not compatible with non-timeseries data.
Therefore to increase replication of other tables (like the Marktstammdatenregister), one still needs to have manual replication or use something like [Patroni](https://patroni.readthedocs.io/en/latest/).

## PostGIS
The database server also includes the [PostGIS](https://postgis.net/) extension which allows for spatial queries and storage of geospatial data.
PostGIS is installed once per database and can be used by every schema afterwards.

### What is a geospatial database?
Geospatial databases are optimized for storing and querying geospatial data.
They can store points, lines, polygons, and other geospatial data types and can perform spatial queries like finding all points within a certain distance of a given point.
Coordinate transformations and other geospatial operations are also possible with PostGIS.

## Contributing

Do you know of other interesting open-access databases which are worth mentioning here?
Maybe some are too volatile, large or unknown and are therefore not useful to store in the [OEP](https://openenergy-platform.org/).

Just send a PR and add a new file in the crawler folder with your implemented `ContinuousCrawler` or `DownloadOnceCrawler` and add it to the repository.

## Citation

You can cite the `open-energy-data-server` through the Conference proceedings:

> Maurer, F., Sejdija, J., & Sander, V. (2024, February 2). Decentralized energy data storages through an Open Energy Database Server. 1st NFDI4Energy Conference (NFDI4Energy), Hanover, Germany. https://doi.org/10.5281/zenodo.10607895

## Using the ECMWF crawler

If you want to use the ECMWF crawler you need to create an account at [copernicus](https://cds.climate.copernicus.eu) to get an API key which allows you to query the API of copernicus. Follow the [instructions](https://cds.climate.copernicus.eu/api-how-to) of copernicus for that.
