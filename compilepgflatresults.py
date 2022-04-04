import logging
import csv
import uuid
import json
import os
import sys
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
from itertools import islice
from contextlib import contextmanager
from enum import Enum
from argparse import ArgumentParser
from glob import glob
from tempfile import TemporaryDirectory
from sqlalchemy import *


class FluxSource(Enum):
    
    Disturbance   = 1
    AnnualProcess = 2
    Any           = 3
    
    @staticmethod
    def from_string(str):
        return (
            FluxSource.Disturbance if str == "Disturbance"
            else FluxSource.AnnualProcess if str == "Annual Process"
            else FluxSource.Any
        )


class FluxIndicator:

    def __init__(self, name, from_pools, to_pools, flux_source):
        self.name = name
        self.from_pools = tuple(from_pools)
        self.to_pools = tuple(to_pools)
        self.flux_source = flux_source


def table_exists(conn, schema, table):
    return conn.execute(text(
        """
        SELECT EXISTS (
            SELECT FROM pg_tables
            WHERE schemaname = :schema
                AND tablename  = :table
        )
        """), schema=schema, table=table).fetchone()[0]


def copy_reporting_tables(from_conn, from_schema, to_conn, to_schema=None):
    for sql in (
        "PRAGMA main.page_size=4096;",
        "PRAGMA main.cache_size=250000;",
        "PRAGMA synchronous=OFF"
    ):
        to_conn.execute(sql)
    
    md = MetaData()
    md.reflect(bind=from_conn, schema=from_schema)
    with to_conn.begin():
        for fqn, table in md.tables.items():
            for pg_data in pd.read_sql_table(table.name, from_conn, from_schema, chunksize=10000):
                pg_data.to_sql(table.name, to_conn, if_exists="append")
        

def read_flux_indicators(indicator_config):
    pool_collections = indicator_config["pool_collections"]
    fluxes = [
        FluxIndicator(
            flux, pool_collections[details["from"]], pool_collections[details["to"]],
            FluxSource.from_string(details["source"]))
        for flux, details in indicator_config["fluxes"].items()
    ]

    return fluxes


def chunk(it, size):
    size = size or len(it)
    it = iter(it)
    return iter(lambda: tuple(islice(it, size)), ())


def merge(table, pattern, conn, schema, *sum_cols, chunk_size):
    csv_files = glob(pattern)
    if not csv_files:
        return False

    merge_sql = []
    for i, batch in enumerate(chunk(csv_files, chunk_size)):
        with conn.begin():
            conn.execute(text(f"SET SEARCH_PATH={schema}"))
            batch = list(batch)
            if not merge_sql:
                header = next(csv.reader(open(batch[0])))
                dtypes = [
                    "INTEGER" if c in ("year", "disturbance_code")
                    else "NUMERIC" if c in ("area", "flux_tc", "pool_tc")
                    else "VARCHAR"
                    for c in header
                ]
                
                columns_ddl = [" ".join(item) for item in zip(header, dtypes)]
                conn.execute(text(f"CREATE UNLOGGED TABLE {table} ({','.join(columns_ddl)})"))

                columns_select = [
                    f"CAST({col} AS {dtype}) AS {col}" if col not in sum_cols
                    else f"SUM({col}) AS {col}"
                    for (col, dtype) in zip(header, dtypes)
                ]
                
                columns_groupby = set(header) - set(sum_cols)
                
                merge_sql.append(text(
                    f"""
                    CREATE UNLOGGED TABLE _{table} AS
                    SELECT {','.join(columns_select)}
                    FROM {table}
                    GROUP BY {','.join(columns_groupby)}
                    """))
                    
                merge_sql.append(text(f"DROP TABLE IF EXISTS {table}"))
                merge_sql.append(text(f"ALTER TABLE IF EXISTS _{table} RENAME TO {table}"))
                
            for j, file in enumerate(batch):
                conn.execute(text(f"COPY {table} FROM '{file}' WITH (FORMAT CSV, HEADER TRUE)"))
            
            for sql in merge_sql:
                conn.execute(sql)
    
    return True


def compile_flux_indicators(conn, schema, indicator_config, classifiers):
    flux_indicators = read_flux_indicators(indicator_config)
    classifiers_ddl = " VARCHAR, ".join(classifiers)
    classifiers_select = ",".join(classifiers)

    with conn.begin():
        conn.execute(text(f"SET SEARCH_PATH={schema}"))
        conn.execute(
            f"""
            CREATE UNLOGGED TABLE IF NOT EXISTS v_flux_indicators (
                indicator VARCHAR, year INTEGER, {classifiers_ddl} VARCHAR, unfccc_land_class VARCHAR,
                age_range VARCHAR, disturbance_type VARCHAR, disturbance_code INTEGER, flux_tc NUMERIC)
            """)
        
        for flux in flux_indicators:
            disturbance_filter = (
                "disturbance_type IS NOT NULL AND disturbance_type <> ''"
                    if flux.flux_source == FluxSource.Disturbance
                else "disturbance_type IS NULL OR disturbance_type = ''"
                    if flux.flux_source == FluxSource.AnnualProcess
                else "1=1"
            )
        
            sql = \
                f"""
                INSERT INTO v_flux_indicators (
                    indicator, year, {classifiers_select}, unfccc_land_class, age_range,
                    disturbance_type, disturbance_code, flux_tc)
                SELECT :name AS indicator, year, {classifiers_select}, unfccc_land_class,
                    age_range, disturbance_type, disturbance_code, SUM(flux_tc) AS flux_tc
                FROM raw_fluxes
                WHERE {disturbance_filter}
                    AND from_pool IN :from_pools
                    AND to_pool IN :to_pools
                GROUP BY year, {classifiers_select}, unfccc_land_class, age_range,
                    disturbance_type, disturbance_code
                """
                
            conn.execute(text(sql), name=flux.name, from_pools=flux.from_pools, to_pools=flux.to_pools)
        

def compile_flux_indicator_aggregates(conn, schema, indicator_config, classifiers):
    classifiers_ddl = " VARCHAR, ".join(classifiers)
    classifiers_select = ",".join(classifiers)

    with conn.begin():
        conn.execute(text(f"SET SEARCH_PATH={schema}"))
        conn.execute(text(
            f"""
            CREATE UNLOGGED TABLE IF NOT EXISTS v_flux_indicator_aggregates (
                indicator VARCHAR, year INTEGER, {classifiers_ddl} VARCHAR,
                unfccc_land_class VARCHAR, age_range VARCHAR, flux_tc NUMERIC)
            """))
        
        for name, flux_indicators in indicator_config["flux_collections"].items():
            conn.execute(text(
                f"""
                INSERT INTO v_flux_indicator_aggregates (
                    indicator, year, {classifiers_select}, unfccc_land_class, age_range,
                    flux_tc)
                SELECT
                    :name AS indicator, year, {classifiers_select}, unfccc_land_class,
                    age_range, SUM(flux_tc) AS flux_tc
                FROM v_flux_indicators
                WHERE indicator IN :flux_indicators
                GROUP BY year, {classifiers_select}, unfccc_land_class, age_range
                """), name=name, flux_indicators=tuple(flux_indicators))


def compile_stock_change_indicators(conn, schema, indicator_config, classifiers):
    classifiers_ddl = " VARCHAR, ".join(classifiers)
    classifiers_select = ",".join(classifiers)

    with conn.begin():
        conn.execute(text(f"SET SEARCH_PATH={schema}"))
        conn.execute(text(
            f"""
            CREATE UNLOGGED TABLE IF NOT EXISTS v_stock_change_indicators (
                indicator VARCHAR, year INTEGER, {classifiers_ddl} VARCHAR,
                unfccc_land_class VARCHAR, age_range VARCHAR, flux_tc NUMERIC)
            """))

        for name, components in indicator_config["stock_changes"].items():
            query_params = {"name": name}
            unique_aggregates = set()

            add_sub_sql = []
            for i, (sign, flux_aggregates) in enumerate(components.items()):
                mult = 1 if sign == '+' else -1 if sign == '-' else 'err'
                add_sub_sql.append(f"WHEN indicator IN :fluxes_{i} THEN {mult}")
                unique_aggregates.update(flux_aggregates)
                query_params[f"fluxes_{i}"] = tuple(flux_aggregates)
            
            query_params["unique_aggregates"] = tuple(unique_aggregates)
            
            conn.execute(text(
                f"""
                INSERT INTO v_stock_change_indicators (
                    indicator, year, {classifiers_select}, unfccc_land_class,
                    age_range, flux_tc)
                SELECT
                    :name AS indicator, year, {classifiers_select}, unfccc_land_class, age_range,
                    SUM(flux_tc * CASE {' '.join(add_sub_sql)} END) AS flux_tc
                FROM v_flux_indicator_aggregates
                WHERE indicator IN :unique_aggregates
                GROUP BY year, {classifiers_select}, unfccc_land_class, age_range
                """), **query_params)


def compile_pool_indicators(conn, schema, indicator_config, classifiers):
    classifiers_ddl = " VARCHAR, ".join(classifiers)
    classifiers_select = ",".join(classifiers)

    with conn.begin():
        conn.execute(text(f"SET SEARCH_PATH={schema}"))
        conn.execute(text(
            f"""
            CREATE UNLOGGED TABLE IF NOT EXISTS v_pool_indicators (
                indicator VARCHAR, year INTEGER, {classifiers_ddl} VARCHAR,
                unfccc_land_class VARCHAR, age_range VARCHAR, pool_tc NUMERIC)
            """))
        
        for name, pool_collection in indicator_config["pool_indicators"].items():
            pools = tuple(indicator_config["pool_collections"][pool_collection])
            conn.execute(text(
                f"""
                INSERT INTO v_pool_indicators (
                    indicator, year, {classifiers_select}, unfccc_land_class,
                    age_range, pool_tc)
                SELECT
                    :name AS indicator, year, {classifiers_select}, unfccc_land_class,
                    age_range, SUM(pool_tc)
                FROM raw_pools
                WHERE pool IN :pools
                    AND year > 0
                GROUP BY year, {classifiers_select}, unfccc_land_class, age_range
                """), name=name, pools=pools)


def create_views(output_db):
    output_db_engine = create_engine(f"sqlite:///{output_db}")
    conn = output_db_engine.connect()
    
    raw_flux_cols = {f'"{c}"' for c in (
        {c for c in conn.execute("SELECT * FROM raw_fluxes LIMIT 1").keys()}
        - {"age_range", "age_range_previous"})}
    
    raw_dist_cols = {f'"{c}"' for c in (
        {c for c in conn.execute("SELECT * FROM raw_fluxes LIMIT 1").keys()}
        - {"age_range", "age_range_previous", "flux_tc", "from_pool", "to_pool"})}
    
    with conn.begin():
        for sql in (
            "CREATE VIEW IF NOT EXISTS v_age_indicators AS SELECT * FROM raw_ages WHERE year > 0",
            "CREATE VIEW IF NOT EXISTS v_error_indicators AS SELECT * FROM raw_errors",
            f"""
            CREATE VIEW IF NOT EXISTS v_disturbance_fluxes AS
            SELECT
                {','.join(raw_flux_cols)},
                age_range_previous AS pre_dist_age_range,
                age_range AS post_dist_age_range
            FROM raw_fluxes
            WHERE disturbance_type IS NOT NULL
            """,
            f"""
            CREATE VIEW IF NOT EXISTS v_disturbance_indicators AS
            SELECT
                {','.join((f'd.{c}' for c in raw_dist_cols))},
                d.age_range_previous AS pre_dist_age_range,
                d.age_range AS post_dist_age_range,
                d.area AS dist_area,
                f.flux_tc AS dist_carbon,
                f.flux_tc / d.area AS dist_carbon_per_ha
            FROM raw_disturbances d
            INNER JOIN (
                SELECT
                    {','.join(raw_dist_cols)},
                    age_range_previous AS pre_dist_age_range,
                    age_range AS post_dist_age_range,
                    SUM(flux_tc) AS flux_tc
                FROM raw_fluxes
                WHERE disturbance_type IS NOT NULL
                GROUP BY {','.join(raw_dist_cols)}
            ) AS f
            ON {' AND '.join((f'd.{c} = f.{c}' for c in raw_dist_cols))}
            """,
            f"""
            CREATE VIEW IF NOT EXISTS v_total_disturbed_areas AS
            SELECT
                {','.join(raw_dist_cols)},
                SUM(area) AS dist_area
            FROM raw_disturbances
            GROUP BY {','.join(raw_dist_cols)}
            """,
        ):
            conn.execute(text(sql))


def compile_gcbm_output(title, conn_str, results_path, output_db, indicator_config_file=None,
                        chunk_size=1000, drop_schema=False):
    
    output_dir = os.path.dirname(output_db)
    os.makedirs(output_dir, exist_ok=True)
    
    if os.path.exists(output_db):
        os.remove(output_db)

    results_schema = title.lower()
    
    # Create the reporting tables in the simulation output schema.
    results_db_engine = create_engine(conn_str)
    with results_db_engine.connect() as conn:
        conn = conn.execution_options(stream_results=True, max_row_buffer=100000)

        if drop_schema:
            conn.execute(text(f"DROP SCHEMA IF EXISTS {results_schema} CASCADE"))
        
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {results_schema}"))
        conn.execute(text(f"SET SEARCH_PATH={results_schema}"))

        if (not table_exists(conn, results_schema, "raw_ages")
            and not merge("raw_ages", os.path.join(results_path, "age_*.csv"), conn,
                          results_schema, "area", chunk_size=chunk_size)
        ):
            return
        
        classifier_names = {col for col in conn.execute(text("SELECT * FROM raw_ages LIMIT 1")).keys()} \
            - {"year", "unfccc_land_class", "age_range", "area"}

        classifiers = [f'"{c}"' for c in classifier_names]
        
        indicators = json.load(open(
            indicator_config_file
            or os.path.join(os.path.dirname(__file__), "compileresults.json")))
        
        if not table_exists(conn, results_schema, "raw_disturbances"):
            merge("raw_disturbances", os.path.join(results_path, "disturbance_*.csv"),
                  conn, results_schema, "area", chunk_size=chunk_size)

        if (table_exists(conn, results_schema, "raw_fluxes")
            or merge("raw_fluxes", os.path.join(results_path, "flux_*.csv"), conn,
                     results_schema, "flux_tc", chunk_size=chunk_size)
        ):
            compile_flux_indicators(conn, results_schema, indicators, classifiers)
            compile_flux_indicator_aggregates(conn, results_schema, indicators, classifiers)
            compile_stock_change_indicators(conn, results_schema, indicators, classifiers)

        if (table_exists(conn, results_schema, "raw_pools")
            or merge("raw_pools", os.path.join(results_path, "pool_*.csv"), conn,
                     results_schema, "pool_tc", chunk_size=chunk_size)
        ):
            compile_pool_indicators(conn, results_schema, indicators, classifiers)

        if not table_exists(conn, results_schema, "raw_errors"):
            merge("raw_errors", os.path.join(results_path, "error_*.csv"), conn,
                  results_schema, "area", chunk_size=chunk_size)

        # Export the reporting tables to SQLite.
        output_db_engine = create_engine("sqlite:///{}".format(output_db))
        with output_db_engine.connect() as output_conn:
            copy_reporting_tables(conn, results_schema, output_conn)
    
    del output_db_engine
    del results_db_engine
    conn = None
    output_conn = None

    create_views(output_db)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, stream=sys.stdout, format="%(asctime)s %(message)s",
                        datefmt="%m/%d %H:%M:%S")

    parser = ArgumentParser(description="Produce reporting tables from GCBM CSV results.")

    parser.add_argument("title")
    parser.add_argument("results_conn_str",   help="SQLAlchemy connection string to Postgres database")
    parser.add_argument("results_path",       help="path to CSV output files", type=os.path.abspath)
    parser.add_argument("output_db",          help="path to the database to write results tables to", type=os.path.abspath)
    parser.add_argument("--indicator_config", help="indicator configuration file - defaults to a generic set")
    parser.add_argument("--chunk_size",       help="number of CSV files to merge at a time", type=int, default=1000)
    args = parser.parse_args()
    
    compile_gcbm_output(args.title, args.results_conn_str, args.results_path, args.output_db, args.indicator_config, args.chunk_size, True)
