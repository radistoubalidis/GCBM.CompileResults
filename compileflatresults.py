import vaex
import logging
import json
import os
import sys
import sqlite3
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
        self.from_pools = from_pools
        self.to_pools = to_pools
        self.flux_source = flux_source


@contextmanager
def vaex_open(file):
    data = vaex.open(file)
    try:
        yield data
    finally:
        data.close()
        
        
def read_flux_indicators(indicator_config):
    pool_collections = indicator_config["pool_collections"]
    fluxes = [
        FluxIndicator(
            flux, pool_collections[details["from"]], pool_collections[details["to"]],
            FluxSource.from_string(details["source"]))
        for flux, details in indicator_config["fluxes"].items()
    ]

    return fluxes


def merge(pattern, output_path, *sum_cols):
    csv_data = [vaex.from_csv(csv) for csv in glob(pattern)]
    if not csv_data:
        return False
        
    df = csv_data[0].concat(*csv_data[1:])
    df = df.groupby(set(df.columns) - set(sum_cols), agg={c: "sum" for c in sum_cols})
    df.export_hdf5(output_path)
    df.close()
    
    return True


def vaex_to_table(data, db_path, table_name, *column_overrides, append=False, value_col=None):
    output_db_engine = create_engine(f"sqlite:///{db_path}")
    conn = output_db_engine.connect()
    with conn.begin():
        cols = list(data.columns)
        override_cols = [c.name for c in column_overrides]
    
        md = MetaData()
        table = Table(table_name, md,
            *(Column(c, Text) for c in cols if c not in override_cols),
            *column_overrides
        )
        
        if not append:
            table.drop(output_db_engine, checkfirst=True)
            
        table.create(output_db_engine, checkfirst=True)
        
        if value_col:
            data.select(f"{value_col} != 0")
        
        for _, _, chunk in data.to_records(selection=value_col is not None, chunk_size=10000):
            conn.execute(insert(table), chunk)
        
        if value_col:
            conn.execute(delete(table).where(text(f"{value_col} IS NULL")))


def compile_flux_indicators(merged_flux_data, indicators, output_db):
    flux_indicators = read_flux_indicators(indicators)

    groupby_columns = (set(merged_flux_data.columns)
        - {"flux_tc", "from_pool", "to_pool"}
        - set(c for c in merged_flux_data.columns if c.endswith("previous")))
        
    for flux in flux_indicators:
        if flux.flux_source == FluxSource.Disturbance:
            merged_flux_data.select(~merged_flux_data.disturbance_type.ismissing()
                & merged_flux_data.from_pool.isin(flux.from_pools)
                & merged_flux_data.to_pool.isin(flux.to_pools))
        elif flux.flux_source == FluxSource.AnnualProcess:
            merged_flux_data.select(merged_flux_data.disturbance_type.ismissing()
                & merged_flux_data.from_pool.isin(flux.from_pools)
                & merged_flux_data.to_pool.isin(flux.to_pools))
        else:
            merged_flux_data.select(merged_flux_data.from_pool.isin(flux.from_pools)
                & merged_flux_data.to_pool.isin(flux.to_pools))
        
        flux_data = merged_flux_data.groupby(
            groupby_columns,
            agg={"flux_tc": vaex.agg.sum("flux_tc", selection=True)})
            
        flux_data["indicator"] = vaex.vconstant(flux.name, flux_data.shape[0])
            
        vaex_to_table(flux_data, output_db, "v_flux_indicators",
            Column("year", Integer),
            Column("disturbance_code", Integer),
            Column("flux_tc", Numeric),
            append=True, value_col="flux_tc")


def compile_flux_indicator_aggregates(base_columns, indicator_config, output_db):
    output_db_engine = create_engine(f"sqlite:///{output_db}")
    conn = output_db_engine.connect()
    with conn.begin():
        md = MetaData()
        table = Table("v_flux_indicator_aggregates", md,
            Column("indicator", Text),
            Column("year", Integer),
            *(Column(c, Text) for c in base_columns - {"year"}),
            Column("flux_tc", Numeric)
        )
        
        table.create(output_db_engine, checkfirst=True)

        for name, flux_indicators in indicator_config["flux_collections"].items():
            conn.execute(
                f"""
                INSERT INTO v_flux_indicator_aggregates (indicator, {','.join(base_columns)}, flux_tc)
                SELECT ? AS aggregate_indicator, {','.join(base_columns)}, SUM(flux_tc)
                FROM v_flux_indicators
                WHERE indicator IN ({','.join('?' * len(flux_indicators))})
                GROUP BY {','.join(base_columns)}
                """, [name] + flux_indicators)


def compile_stock_change_indicators(base_columns, indicator_config, output_db):
    output_db_engine = create_engine(f"sqlite:///{output_db}")
    conn = output_db_engine.connect()
    with conn.begin():
        md = MetaData()
        table = Table("v_stock_change_indicators", md,
            Column("indicator", Text),
            Column("year", Integer),
            *(Column(c, Text) for c in base_columns - {"year"}),
            Column("flux_tc", Numeric)
        )
        
        table.create(output_db_engine, checkfirst=True)

        for name, components in indicator_config["stock_changes"].items():
            add_sub_sql = []
            add_sub_params = []
            for sign, flux_aggregates in components.items():
                mult = 1 if sign == '+' else -1 if sign == '-' else 'err'
                add_sub_sql.append(f"WHEN indicator IN ({','.join('?' * len(flux_aggregates))}) THEN {mult}")
                add_sub_params.extend(flux_aggregates)
            
            unique_aggregates = list(set(add_sub_params))
            
            conn.execute(
                f"""
                INSERT INTO v_stock_change_indicators (indicator, {','.join(base_columns)}, flux_tc)
                SELECT
                    ? AS stock_change_indicator,
                    {','.join(base_columns)},
                    SUM(flux_tc * CASE {' '.join(add_sub_sql)} END)
                FROM v_flux_indicator_aggregates
                WHERE indicator IN ({','.join('?' * len(unique_aggregates))})
                GROUP BY {','.join(base_columns)}
                """, [name] + add_sub_params + unique_aggregates)


def compile_pool_indicators(base_columns, indicator_config, output_db):
    output_db_engine = create_engine(f"sqlite:///{output_db}")
    conn = output_db_engine.connect()
    with conn.begin():
        md = MetaData()
        table = Table("v_pool_indicators", md,
            Column("indicator", Text),
            Column("year", Integer),
            *(Column(c, Text) for c in base_columns - {"year"}),
            Column("pool_tc", Numeric)
        )
        
        table.create(output_db_engine, checkfirst=True)

        for name, pool_collection in indicator_config["pool_indicators"].items():
            pools = indicator_config["pool_collections"][pool_collection]
            conn.execute(
                f"""
                INSERT INTO v_pool_indicators (indicator, {','.join(base_columns)}, pool_tc)
                SELECT
                    ? AS pool_indicator,
                    {','.join(base_columns)},
                    SUM(pool_tc)
                FROM raw_pools
                WHERE pool IN ({','.join('?' * len(pools))})
                    AND year > 0
                GROUP BY {','.join(base_columns)}
                """, [name] + pools)


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
            conn.execute(sql)


def compile_gcbm_output(results_path, output_db, indicator_config_file=None):
    output_dir = os.path.dirname(output_db)
    os.makedirs(output_dir, exist_ok=True)
    
    if os.path.exists(output_db):
        os.remove(output_db)

    with TemporaryDirectory(dir=output_dir) as tmp:
        age_output_file = os.path.join(tmp, "age.hdf5")
        if not merge(os.path.join(results_path, "age_*.csv"), age_output_file, "area"):
            logging.info(f"No results to process in {results_path}")
            return
        
        with vaex_open(age_output_file) as data:
            vaex_to_table(data, output_db, "raw_ages",
                Column("year", Integer),
                Column("area", Numeric)
            )

        base_columns = set(vaex.open(age_output_file).columns) - {"area"}
        indicators = json.load(open(
            indicator_config_file
            or os.path.join(os.path.dirname(__file__), "compileresults.json")))
        
        dist_output_file = os.path.join(tmp, "disturbance.hdf5")
        if merge(os.path.join(results_path, "disturbance_*.csv"), dist_output_file, "area"):
            with vaex_open(dist_output_file) as data:
                vaex_to_table(data, output_db, "raw_disturbances",
                    Column("year", Integer),
                    Column("disturbance_code", Integer),
                    Column("area", Numeric)
                )

        flux_output_file = os.path.join(tmp, "flux.hdf5")
        if merge(os.path.join(results_path, "flux_*.csv"), flux_output_file, "flux_tc"):
            with vaex_open(flux_output_file) as data:
                vaex_to_table(data, output_db, "raw_fluxes",
                    Column("year", Integer),
                    Column("disturbance_code", Integer),
                    Column("flux_tc", Numeric)
                )

                compile_flux_indicators(data, indicators, output_db)

            compile_flux_indicator_aggregates(base_columns, indicators, output_db)
            compile_stock_change_indicators(base_columns, indicators, output_db)

        pool_output_file = os.path.join(tmp, "pool.hdf5")
        if merge(os.path.join(results_path, "pool_*.csv"), pool_output_file, "pool_tc"):
            with vaex_open(pool_output_file) as data:
                vaex_to_table(data, output_db, "raw_pools",
                    Column("year", Integer),
                    Column("pool_tc", Numeric)
                )
            
            compile_pool_indicators(base_columns, indicators, output_db)
            
        error_output_file = os.path.join(tmp, "error.hdf5")
        if merge(os.path.join(results_path, "error_*.csv"), error_output_file, "area"):
            with vaex_open(error_output_file) as data:
                vaex_to_table(data, output_db, "raw_errors",
                    Column("year", Integer),
                    Column("area", Numeric)
                )

        create_views(output_db)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, stream=sys.stdout, format="%(asctime)s %(message)s",
                        datefmt="%m/%d %H:%M:%S")

    parser = ArgumentParser(description="Produce reporting tables from GCBM CSV results.")

    parser.add_argument("results_path",       help="path to CSV output files", type=os.path.abspath)
    parser.add_argument("output_db",          help="path to the database to write results tables to", type=os.path.abspath)
    parser.add_argument("--indicator_config", help="indicator configuration file - defaults to a generic set")
    args = parser.parse_args()
    
    compile_gcbm_output(args.results_path, args.output_db, args.indicator_config)
