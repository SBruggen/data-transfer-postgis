import argparse
import json
from pathlib import Path
from typing import Optional

import geopandas as gpd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


def load_db_config(path: Path) -> dict:
    cfg = json.loads(path.read_text(encoding="utf-8"))
    required = ["host", "port", "database", "user", "password"]
    missing = [k for k in required if k not in cfg]
    if missing:
        raise ValueError(f"Missing keys in db config: {missing}")
    return cfg


def make_engine(cfg: dict) -> Engine:
    # psycopg2 driver (meest gebruikt met GeoPandas)
    # Alternatief: postgresql+psycopg (psycopg3)
    sslmode = cfg.get("sslmode", "prefer")
    url = (
        f"postgresql+psycopg2://{cfg['user']}:{cfg['password']}"
        f"@{cfg['host']}:{cfg['port']}/{cfg['database']}?sslmode={sslmode}"
    )
    return create_engine(url, future=True)


def ensure_schema(engine: Engine, schema: str) -> None:
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))


def grant_readonly(engine: Engine, schema: str, table: str, roles: list[str]) -> None:
    roles_csv = ", ".join(roles)
    with engine.begin() as conn:
        conn.execute(text(f"GRANT SELECT ON TABLE {schema}.{table} TO {roles_csv}"))


def add_pk(engine: Engine, schema: str, table: str, pk_col: str) -> None:
    # Idempotent PK toevoegen: enkel als er nog geen PK is
    sql = f"""
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint c
    JOIN pg_class t ON t.oid = c.conrelid
    JOIN pg_namespace n ON n.oid = t.relnamespace
    WHERE c.contype = 'p'
      AND n.nspname = '{schema}'
      AND t.relname = '{table}'
  ) THEN
    EXECUTE 'ALTER TABLE {schema}.{table} ADD CONSTRAINT {table}_pkey PRIMARY KEY ({pk_col})';
  END IF;
END $$;
"""
    with engine.begin() as conn:
        conn.execute(text(sql))


def create_gist_index(engine: Engine, schema: str, table: str, geom_col: str) -> None:
    # Idempotent index (naamgeving volgens conventie)
    idx_name = f"{table}_{geom_col}_gist"
    sql = f"CREATE INDEX IF NOT EXISTS {idx_name} ON {schema}.{table} USING GIST ({geom_col})"
    with engine.begin() as conn:
        conn.execute(text(sql))


def main() -> None:
    parser = argparse.ArgumentParser(description="Import GeoPackage layer into PostGIS schema with optional grants.")
    parser.add_argument("--db-config", default= "C:/Users/20004633/OneDrive - PXL/Documenten/GitHub/data_transfer_postgis/db_config.json", help="Path to db_config.json")
    parser.add_argument("--gpkg", required=True, help="Path to .gpkg file")
    parser.add_argument("--layer", required=False, help="Layer name in the GeoPackage (optional if only one layer)")
    parser.add_argument("--schema", required=True, help="Target schema, e.g. proj_beheer_bullfrog")
    parser.add_argument("--table", required=True, help="Target table name, e.g. ref_watervlakken")
    parser.add_argument("--if-exists", choices=["fail", "replace", "append"], default="fail")
    parser.add_argument("--target-epsg", type=int, default=31370, help="Reproject to EPSG (e.g. 31370) before upload")
    parser.add_argument("--pk", default=None, help="Column to use as primary key (optional)")
    parser.add_argument(
        "--grant-select-to",
        default="role_proj_beheer_bullfrog_ro,role_proj_beheer_bullfrog_rw,role_proj_beheer_bullfrog_admin",
        help="Comma-separated roles to GRANT SELECT to (optional)",
    )
    args = parser.parse_args()

    cfg = load_db_config(Path(args.db_config))
    engine = make_engine(cfg)

    gpkg_path = Path(args.gpkg)
    if not gpkg_path.exists():
        raise FileNotFoundError(gpkg_path)

    # Lees laag
    gdf = gpd.read_file(gpkg_path, layer=args.layer) if args.layer else gpd.read_file(gpkg_path)

    if gdf.empty:
        raise ValueError("GeoDataFrame is empty. Check layer selection / file content.")

    # Detecteer geometriekolom
    geom_col = gdf.geometry.name  # dit is precies wat jij als voordeel aangaf

    # CRS check + optioneel reproj
    if args.target_epsg is not None:
        if gdf.crs is None:
            raise ValueError("Input layer has no CRS. Cannot reproject automatically.")
        gdf = gdf.to_crs(epsg=args.target_epsg)

    # Zorg dat schema bestaat
    ensure_schema(engine, args.schema)

    # Upload naar PostGIS
    # GeoPandas maakt de tabel aan als die nog niet bestaat (of replace/append volgens if_exists)
    gdf.to_postgis(
        name=args.table,
        con=engine,
        schema=args.schema,
        if_exists=args.if_exists,
        index=False,          # liever geen pandas index als kolom
        index_label=None,
    )

    # Optioneel: PK toevoegen
    if args.pk:
        add_pk(engine, args.schema, args.table, args.pk)

    # Index op geometry (aanbevolen voor spatial joins)
    create_gist_index(engine, args.schema, args.table, geom_col)

    # Read-only grants voor referentietabel
    roles = [r.strip() for r in args.grant_select_to.split(",") if r.strip()]
    if roles:
        grant_readonly(engine, args.schema, args.table, roles)

    print(f"Imported {args.gpkg} (layer={args.layer or '[auto]'}) → {args.schema}.{args.table}")
    print(f"Geometry column detected: {geom_col}")


if __name__ == "__main__":
    main()
