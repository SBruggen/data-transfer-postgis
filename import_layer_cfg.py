import argparse
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, List, Tuple

import geopandas as gpd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError


# -----------------------------
# Config helpers
# -----------------------------

def load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(path)
    return json.loads(path.read_text(encoding="utf-8"))


def deep_get(d: Dict[str, Any], keys: List[str], default=None):
    cur = d
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def require(d: Dict[str, Any], keys: List[str], label: str):
    v = deep_get(d, keys, None)
    if v is None:
        raise ValueError(f"Missing required config key: {label} ({'.'.join(keys)})")
    return v


def apply_defaults(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """
    Minimal defaults so the template can be trimmed safely.
    """
    cfg.setdefault("version", 1)
    cfg.setdefault("logging", {"level": "INFO", "sql_echo": False})
    cfg.setdefault("safety", {"require_confirm_for_drop": True})

    cfg.setdefault("source", {})
    cfg["source"].setdefault("layer", None)
    cfg["source"].setdefault("target_epsg", 31370)
    cfg["source"].setdefault("geom", {})
    cfg["source"]["geom"].setdefault("source_geom_column", None)
    cfg["source"]["geom"].setdefault("target_geom_column", "geom")
    cfg["source"]["geom"].setdefault("force_geometry_type", None)
    cfg["source"]["geom"].setdefault("drop_z", True)
    cfg["source"]["geom"].setdefault("make_valid", False)

    cfg["source"].setdefault("columns", {})
    cfg["source"]["columns"].setdefault("rename_map", {})
    cfg["source"]["columns"].setdefault("drop", [])
    cfg["source"]["columns"].setdefault("keep_only", None)

    cfg.setdefault("target", {})
    cfg["target"].setdefault("pk", {"enabled": False, "column": None, "source_column": None, "on_conflict": "fail"})
    cfg["target"].setdefault("table_ddl", {
        "mode": "auto",
        "create_if_missing": True,
        "drop_and_recreate": False,
        "column_type_overrides": {},
        "srid": 31370
    })

    cfg.setdefault("audit", {"enabled": False})
    cfg["audit"].setdefault("columns", {
        "created_at": "created_at",
        "created_by": "created_by",
        "updated_at": "updated_at",
        "updated_by": "updated_by"
    })
    cfg["audit"].setdefault("trigger", {
        "schema": "util",
        "function": "set_audit_fields",
        "trigger_name": None,
        "timing": "BEFORE",
        "events": ["INSERT", "UPDATE"]
    })

    cfg.setdefault("indexes", {})
    cfg["indexes"].setdefault("gist_geometry", {"enabled": True, "column": "geom", "name": None})
    cfg["indexes"].setdefault("btree", [])

    cfg.setdefault("permissions", {"enabled": False})
    cfg["permissions"].setdefault("schema_usage_roles", [])
    cfg["permissions"].setdefault("table_grants", [])

    cfg.setdefault("load", {})
    cfg["load"].setdefault("mode", "append")  # append | replace (discouraged)
    cfg["load"].setdefault("batch", {"enabled": False, "chunksize": 5000})
    cfg["load"].setdefault("post_load", {"analyze": True, "vacuum": False})

    cfg.setdefault("staging", {"enabled": False})
    cfg["staging"].setdefault("schema", deep_get(cfg, ["target", "schema"], None))
    cfg["staging"].setdefault("table_suffix", "_stg")
    cfg["staging"].setdefault("upsert", {"enabled": False, "update_columns": "auto"})

    return cfg


def validate_cfg(cfg: Dict[str, Any]) -> None:
    require(cfg, ["db_config_path"], "db_config_path")
    require(cfg, ["source", "gpkg"], "source.gpkg")
    require(cfg, ["target", "schema"], "target.schema")
    require(cfg, ["target", "table"], "target.table")

    pk_enabled = deep_get(cfg, ["target", "pk", "enabled"], False)
    if pk_enabled:
        if not deep_get(cfg, ["target", "pk", "column"], None):
            raise ValueError("target.pk.enabled=true but target.pk.column is missing")
        if not deep_get(cfg, ["target", "pk", "source_column"], None):
            # if not specified, assume same
            cfg["target"]["pk"]["source_column"] = cfg["target"]["pk"]["column"]

    on_conflict = deep_get(cfg, ["target", "pk", "on_conflict"], "fail")
    if on_conflict not in ("fail", "skip", "upsert"):
        raise ValueError("target.pk.on_conflict must be one of: fail, skip, upsert")

    if deep_get(cfg, ["audit", "enabled"], False):
        trig_schema = deep_get(cfg, ["audit", "trigger", "schema"], None)
        trig_func = deep_get(cfg, ["audit", "trigger", "function"], None)
        if not trig_schema or not trig_func:
            raise ValueError("audit.enabled=true but audit.trigger.schema/function missing")

    load_mode = deep_get(cfg, ["load", "mode"], "append")
    if load_mode not in ("append", "replace", "fail"):
        raise ValueError("load.mode must be one of: append, replace, fail")


# -----------------------------
# DB helpers
# -----------------------------

def load_db_config(path: Path) -> dict:
    cfg = json.loads(path.read_text(encoding="utf-8"))
    required = ["host", "port", "database", "user", "password"]
    missing = [k for k in required if k not in cfg]
    if missing:
        raise ValueError(f"Missing keys in db config: {missing}")
    return cfg


def make_engine(cfg: dict, sql_echo: bool = False) -> Engine:
    sslmode = cfg.get("sslmode", "prefer")
    url = (
        f"postgresql+psycopg2://{cfg['user']}:{cfg['password']}"
        f"@{cfg['host']}:{cfg['port']}/{cfg['database']}?sslmode={sslmode}"
    )
    return create_engine(url, future=True, echo=sql_echo)


def schema_exists(engine: Engine, schema: str) -> bool:
    sql = "SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = :s)"
    with engine.begin() as conn:
        return bool(conn.execute(text(sql), {"s": schema}).scalar())


def ensure_schema(engine: Engine, schema: str) -> None:
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))


def table_exists(engine: Engine, schema: str, table: str) -> bool:
    sql = """
    SELECT EXISTS (
      SELECT 1
      FROM information_schema.tables
      WHERE table_schema = :schema
        AND table_name = :table
    )
    """
    with engine.begin() as conn:
        return bool(conn.execute(text(sql), {"schema": schema, "table": table}).scalar())


def column_exists(engine: Engine, schema: str, table: str, column: str) -> bool:
    sql = """
    SELECT EXISTS (
      SELECT 1
      FROM information_schema.columns
      WHERE table_schema = :schema
        AND table_name = :table
        AND column_name = :col
    )
    """
    with engine.begin() as conn:
        return bool(conn.execute(text(sql), {"schema": schema, "table": table, "col": column}).scalar())


def function_exists(engine: Engine, schema: str, function_name: str) -> bool:
    sql = """
    SELECT EXISTS (
      SELECT 1
      FROM pg_proc p
      JOIN pg_namespace n ON n.oid = p.pronamespace
      WHERE n.nspname = :schema
        AND p.proname = :fname
    )
    """
    with engine.begin() as conn:
        return bool(conn.execute(text(sql), {"schema": schema, "fname": function_name}).scalar())


def add_pk_if_missing(engine: Engine, schema: str, table: str, pk_col: str) -> None:
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
    EXECUTE 'ALTER TABLE "{schema}"."{table}" ADD CONSTRAINT "{table}_pkey" PRIMARY KEY ("{pk_col}")';
  END IF;
END $$;
"""
    with engine.begin() as conn:
        conn.execute(text(sql))


def create_gist_index(engine: Engine, schema: str, table: str, geom_col: str, idx_name: Optional[str] = None) -> None:
    idx = idx_name or f"{table}_{geom_col}_gist"
    sql = f'CREATE INDEX IF NOT EXISTS "{idx}" ON "{schema}"."{table}" USING GIST ("{geom_col}")'
    with engine.begin() as conn:
        conn.execute(text(sql))


def create_btree_index(engine: Engine, schema: str, table: str, col: str, idx_name: Optional[str] = None) -> None:
    idx = idx_name or f"{table}_{col}_btree"
    sql = f'CREATE INDEX IF NOT EXISTS "{idx}" ON "{schema}"."{table}" ("{col}")'
    with engine.begin() as conn:
        conn.execute(text(sql))


def grant_schema_usage(engine: Engine, schema: str, roles: List[str]) -> None:
    if not roles:
        return
    roles_csv = ", ".join([f'"{r}"' for r in roles])
    with engine.begin() as conn:
        conn.execute(text(f'GRANT USAGE ON SCHEMA "{schema}" TO {roles_csv}'))


def grant_table_privileges(engine: Engine, schema: str, table: str, grants: List[Dict[str, Any]]) -> None:
    with engine.begin() as conn:
        for g in grants:
            roles = g.get("roles", [])
            privs = g.get("privileges", [])
            if not roles or not privs:
                continue
            roles_csv = ", ".join([f'"{r}"' for r in roles])
            privs_csv = ", ".join(privs)
            conn.execute(text(f'GRANT {privs_csv} ON TABLE "{schema}"."{table}" TO {roles_csv}'))


def ensure_audit_columns(engine: Engine, schema: str, table: str, audit_cols: Dict[str, str]) -> None:
    # Adds columns if missing; does not overwrite existing types.
    col_defs = {
        audit_cols["created_at"]: "timestamptz",
        audit_cols["created_by"]: "text",
        audit_cols["updated_at"]: "timestamptz",
        audit_cols["updated_by"]: "text",
    }
    with engine.begin() as conn:
        for col, typ in col_defs.items():
            conn.execute(text(f'ALTER TABLE "{schema}"."{table}" ADD COLUMN IF NOT EXISTS "{col}" {typ}'))


def ensure_audit_trigger(engine: Engine,
                         schema: str,
                         table: str,
                         trigger_name: str,
                         func_schema: str,
                         func_name: str,
                         timing: str = "BEFORE",
                         events: List[str] = None) -> None:
    events = events or ["INSERT", "UPDATE"]
    events_sql = " OR ".join(events)

    # Create trigger if it doesn't exist.
    sql = f"""
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_trigger
    WHERE tgname = '{trigger_name}'
  ) THEN
    EXECUTE 'CREATE TRIGGER "{trigger_name}" {timing} {events_sql} ON "{schema}"."{table}"
             FOR EACH ROW EXECUTE FUNCTION "{func_schema}"."{func_name}"()';
  END IF;
END $$;
"""
    with engine.begin() as conn:
        conn.execute(text(sql))


def analyze_table(engine: Engine, schema: str, table: str) -> None:
    with engine.begin() as conn:
        conn.execute(text(f'ANALYZE "{schema}"."{table}"'))


# -----------------------------
# Geo helpers
# -----------------------------

def read_source_gpkg(cfg: Dict[str, Any]) -> gpd.GeoDataFrame:
    gpkg = Path(require(cfg, ["source", "gpkg"], "source.gpkg"))
    if not gpkg.exists():
        raise FileNotFoundError(gpkg)

    layer = deep_get(cfg, ["source", "layer"], None)
    gdf = gpd.read_file(gpkg, layer=layer) if layer else gpd.read_file(gpkg)

    if gdf.empty:
        raise ValueError("GeoDataFrame is empty. Check layer selection / file content.")

    # Geometry column detection / override
    src_geom_col = deep_get(cfg, ["source", "geom", "source_geom_column"], None)
    if src_geom_col:
        if src_geom_col not in gdf.columns:
            raise ValueError(f"Configured source geom column '{src_geom_col}' not found in layer.")
        gdf = gdf.set_geometry(src_geom_col)

    # Column ops: keep_only / drop / rename
    keep_only = deep_get(cfg, ["source", "columns", "keep_only"], None)
    if keep_only is not None:
        missing = [c for c in keep_only if c not in gdf.columns]
        if missing:
            raise ValueError(f"keep_only contains missing columns: {missing}")
        gdf = gdf[keep_only]

    drop_cols = deep_get(cfg, ["source", "columns", "drop"], [])
    for c in drop_cols:
        if c in gdf.columns:
            gdf = gdf.drop(columns=[c])

    rename_map = deep_get(cfg, ["source", "columns", "rename_map"], {}) or {}
    # do not rename geometry column implicitly unless explicit mapping provides it
    gdf = gdf.rename(columns=rename_map)

    # CRS / reprojection
    target_epsg = deep_get(cfg, ["source", "target_epsg"], 31370)
    if target_epsg is not None:
        if gdf.crs is None:
            raise ValueError("Input layer has no CRS. Cannot reproject automatically.")
        gdf = gdf.to_crs(epsg=int(target_epsg))

    # drop Z (optional)
    if deep_get(cfg, ["source", "geom", "drop_z"], True):
        # geopandas/shapely: convert to 2D by mapping coords
        try:
            from shapely.ops import transform
            import shapely

            def _to_2d(geom):
                if geom is None or geom.is_empty:
                    return geom

                def _f(x, y, z=None):
                    return (x, y)

                return transform(lambda x, y, z=None: (x, y), geom)

            gdf[gdf.geometry.name] = gdf.geometry.apply(_to_2d)
        except Exception:
            # If shapely transform isn't available, keep geometry as-is.
            pass

    # make_valid (optional, costly)
    if deep_get(cfg, ["source", "geom", "make_valid"], False):
        try:
            gdf[gdf.geometry.name] = gdf.geometry.make_valid()
        except Exception as e:
            raise ValueError(f"make_valid requested but failed: {e}")

    # target geom column name (normalize)
    target_geom_col = deep_get(cfg, ["source", "geom", "target_geom_column"], "geom")
    if gdf.geometry.name != target_geom_col:
        gdf = gdf.rename_geometry(target_geom_col)

    return gdf


def preflight_pk_checks(cfg: Dict[str, Any], gdf: gpd.GeoDataFrame) -> None:
    pk_enabled = deep_get(cfg, ["target", "pk", "enabled"], False)
    if not pk_enabled:
        return

    src_pk = deep_get(cfg, ["target", "pk", "source_column"], None) or deep_get(cfg, ["target", "pk", "column"], None)
    if src_pk not in gdf.columns:
        raise ValueError(f"PK enabled but source PK column '{src_pk}' not found in GeoDataFrame columns.")

    # null check
    if gdf[src_pk].isna().any():
        n = int(gdf[src_pk].isna().sum())
        raise ValueError(f"PK column '{src_pk}' contains {n} NULL values. Fix source data or disable PK.")

    # duplicate check
    if gdf[src_pk].duplicated().any():
        dups = gdf.loc[gdf[src_pk].duplicated(), src_pk].head(10).tolist()
        raise ValueError(f"PK column '{src_pk}' contains duplicates. Example(s): {dups}")


# -----------------------------
# Import logic
# -----------------------------

def prepare_target(engine: Engine, cfg: Dict[str, Any], gdf: Optional[gpd.GeoDataFrame]) -> None:
    schema = require(cfg, ["target", "schema"], "target.schema")
    table = require(cfg, ["target", "table"], "target.table")

    ensure_schema(engine, schema)

    # Drop/recreate protection
    drop_and_recreate = deep_get(cfg, ["target", "table_ddl", "drop_and_recreate"], False)
    if drop_and_recreate:
        if deep_get(cfg, ["safety", "require_confirm_for_drop"], True):
            raise ValueError(
                "Config requests drop_and_recreate=true but safety.require_confirm_for_drop=true. "
                "Set require_confirm_for_drop=false only if you explicitly want destructive behavior."
            )
        with engine.begin() as conn:
            conn.execute(text(f'DROP TABLE IF EXISTS "{schema}"."{table}" CASCADE'))

    exists = table_exists(engine, schema, table)

    # Create table if missing (auto mode)
    create_if_missing = deep_get(cfg, ["target", "table_ddl", "create_if_missing"], True)
    if not exists and create_if_missing:
        if gdf is None:
            raise ValueError("Table does not exist and create_if_missing=true, but no GeoDataFrame was provided.")
        # Use GeoPandas to create the base table
        logging.info("Creating base table via GeoPandas to_postgis (initial create)...")
        gdf.head(0).to_postgis(
            name=table,
            con=engine,
            schema=schema,
            if_exists="fail",
            index=False,
            index_label=None,
        )
        exists = True

    if not exists:
        raise ValueError(f'Target table "{schema}"."{table}" does not exist and create_if_missing=false')

    # Audit columns + trigger
    if deep_get(cfg, ["audit", "enabled"], False):
        func_schema = deep_get(cfg, ["audit", "trigger", "schema"], "util")
        func_name = deep_get(cfg, ["audit", "trigger", "function"], "set_audit_fields")
        if not function_exists(engine, func_schema, func_name):
            raise ValueError(
                f'Audit enabled but function "{func_schema}"."{func_name}"() not found. '
                f"Create it first or set audit.enabled=false."
            )

        audit_cols = deep_get(cfg, ["audit", "columns"], {})
        ensure_audit_columns(engine, schema, table, audit_cols)

        trig_name = deep_get(cfg, ["audit", "trigger", "trigger_name"], None) or f"trg_audit_{table}"
        timing = deep_get(cfg, ["audit", "trigger", "timing"], "BEFORE")
        events = deep_get(cfg, ["audit", "trigger", "events"], ["INSERT", "UPDATE"])
        ensure_audit_trigger(engine, schema, table, trig_name, func_schema, func_name, timing=timing, events=events)

    # PK
    if deep_get(cfg, ["target", "pk", "enabled"], False):
        pk_col = deep_get(cfg, ["target", "pk", "column"], None)
        if not column_exists(engine, schema, table, pk_col):
            raise ValueError(f"PK column '{pk_col}' not found in target table. Check column mapping or table creation.")
        add_pk_if_missing(engine, schema, table, pk_col)

    # Indexes
    if deep_get(cfg, ["indexes", "gist_geometry", "enabled"], True):
        geom_col = deep_get(cfg, ["indexes", "gist_geometry", "column"], "geom")
        if not column_exists(engine, schema, table, geom_col):
            raise ValueError(f"GiST index requested but geometry column '{geom_col}' not found in target table.")
        idx_name = deep_get(cfg, ["indexes", "gist_geometry", "name"], None)
        create_gist_index(engine, schema, table, geom_col, idx_name)

    for idx in deep_get(cfg, ["indexes", "btree"], []) or []:
        if not idx.get("enabled", False):
            continue
        col = idx.get("column")
        if not col:
            continue
        if not column_exists(engine, schema, table, col):
            raise ValueError(f"B-tree index requested but column '{col}' not found in target table.")
        create_btree_index(engine, schema, table, col, idx.get("name"))

    # Permissions
    if deep_get(cfg, ["permissions", "enabled"], False):
        roles_usage = deep_get(cfg, ["permissions", "schema_usage_roles"], []) or []
        grant_schema_usage(engine, schema, roles_usage)

        grants = deep_get(cfg, ["permissions", "table_grants"], []) or []
        grant_table_privileges(engine, schema, table, grants)


def load_data(engine: Engine, cfg: Dict[str, Any], gdf: gpd.GeoDataFrame) -> None:
    schema = require(cfg, ["target", "schema"], "target.schema")
    table = require(cfg, ["target", "table"], "target.table")

    pk_enabled = deep_get(cfg, ["target", "pk", "enabled"], False)
    pk_col = deep_get(cfg, ["target", "pk", "column"], None) if pk_enabled else None
    src_pk = deep_get(cfg, ["target", "pk", "source_column"], pk_col) if pk_enabled else None
    on_conflict = deep_get(cfg, ["target", "pk", "on_conflict"], "fail")

    # Ensure PK column name in dataframe matches target pk column if needed
    if pk_enabled and src_pk and pk_col and src_pk != pk_col:
        if src_pk not in gdf.columns:
            raise ValueError(f"Configured source PK '{src_pk}' not found.")
        gdf = gdf.rename(columns={src_pk: pk_col})

    mode = deep_get(cfg, ["load", "mode"], "append")

    # Strongly discourage replace, but keep option
    if mode == "replace":
        logging.warning("load.mode=replace will DROP/CREATE the table content and may remove constraints/triggers set outside this script.")
    if mode == "fail":
        # fail means: don't load if table already has rows
        sql = f'SELECT EXISTS (SELECT 1 FROM "{schema}"."{table}" LIMIT 1)'
        with engine.begin() as conn:
            has_rows = bool(conn.execute(text(sql)).scalar())
        if has_rows:
            raise ValueError(f'load.mode=fail and target table "{schema}"."{table}" is not empty.')

    # Decide path:
    # - on_conflict=fail: direct to_postgis append/replace
    # - on_conflict=skip/upsert: requires staging.enabled=true to do ON CONFLICT
    if pk_enabled and on_conflict in ("skip", "upsert"):
        if not deep_get(cfg, ["staging", "enabled"], False):
            raise ValueError("target.pk.on_conflict is skip/upsert but staging.enabled=false. Enable staging for ON CONFLICT logic.")
        staging_schema = deep_get(cfg, ["staging", "schema"], schema) or schema
        stg_suffix = deep_get(cfg, ["staging", "table_suffix"], "_stg")
        staging_table = f"{table}{stg_suffix}"

        ensure_schema(engine, staging_schema)

        # Recreate staging table every run (safe)
        with engine.begin() as conn:
            conn.execute(text(f'DROP TABLE IF EXISTS "{staging_schema}"."{staging_table}"'))

        logging.info(f"Creating staging table {staging_schema}.{staging_table} ...")
        gdf.head(0).to_postgis(
            name=staging_table,
            con=engine,
            schema=staging_schema,
            if_exists="fail",
            index=False,
            index_label=None,
        )

        logging.info(f"Loading into staging table {staging_schema}.{staging_table} ...")
        gdf.to_postgis(
            name=staging_table,
            con=engine,
            schema=staging_schema,
            if_exists="append",
            index=False,
            index_label=None,
        )

        # Build INSERT ... ON CONFLICT ...
        cols = list(gdf.columns)
        cols_sql = ", ".join([f'"{c}"' for c in cols])
        select_sql = ", ".join([f's."{c}"' for c in cols])

        if on_conflict == "skip":
            conflict_sql = f'ON CONFLICT ("{pk_col}") DO NOTHING'
        else:
            # upsert: update non-PK columns
            update_cols_cfg = deep_get(cfg, ["staging", "upsert", "update_columns"], "auto")
            if update_cols_cfg == "auto":
                update_cols = [c for c in cols if c != pk_col]
            elif isinstance(update_cols_cfg, list):
                update_cols = [c for c in update_cols_cfg if c != pk_col]
            else:
                raise ValueError("staging.upsert.update_columns must be 'auto' or a list of column names.")

            if not update_cols:
                conflict_sql = f'ON CONFLICT ("{pk_col}") DO NOTHING'
            else:
                set_sql = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in update_cols])
                conflict_sql = f'ON CONFLICT ("{pk_col}") DO UPDATE SET {set_sql}'

        upsert_sql = f"""
INSERT INTO "{schema}"."{table}" ({cols_sql})
SELECT {select_sql}
FROM "{staging_schema}"."{staging_table}" s
{conflict_sql};
"""
        logging.info(f"Applying {on_conflict.upper()} from staging into target ...")
        with engine.begin() as conn:
            conn.execute(text(upsert_sql))

        # cleanup staging (optional: keep for debugging)
        with engine.begin() as conn:
            conn.execute(text(f'DROP TABLE IF EXISTS "{staging_schema}"."{staging_table}"'))

    else:
        # Direct load
        if_exists = "append" if mode in ("append", "fail") else "replace"
        logging.info(f"Loading directly to {schema}.{table} (if_exists={if_exists}) ...")
        gdf.to_postgis(
            name=table,
            con=engine,
            schema=schema,
            if_exists=if_exists,
            index=False,
            index_label=None,
        )

    # Post-load maintenance
    if deep_get(cfg, ["load", "post_load", "analyze"], True):
        analyze_table(engine, schema, table)


# -----------------------------
# CLI
# -----------------------------

def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s"
    )


def cmd_prepare(cfg: Dict[str, Any], engine: Engine) -> None:
    # for create-if-missing we need a gdf schema; read just enough
    gdf = read_source_gpkg(cfg)
    preflight_pk_checks(cfg, gdf)
    prepare_target(engine, cfg, gdf)
    logging.info("Prepare completed.")


def cmd_load(cfg: Dict[str, Any], engine: Engine) -> None:
    gdf = read_source_gpkg(cfg)
    preflight_pk_checks(cfg, gdf)
    # Ensure target is ready (non-destructive)
    prepare_target(engine, cfg, gdf=None)
    load_data(engine, cfg, gdf)
    logging.info("Load completed.")


def cmd_full(cfg: Dict[str, Any], engine: Engine) -> None:
    gdf = read_source_gpkg(cfg)
    preflight_pk_checks(cfg, gdf)
    prepare_target(engine, cfg, gdf)
    load_data(engine, cfg, gdf)
    logging.info("Full import completed.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Import a GeoPackage layer into PostGIS using a JSON config.")
    sub = parser.add_subparsers(dest="command", required=True)

    p_prepare = sub.add_parser("prepare", help="Prepare schema/table (DDL, audit, pk, indexes, grants).")
    p_prepare.add_argument("--config", required=True, help="Path to dataset config JSON.")

    p_load = sub.add_parser("load", help="Load data into an existing prepared table.")
    p_load.add_argument("--config", required=True, help="Path to dataset config JSON.")

    p_full = sub.add_parser("full", help="Prepare + Load (typical).")
    p_full.add_argument("--config", required=True, help="Path to dataset config JSON.")

    args = parser.parse_args()

    cfg_path = Path(args.config)
    cfg = apply_defaults(load_json(cfg_path))
    validate_cfg(cfg)

    setup_logging(deep_get(cfg, ["logging", "level"], "INFO"))

    db_cfg = load_db_config(Path(cfg["db_config_path"]))
    engine = make_engine(db_cfg, sql_echo=deep_get(cfg, ["logging", "sql_echo"], False))

    try:
        if args.command == "prepare":
            cmd_prepare(cfg, engine)
        elif args.command == "load":
            cmd_load(cfg, engine)
        elif args.command == "full":
            cmd_full(cfg, engine)
        else:
            raise ValueError(f"Unknown command: {args.command}")

    except (ValueError, SQLAlchemyError) as e:
        logging.error(str(e))
        raise


if __name__ == "__main__":
    main()
