import argparse
import json
import re
from pathlib import Path
from typing import Optional, List, Dict, Any

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


# ---------------------------
# DB helpers (zelfde stijl)
# ---------------------------

def load_db_config(path: Path) -> dict:
    cfg = json.loads(path.read_text(encoding="utf-8"))
    required = ["host", "port", "database", "user", "password"]
    missing = [k for k in required if k not in cfg]
    if missing:
        raise ValueError(f"Missing keys in db config: {missing}")
    return cfg


def make_engine(cfg: dict) -> Engine:
    sslmode = cfg.get("sslmode", "prefer")
    # Tip: als je GSS issues hebt, kan je dit toevoegen:
    # + "&gssencmode=disable"
    url = (
        f"postgresql+psycopg2://{cfg['user']}:{cfg['password']}"
        f"@{cfg['host']}:{cfg['port']}/{cfg['database']}?sslmode={sslmode}"
    )
    return create_engine(url, future=True)


def ensure_schema(engine: Engine, schema: str) -> None:
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))


def grant_readonly(engine: Engine, schema: str, table: str, roles: List[str]) -> None:
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


def create_btree_index(engine: Engine, schema: str, table: str, col: str) -> None:
    idx_name = f"{table}_{col}_btree"
    with engine.begin() as conn:
        conn.execute(text(f'CREATE INDEX IF NOT EXISTS "{idx_name}" ON {schema}.{table} ("{col}")'))


# ---------------------------
# Excel-specific utilities
# ---------------------------

_VALID_IDENT = re.compile(r"^[a-z_][a-z0-9_]*$")


def normalize_identifier(name: str) -> str:
    """
    Maak kolomnamen DB-vriendelijk:
    - lowercase
    - spaties/punctuatie -> underscore
    - geen dubbele underscores
    - start niet met cijfer
    """
    s = name.strip().lower()
    s = re.sub(r"[^\w]+", "_", s, flags=re.UNICODE)  # alles wat geen [a-zA-Z0-9_] is -> _
    s = re.sub(r"_+", "_", s).strip("_")
    if not s:
        s = "col"
    if s[0].isdigit():
        s = f"col_{s}"
    return s


def sanitize_dataframe_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rename kolommen zodat ze veilige SQL identifiers worden en unique blijven.
    """
    new_cols: List[str] = []
    seen: Dict[str, int] = {}
    for c in df.columns:
        base = normalize_identifier(str(c))
        candidate = base
        if candidate in seen:
            seen[candidate] += 1
            candidate = f"{candidate}_{seen[base]}"
        else:
            seen[candidate] = 0
        new_cols.append(candidate)
    out = df.copy()
    out.columns = new_cols
    return out


def coerce_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Minimalistische type-coercion:
    - datums: probeer te parsen
    - booleans: 'yes/no', 'true/false', 0/1 blijven meestal ok
    - strings: laat pandas object staan
    """
    out = df.copy()
    for col in out.columns:
        s = out[col]
        # probeer datetime als veel values lijken op datum
        if s.dtype == "object":
            # Heuristiek: als minstens 60% parsebaar als datum -> converteer
            parsed = pd.to_datetime(s, errors="coerce", dayfirst=True)
            ratio = parsed.notna().mean()
            if ratio >= 0.6:
                out[col] = parsed
    return out

def add_id_column(df: pd.DataFrame, id_col: str, start: int = 1) -> pd.DataFrame:
    """
    Voeg een oplopende ID-kolom toe als die nog niet bestaat.
    """
    if id_col in df.columns:
        return df
    out = df.copy()
    out.insert(0, id_col, range(start, start + len(out)))
    return out

# ---------------------------
# Main
# ---------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Import Excel sheet into PostgreSQL schema with optional grants.")
    parser.add_argument(
        "--db-config",
        default="C:/Users/20004633/OneDrive - PXL/Documenten/GitHub/data_transfer_postgis/db_config.json",
        help="Path to db_config.json",
    )
    parser.add_argument("--xlsx", required=True, help="Path to Excel file (.xlsx)")
    parser.add_argument("--sheet", default=None, help="Sheet name (default: first sheet)")
    parser.add_argument("--schema", required=True, help="Target schema, e.g. proj_beheer_bullfrog")
    parser.add_argument("--table", required=True, help="Target table name, e.g. ref_water_extra")
    parser.add_argument("--if-exists", choices=["fail", "replace", "append"], default="fail")
    parser.add_argument("--pk", default=None, help="Column to use as primary key (optional)")
    parser.add_argument("--add-id-col", action="store_true",
                        help="Add an auto-incrementing ID column to the dataframe before upload (if missing).")
    parser.add_argument("--id-col-name", default="id",
                        help="Name of the generated ID column (default: id).")
    parser.add_argument("--id-start", type=int, default=1,
                        help="Start value for generated ID column (default: 1).")
    parser.add_argument("--make-id-pk", action="store_true",
                        help="If --add-id-col is used, also set that ID column as PRIMARY KEY (recommended with replace).")
    parser.add_argument(
        "--index-cols",
        default=None,
        help="Comma-separated columns to create BTREE indexes for (optional). Example: water_id,code",
    )
    parser.add_argument(
        "--grant-select-to",
        default="role_proj_beheer_bullfrog_ro,role_proj_beheer_bullfrog_rw,role_proj_beheer_bullfrog_admin",
        help="Comma-separated roles to GRANT SELECT to (optional)",
    )
    parser.add_argument(
        "--normalize-cols",
        action="store_true",
        help="Normalize column names to lowercase_safe_identifiers before upload.",
    )
    args = parser.parse_args()

    cfg = load_db_config(Path(args.db_config))
    engine = make_engine(cfg)

    xlsx_path = Path(args.xlsx)
    if not xlsx_path.exists():
        raise FileNotFoundError(xlsx_path)

    # Lees Excel
    df = pd.read_excel(xlsx_path, sheet_name=args.sheet)
    if df.empty:
        raise ValueError("Excel dataframe is empty. Check sheet selection / file content.")

    # Kolommen opschonen (sterk aanbevolen voor DB)
    if args.normalize_cols:
        df = sanitize_dataframe_columns(df)

    # Type-coercion (optioneel, maar helpt vaak)
    df = coerce_dtypes(df)

    # ID-kolom toevoegen indien gevraagd
    if args.add_id_col:
        id_col = normalize_identifier(args.id_col_name) if args.normalize_cols else args.id_col_name
        df = add_id_column(df, id_col=id_col, start=args.id_start)

        # Als user geen pk opgeeft en make-id-pk is aan: gebruik id als pk
        if args.make_id_pk and not args.pk:
            args.pk = id_col

    # Zorg dat schema bestaat
    ensure_schema(engine, args.schema)

    # Upload naar Postgres
    # pandas.to_sql maakt tabel aan of append/replace volgens if_exists
    df.to_sql(
        name=args.table,
        con=engine,
        schema=args.schema,
        if_exists=args.if_exists,
        index=False,     # geen pandas index kolom
        method="multi",  # sneller inserts
        chunksize=5000,  # tuning: verhoog indien nodig
    )

    # Optioneel: PK toevoegen
    if args.pk:
        pk_col = normalize_identifier(args.pk) if args.normalize_cols else args.pk
        add_pk(engine, args.schema, args.table, pk_col)

    # Optioneel: BTREE indexes voor join-kolommen
    if args.index_cols:
        cols = [c.strip() for c in args.index_cols.split(",") if c.strip()]
        for c in cols:
            col = normalize_identifier(c) if args.normalize_cols else c
            create_btree_index(engine, args.schema, args.table, col)

    # Read-only grants (SELECT)
    roles = [r.strip() for r in args.grant_select_to.split(",") if r.strip()]
    if roles:
        grant_readonly(engine, args.schema, args.table, roles)

    print(f"Imported {args.xlsx} (sheet={args.sheet or '[first sheet]'}) → {args.schema}.{args.table}")
    print(f"Rows: {len(df):,} | Columns: {len(df.columns)}")
    if args.add_id_col:
        print(f"Generated ID column: {args.id_col_name} (start={args.id_start})")
    if args.pk:
        print(f"Primary key: {args.pk}")
    if args.normalize_cols:
        print("Column normalization: enabled")
        print("Columns:", ", ".join(df.columns))


if __name__ == "__main__":
    main()
