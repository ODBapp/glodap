import pandas as pd
from fastapi import APIRouter, Query, HTTPException
from fastapi.responses import JSONResponse, StreamingResponse
from typing import Optional, List
from datetime import datetime, timezone
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from async_lru import alru_cache
import re
import os
import io

# Load environment variables if needed
from dotenv import load_dotenv
load_dotenv()

DBUSER = os.getenv('DBUSER')
DBPASS = os.getenv('DBPASS')
DBHOST = os.getenv('DBHOST')
DBPORT = os.getenv('DBPORT')
DBNAME = os.getenv('DBNAME')

ASYNC_DB_URL = f"postgresql+asyncpg://{DBUSER}:{DBPASS}@{DBHOST}:{DBPORT}/{DBNAME}"
engine: AsyncEngine = create_async_engine(
    ASYNC_DB_URL,
    echo=False,
    future=True,
    pool_size=20,
    max_overflow=5,
    pool_timeout=30,
    pool_recycle=1800
)

router = APIRouter()

@alru_cache(maxsize=64)
async def get_cruise_columns():
    async with engine.begin() as conn:
        result = await conn.execute(text("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'cruisev2_2023'
        """))
        return [r[0] for r in result.fetchall()]

@router.get("/glodap/v2/2023/cruise", summary="Query GLODAP cruise metadata", description="Search cruise metadata with fuzzy filters for cruise,PI names, and etc. Refer to https://www.ncei.noaa.gov/access/ocean-carbon-acidification-data-system/oceans/GLODAPv2_2023/cruise_table_v2023.html")
async def query_cruise_metadata(
    cruise: Optional[str] = Query(None, description="Comma-separated cruise expocode(s) to match expocode or alias by using wildcard (e.g. *ARK*)"),
    start: Optional[datetime] = Query(None, description="Filtering cruises after given start date (e.g. 1990-01-01)"),
    end: Optional[datetime] = Query(None, description="Filtering cruises before given end date (e.g. 2000-12-31)"),
    pi: Optional[str] = Query(None, description="Comma-separated PI names or fuzzy match (e.g. Kelly*, Schlosser)"),
    field: Optional[str] = Query("all", description="Comma-separated PI fields (e.g. chief, carbon, other). Add 'false' to disable extra PI columns output."),
    region: Optional[str] = Query(None, description="Comma-separated region names or prefix (e.g. pacific, Okhotsk) to filter desired marine region."),
    ship: Optional[str] = Query(None, description="Comma-separated ship name(s) with fuzzy matching. Supports wildcard '*' (e.g. Polarstern, Arctic*)"),
    measurement: Optional[str] = Query(None, description="Fuzzy match measurement types (e.g. CTD*) to search the measurements in the cruise metadata."),
    append: Optional[str] = Query("*", description="Extra columns to include: data_files, qc_details, map, metadata_report, cruise_references by using abbreviations: file, qc, map, metadata, ref). Comma-separated (e.g. file,map), use 'all' or '*' to include all, 'false' to include none."),
    format: Optional[str] = Query("json", description="Output format: 'json' (default) or 'csv'")
):
    all_cols = await get_cruise_columns()
    base_order = ["expocode", "start_date", "end_date", "region", "alias", "ship"]
    pi_fields_map = {
        "chief": "chief_scientist", "carbon": "carbon_pi", "hydrography": "hydrography_pi",
        "oxygen": "oxygen_pi", "nutrients": "nutrients_pi", "cfc": "cfc_pi", "organics": "organics_pi",
        "isotopes": "isotopes_pi", "other": "other_pi"
    }

    def dedup_and_strip(lst):
        return list({x.strip().lower() for x in lst if x.strip()})

    selected_cols = base_order.copy()
    pi_cols = []
    field_flags = dedup_and_strip(field.split(',')) if field and field.lower() != "all" else list(pi_fields_map.keys())
    disable_extra_pi = 'false' in field_flags

    if field and field.lower() != "false":
        for f in field_flags:
            if f in pi_fields_map:
                pi_cols.append(pi_fields_map[f])
        selected_cols.extend(pi_cols)

    append_flags = dedup_and_strip(append.split(',')) if append else []
    append_all = '*' in append_flags or 'all' in append_flags
    disable_append = 'false' in append_flags

    include_cols = {
        "file": "data_files", "qc": "qc_details", "map": "map",
        "metadata": "metadata_report", "ref": "cruise_references"
    }
    non_output_cols = ["legs", "geom", "cruise_id"]

    if disable_append:
        non_output_cols += list(include_cols.values())
    else:
        for flag, col in include_cols.items():
            if append_all or flag in append_flags:
                if col in all_cols:
                    selected_cols.append(col)
            else:
                non_output_cols.append(col)

    include_measurements = measurement and measurement.lower() != "false"
    if include_measurements and "measurements" in all_cols:
        selected_cols.append("measurements")
    else:
        non_output_cols.append("measurements")

    if disable_extra_pi or (pi and pi.lower() == "false"):
        for f, col in pi_fields_map.items():
            if col not in pi_cols and col in all_cols:
                non_output_cols.append(col)

    already_selected = set(selected_cols)
    remaining_cols = sorted([col for col in all_cols if col not in already_selected and col not in non_output_cols])
    selected_cols += remaining_cols

    conditions = []
    params = {}
    ''' Directly use SQL, without params binding (it works)
    if pi and pi.lower() != "false":
        pi_terms = dedup_and_strip(pi.split(","))
        inner_conditions = []
        for pi_idx, pi_term in enumerate(pi_terms):
            for i, col in enumerate(pi_cols):
                key = f"pi_{pi_idx}_{col}_{i}"
                if '*' in pi_term:
                    params[key] = pi_term.lower().replace("*", "%")
                else:
                    params[key] = f"%{pi_term.lower()}%"
                item = params[key]
                inner_conditions.append(f"LOWER({col}) ILIKE '{item}'")
        if inner_conditions:
            conditions.append("(" + " OR ".join(inner_conditions) + ")")
    '''
    if pi and pi.lower() != "false":
        pi_terms = dedup_and_strip(pi.split(","))
        inner_conditions = []
        for pi_idx, pi_term in enumerate(pi_terms):
            for i, col in enumerate(pi_cols):
                key = f"pi_{pi_idx}_{col}_{i}"
                if '*' in pi_term:
                    params[key] = pi_term.lower().replace("*", "%")
                else:
                    params[key] = f"%{pi_term.lower()}%"
                inner_conditions.append(f"LOWER({col}) ILIKE :{key}")
        if inner_conditions:
            conditions.append("(" + " OR ".join(inner_conditions) + ")")

    if include_measurements:
        pattern = measurement.lower().replace("*", "%")
        conditions.append("LOWER(measurements) LIKE :measurement")
        params["measurement"] = f"%{pattern.strip('%')}%"

    if region:
        region_terms = dedup_and_strip(region.split(','))
        region_clauses = []
        for r in region_terms:
            if r.startswith("pacific"):
                region_clauses.append("(LOWER(region) LIKE 'pacific%' OR LOWER(region) LIKE 'sea of okhotsk%')")
            elif r.startswith("okhotsk") or r.startswith("sea of okhotsk"):
                region_clauses.append("LOWER(region) LIKE 'sea of okhotsk%'")
            else:
                key = f"region_{r}"
                region_clauses.append(f"LOWER(region) LIKE :{key}")
                params[key] = r + "%"
        if region_clauses:
            conditions.append("(" + " OR ".join(region_clauses) + ")")

    if ship:
        ship_terms = dedup_and_strip(ship.split(','))
        ship_clauses = []
        for i, term in enumerate(ship_terms):
            if '*' in term:
                ship_clauses.append(f"LOWER(ship) LIKE :ship_{i}")
                params[f"ship_{i}"] = term.replace("*", "%")
            else:
                ship_clauses.append(f"LOWER(ship) = :ship_{i}")
                params[f"ship_{i}"] = term.lower()
        if ship_clauses:
            conditions.append("(" + " OR ".join(ship_clauses) + ")")

    # Cruise filter (safe OR handling)
    if cruise:
        cruise_list = list({c.strip().lower() for c in cruise.split(',') if c.strip()})
        cruise_conditions = []
        for i, cruise_code in enumerate(cruise_list):
            if '*' in cruise_code:
                cruise_like = cruise_code.replace("*", "%")
                cruise_conditions.append(f"(LOWER(expocode) LIKE :cruise_exp_{i} OR LOWER(alias) LIKE :cruise_alias_{i})")
                params[f"cruise_exp_{i}"] = cruise_like
                params[f"cruise_alias_{i}"] = cruise_like
            else:
                cruise_conditions.append(f"LOWER(expocode) = :cruise_exp_{i}")
                params[f"cruise_exp_{i}"] = cruise_code
        if cruise_conditions:
            conditions.append("(" + " OR ".join(cruise_conditions) + ")")

    if start and end and start > end:
        start, end = end, start

    if start or end:
        date_clause = []
        if start and end:
            date_clause.append(
                "((SELECT MIN(d) FROM unnest(string_to_array(start_date, ',')) AS d) >= :start AND (SELECT MAX(d) FROM unnest(string_to_array(end_date, ',')) AS d) <= :end)"
            )
            params["start"] = start.date().isoformat()
            params["end"] = end.date().isoformat()
        elif start:
            date_clause.append(
                "(SELECT MIN(d) FROM unnest(string_to_array(start_date, ',')) AS d) >= :start"
            )
            params["start"] = start.date().isoformat()
        elif end:
            date_clause.append(
                "(SELECT MAX(d) FROM unnest(string_to_array(end_date, ',')) AS d) <= :end"
            )
            params["end"] = end.date().isoformat()
        if date_clause:
            conditions.append(" AND ".join(date_clause))

    sql = f"SELECT {', '.join(selected_cols)} FROM cruisev2_2023"
    if conditions:
        sql += " WHERE " + " AND ".join(conditions)

    try:
        async with engine.connect() as conn:
            result = await conn.execute(text(sql), params)
            rows = result.mappings().all()
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

    records = [dict(row) for row in rows]

    if format.lower() == 'csv':
        df = pd.DataFrame(records)
        stream = io.StringIO()
        df.to_csv(stream, index=False)
        stream.seek(0)
        return StreamingResponse(iter([stream.getvalue()]), media_type="text/csv", headers={"Content-Disposition": "attachment; filename=cruise_metadata.csv"})

    return records
