import pandas as pd
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse, StreamingResponse
from typing import Optional, List
from datetime import datetime, timezone
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
from contextlib import asynccontextmanager
from async_lru import alru_cache
from src.cruise_metadata import router as cruise_router
import os
import re
import io

load_dotenv()
APPHOST = os.getenv('APPHOST')
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

@asynccontextmanager
async def lifespan(app: FastAPI):
    print(APPHOST, " App start at", datetime.now(timezone.utc))
    yield
    print(APPHOST, " App end at", datetime.now(timezone.utc))

app = FastAPI(
    lifespan=lifespan,
    docs_url=None,
    title="ODB GLODAP API",
    description="Open API to query GLODAP v2.2023 data, compiled by ODB. Data source: https://www.ncei.noaa.gov/access/metadata/landing-page/bin/iso?id=gov.noaa.nodc:0283442",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(cruise_router)

'''
class GlodapRecord(BaseModel):
    expocode: Optional[str]
    station: Optional[str]
    region: Optional[str]
    cast_number: Optional[int]
    year: Optional[int]
    month: Optional[int]
    latitude: Optional[float]
    longitude: Optional[float]
    depth: Optional[float]
    datetime: Optional[datetime]
'''

@app.get("/glodap/v2/2023/openapi.json", include_in_schema=False)
async def custom_openapi():
    return JSONResponse(generate_custom_openapi())

@app.get("/glodap/v2/2023/swagger", include_in_schema=False)
async def custom_swagger_ui_html():
    return get_swagger_ui_html(
        openapi_url="/glodap/v2/2023/openapi.json",
        title="ODB GLODAP Swagger UI",
    )

def generate_custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=app.routes,
    )
    openapi_schema["servers"] = [
        {"url": APPHOST}
    ]
    app.openapi_schema = openapi_schema
    return app.openapi_schema

@alru_cache(maxsize=64)
async def get_table_columns():
    async with engine.begin() as conn:
        result = await conn.execute(text("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'glodapv2_2023'
        """))
        return {r[0] for r in result.fetchall()}

@app.get("/glodap/v2/2023/", response_model=List[dict], summary="Query GLODAP v2.2023 dataset", description="Search oceanographic data from GLODAP v2.2023 using spatial, temporal, and cruise filters.")
async def query_glodap(
    lon0: Optional[float] = Query(None, description="Minimum longitude (required if cruise is not specified)"),
    lat0: Optional[float] = Query(None, description="Minimum latitude (required if cruise is not specified)"),
    lon1: Optional[float] = Query(None, description="Maximum longitude (optional, defaults to lon0 if not given)"),
    lat1: Optional[float] = Query(None, description="Maximum latitude (optional, defaults to lat0 if not given)"),
    dep0: Optional[float] = Query(0.0, description="Minimum sampling depth in meters (default 0.0)"),
    dep1: Optional[float] = Query(10000.0, description="Maximum sampling depth in meters (default 10000.0)"),
    start: Optional[datetime] = Query(datetime(1972, 1, 1, tzinfo=timezone.utc), description="Start datetime (ISO format, default 1972-01-01)"),
    end: Optional[datetime] = Query(datetime.now(timezone.utc), description="End datetime (ISO format, default now)"),
    cruise: Optional[str] = Query(None, description="Comma-separated cruise expocode list (case-insensitive, space-tolerant)"),
    append: Optional[str] = Query(None, description="Comma-separated extra variables or wildcards (e.g. '*cfc*', 'nitrate', '*')"),
    flag: Optional[bool] = Query(False, description="Include corresponding World Ocean Circulation Experiment (WOCE) flag of additional varaibles"),
    qc: Optional[bool] = Query(False, description="Include corresponding quality control (QC) flag of additional varaibles"),
    doi: Optional[bool] = Query(True, description="Include DOI of data for citation"),
    format: Optional[str] = Query("json", description="Output format: 'json' (default) or 'csv'")
):
    if cruise is None and (lon0 is None or lat0 is None):
        raise HTTPException(status_code=400, detail="You must specify either cruise or both lon0 and lat0")

    if lon1 is not None and lon0 is not None and lon0 > lon1:
        lon0, lon1 = lon1, lon0
    if lat1 is not None and lat0 is not None and lat0 > lat1:
        lat0, lat1 = lat1, lat0
    if dep0 is not None and dep1 is not None and dep0 > dep1:
        dep0, dep1 = dep1, dep0
    if start and end and start > end:
        start, end = end, start

    columns = [
        'expocode', 'station', 'region', 'cast_number', 'year', 'month',
        'latitude', 'longitude', 'bottomdepth', 'maxsampdepth', 'bottle',
        'pressure', 'depth', 'datetime'
    ]
    if doi:
        columns.append('doi')

    colnames = await get_table_columns()
    all_data_vars = [c for c in colnames if c not in columns and not c.startswith(('flag_', 'qc_', 'err_')) and c not in ('geom', 'doi')]

    extra_cols = []
    requested = []
    if append:
        cleaned = [c.strip().lower() for c in append.split(',') if c.strip()]
        if '*' in cleaned or 'all' in cleaned:
            requested = all_data_vars
        else:
            pattern_vars = set()
            for pattern in cleaned:
                regex = re.compile('^' + pattern.replace('*', '.*') + '$', re.IGNORECASE)
                matches = filter(lambda col: regex.match(col), all_data_vars)
                pattern_vars.update(matches)
            requested = list(pattern_vars)

    for var in requested:
        if var in colnames:
            extra_cols.append(var)
        if flag and f"flag_{var}" in colnames:
            extra_cols.append(f"flag_{var}")
        if qc and f"qc_{var}" in colnames:
            extra_cols.append(f"qc_{var}")

    columns += sorted(set(extra_cols))
    col_str = ', '.join(columns)

    sql = f"""
    SELECT {col_str}
    FROM glodapv2_2023
    WHERE
    """
    params = {}

    if lon0 is not None and lat0 is not None and lon1 is not None and lat1 is not None and lon0 != lon1 and lat0 != lat1:
        sql += """
            ST_Within(
                geom,
                ST_MakeEnvelope(:lon0, :lat0, :lon1, :lat1, 4326)
            ) AND
        """
        params.update({"lon0": lon0, "lat0": lat0, "lon1": lon1, "lat1": lat1})
    elif lon0 is not None and lat0 is not None:
        sql += "longitude = :lon0 AND latitude = :lat0 AND"
        params.update({"lon0": lon0, "lat0": lat0})

    sql += " depth BETWEEN :dep0 AND :dep1 AND datetime BETWEEN :start AND :end"
    params.update({"dep0": dep0, "dep1": dep1, "start": start, "end": end})

    if cruise:
        cruise_list = list({c.strip().lower() for c in cruise.split(',') if c.strip()})
        placeholders = ', '.join(f':cruise_{i}' for i in range(len(cruise_list)))
        sql += f" AND LOWER(expocode) IN ({placeholders})"
        for i, val in enumerate(cruise_list):
            params[f'cruise_{i}'] = val

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
        return StreamingResponse(iter([stream.getvalue()]), media_type="text/csv", headers={"Content-Disposition": "attachment; filename=glodapv2_2023_data.csv"})
 
    return records
