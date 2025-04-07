import pandas as pd
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, List
from datetime import datetime
from pydantic import BaseModel
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()
DBUSER = os.getenv('DBUSER')
DBPASS = os.getenv('DBPASS')
DBHOST = os.getenv('DBHOST')
DBPORT = os.getenv('DBPORT')
DBNAME = os.getenv('DBNAME')

db_url = f"postgresql://{DBUSER}:{DBPASS}@{DBHOST}:{DBPORT}/{DBNAME}"
engine = create_engine(db_url)

app = FastAPI()

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/glodap/v2/2023/", response_model=List[dict])
def query_glodap(
    lon0: float = Query(..., description="Minimum longitude"),
    lat0: float = Query(..., description="Minimum latitude"),
    lon1: Optional[float] = Query(None, description="Maximum longitude"),
    lat1: Optional[float] = Query(None, description="Maximum latitude"),
    dep0: Optional[float] = Query(None, description="Minimum depth"),
    dep1: Optional[float] = Query(None, description="Maximum depth"),
    start: Optional[datetime] = Query(None, description="Start datetime"),
    end: Optional[datetime] = Query(None, description="End datetime"),
    cruise: Optional[str] = Query(None, description="Comma-separated cruise codes"),
    append: Optional[str] = Query(None, description="Comma-separated extra columns to include"),
    flag: Optional[bool] = Query(False, description="Include flag columns"),
    qc: Optional[bool] = Query(False, description="Include QC columns")
):
    # Validate range if upper bounds exist
    if lon1 is not None and lon0 > lon1:
        raise HTTPException(status_code=400, detail="Invalid longitude range")
    if lat1 is not None and lat0 > lat1:
        raise HTTPException(status_code=400, detail="Invalid latitude range")
    if dep0 is not None and dep1 is not None and dep0 > dep1:
        raise HTTPException(status_code=400, detail="Invalid depth range")
    if start and end and start > end:
        raise HTTPException(status_code=400, detail="Invalid datetime range")

    # Get all available columns
    with engine.begin() as conn:
        all_cols = pd.read_sql("SELECT * FROM glodapv2_2023 LIMIT 1", conn).columns.tolist()

    # Base columns
    columns = [
        'expocode', 'station', 'region', 'cast_number', 'year', 'month',
        'latitude', 'longitude', 'bottomdepth', 'maxsampdepth',
        'bottle', 'pressure', 'depth', 'datetime'
    ]

    # Append extra columns if they exist
    extra_cols = []
    if append:
        for var in [c.strip() for c in append.split(',') if c.strip()]:
            if var in all_cols:
                extra_cols.append(var)
                if flag and f"flag_{var}" in all_cols:
                    extra_cols.append(f"flag_{var}")
                if qc and f"qc_{var}" in all_cols:
                    extra_cols.append(f"qc_{var}")
    columns += extra_cols
    col_str = ', '.join(columns)

    # Base SQL
    sql = f"SELECT {col_str} FROM glodapv2_2023 WHERE 1=1"
    params = {}

    if lon1 is not None and lat1 is not None:
        sql += " AND ST_Within(geom, ST_MakeEnvelope(:lon0, :lat0, :lon1, :lat1, 4326))"
        params.update({"lon0": lon0, "lat0": lat0, "lon1": lon1, "lat1": lat1})
    else:
        sql += " AND longitude = :lon0 AND latitude = :lat0"
        params.update({"lon0": lon0, "lat0": lat0})

    if dep0 is not None and dep1 is not None:
        sql += " AND depth BETWEEN :dep0 AND :dep1"
        params.update({"dep0": dep0, "dep1": dep1})

    if start and end:
        sql += " AND datetime BETWEEN :start AND :end"
        params.update({"start": start, "end": end})

    if cruise:
        cruise_list = [c.strip() for c in cruise.split(',') if c.strip()]
        placeholders = ', '.join(f':cruise_{i}' for i in range(len(cruise_list)))
        sql += f" AND expocode IN ({placeholders})"
        for i, val in enumerate(cruise_list):
            params[f'cruise_{i}'] = val

    try:
        with engine.begin() as conn:
            df = pd.read_sql(text(sql), conn, params=params)
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

    return df.to_dict(orient="records")
