from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from typing import Dict, List
from functools import wraps
from datetime import datetime
from pydantic import BaseModel
from shapely import from_wkt
from shapely.geometry import Polygon
import requests
import time
import logging
import threading
import sqlite3


DATABASE_FILE = "iss_data.db"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("fastapi")

app = FastAPI(version="0.0.1")


app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


class PolygonRequest(BaseModel):
    uuid: str
    wkt: str
    color: str


def log(func):

    @wraps(func)
    async def wrapper(*args, **kwargs):

        try:

            logger.info(f"Calling endpoint: {func.__name__}")

            result = await func(*args, **kwargs)

            logger.info(f"Success calling endpoint: {func.__name__}")

            return result

        except:

            logger.error(
                f"Error calling endpoint: {func.__name__}", exc_info=True)

            return {'message': 'An error occured'}

    return wrapper


def init_database():
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS iss_positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            latitude REAL,
            longitude REAL
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS iss_sun_exposures (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            window TEXT
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS polygons (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            uuid TEXT UNIQUE,
            color TEXT,
            wkt TEXT
        )
    ''')
    conn.commit()
    conn.close()


def select(query: str, *args) -> List:

    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()

    cursor.execute(query, *args)

    rows = cursor.fetchall()

    conn.close()

    return rows


def cud_operation(query: str, *args) -> List:

    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()

    cursor.execute(query, *args)

    affected_rows = cursor.rowcount

    conn.commit()

    conn.close()

    return affected_rows


def fetch_iss_data():

    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()

    while True:

        try:

            logger.info("Fetching wheretheiss api")

            url = "https://api.wheretheiss.at/v1/satellites/25544"

            headers = {"accept": "application/json"}

            with requests.get(url, headers=headers) as response:

                response.raise_for_status()

                data = response.json()

                cursor.execute(
                    "SELECT window FROM iss_sun_exposures ORDER BY id DESC LIMIT 1")

                sun_exposure = cursor.fetchone()

                cursor.execute("INSERT INTO iss_positions (timestamp, latitude, longitude) VALUES (?, ?, ?)", (
                    data['timestamp'], data['latitude'], data['longitude']))

                if data['visibility'] == 'daylight':

                    if not sun_exposure or sun_exposure[0] == 'end':

                        cursor.execute("INSERT INTO iss_sun_exposures (timestamp, window) VALUES (?, ?)", (
                            data['timestamp'], 'start'))

                else:

                    if sun_exposure and sun_exposure[0] == 'start':

                        cursor.execute("INSERT INTO iss_sun_exposures (timestamp, window) VALUES (?, ?)", (
                            data['timestamp'], 'end'))

                conn.commit()

        except requests.RequestException:

            logger.error("HTTPError at fetching ISS data", exc_info=True)

        except:

            logger.error("Unknown error at fetching ISS data", exc_info=True)

        finally:

            time.sleep(20)


def is_valid_2d_wkt_polygon(wkt):
    try:

        geometry = from_wkt(wkt)

        if isinstance(geometry, Polygon) and geometry.is_valid and not geometry.is_empty:

            if geometry.has_z:
                return False
            else:
                return True
        else:

            return False

    except:

        logger.error('Eror at validating polygon', exc_info=True)

        return False


@app.on_event("startup")
def startup_event():
    init_database()
    thread = threading.Thread(target=fetch_iss_data)
    thread.daemon = True
    thread.start()


@app.on_event("shutdown")
def shutdown_event():

    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()

    cursor.execute(
        "SELECT id, window FROM iss_sun_exposures ORDER BY id DESC LIMIT 1")

    row = cursor.fetchone()

    if row and row[1] == 'start':

        cursor.execute(
            "DELETE FROM iss_sun_exposures WHERE id = ?", (row[0], ))

        conn.commit()

    conn.close()


@app.get("/health", response_model=Dict[str, str])
@log
async def get_health():
    return {"status": "online"}


@app.get("/iss/sun", response_model=Dict[str, List[Dict[str, str]] | str])
@log
async def get_iss_sun_exposures():

    result = []

    rows = select(
        "SELECT timestamp, window FROM iss_sun_exposures ORDER BY id ASC")

    for i in range(0, len(rows) - 1, 2):

        row = rows[i]
        next_row = rows[i+1]

        assert (row[1] == 'start')
        assert (next_row[1] == 'end')

        result.append({
            'start': row[0],
            'end': next_row[0]
        })

    if len(rows) != 0 and rows[-1][1] == 'start':
        result.append({
            'start': rows[-1][0],
            'end': int(datetime.now().timestamp())
        })

    return {
        "sun_exposures": result
    }


@app.get("/iss/position", response_model=Dict[str, float | str])
@log
async def get_iss_position():

    rows = select(
        "SELECT latitude, longitude FROM iss_positions ORDER BY id DESC LIMIT 1")

    if len(rows):

        return {
            "latitude": rows[0][0],
            "longitude": rows[0][1]
        }

    return {"message": "No position data available"}


@app.post("/2d-polygons", response_model=Dict[str, str])
@log
async def post_2d_polygon(polygon: PolygonRequest):

    if is_valid_2d_wkt_polygon(polygon.wkt):

        if cud_operation("INSERT INTO polygons (uuid, color, wkt) VALUES (?, ?, ?)",
                         (polygon.uuid, polygon.color, polygon.wkt)):

            return {'message': polygon.uuid}

        return {'message': 'No affected rows'}

    return {'message': "The wkt string is not a valid 2D polygon"}


@app.delete("/2d-polygons/{uuid}", response_model=Dict[str, str])
@log
async def delete_2d_polygon(uuid: str):

    if cud_operation("DELETE FROM polygons WHERE uuid = ?", (uuid,)):

        return {'message': uuid}

    return {'message': 'No affected rows'}


@app.get("/2d-polygons", response_model=Dict[str, List[Dict[str, str]] | str])
@log
async def get_2d_polygons():

    rows = select(
        "SELECT uuid, color, wkt FROM polygons")

    return {
        "polygons": [{'uuid': row[0], 'color': row[1], 'wkt': row[2]} for row in rows]
    }


@app.get("/2d-polygons/{uuid}", response_model=Dict[str,  str])
@log
async def get_2d_polygons(uuid: str):

    rows = select(
        "SELECT uuid, color, wkt FROM polygons WHERE uuid = ?", (uuid,))

    if len(rows):

        return {
            "uuid": rows[0][0],
            "color": rows[0][1],
            "wkt": rows[0][2]
        }

    return {"message": "No polygon with the given uuid exists"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
