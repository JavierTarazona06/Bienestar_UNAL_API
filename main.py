from fastapi import FastAPI
import time
import os
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from mysql.connector import errors
from fastapi.encoders import jsonable_encoder
from typing import Any

app = FastAPI()

url = URL.create(
    drivername=os.getenv("DRIVER"),
    username=os.getenv("USERNAME"),
    password=os.getenv("PASSWORD"),
    host=os.getenv("HOST"),
    database=os.getenv("DATABASE"),
    port=int(os.getenv("PORT"))
)

engine = create_engine(url)

while True:
    try:
        connection = engine.raw_connection()
        break
    except errors.DatabaseError as error:
        print(error)
        time.sleep(5)


@app.get("/")
async def root():
    rows = call_procedure('pas_citas_disponibles')
    return jsonable_encoder(rows)


@app.get("/hello/{user_id}")
async def say_hello(user_id: str):
    rows = call_procedure('pas_citas_agendadas', user_id)
    return jsonable_encoder(rows)


def call_procedure(procedure: str, *args: Any) -> list:
    if args:
        print(f"{procedure} {args[0]}")
    else:
        print(f"{procedure}")

    cursor = connection.cursor()
    try:
        cursor.callproc(procedure, list(args))
    except errors.ProgrammingError as e:
        print(e)
        return []
    except errors.DatabaseError as e:
        print(e)
        return []

    answer = None
    for result in cursor.stored_results():
        answer = result

    rows = []
    if answer is not None:
        for row in answer:
            rows.append(row)

    cursor.close()
    connection.commit()
    # connection.close()

    return rows


