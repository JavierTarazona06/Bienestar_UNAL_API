import os
import time
import datetime
from typing import Any
from fastapi import FastAPI
from dotenv import load_dotenv
from sqlalchemy.engine import URL
from mysql.connector import errors
from sqlalchemy import create_engine
from contextlib import asynccontextmanager
from fastapi.encoders import jsonable_encoder

url = URL.create(
    drivername=os.environ["DRIVER"],
    username=os.environ["NAME_USER"],
    password=os.environ["PASSWORD"],
    host=os.environ["HOST_NAME"],
    database=os.environ["DATABASE"],
    port=int(os.environ["PORT"])
)

engine = create_engine(url)


@asynccontextmanager
async def lifespan(app: FastAPI):
    pass
    yield
    # Close the connection with the database
    connection.close()

app = FastAPI(lifespan=lifespan)


while True:
    try:
        connection = engine.raw_connection()
        break
    except errors.DatabaseError as error:
        print(error)
        time.sleep(5)


@app.get("/")
async def root():
    return jsonable_encoder('MAIN PAGE')


@app.get("/citas_medicas_disponibles")
async def select_citamedicas():
    rows = call_procedure('pas_citas_disponibles')
    return jsonable_encoder(rows)


@app.get("/citas_medicas_agendadas/{user_id}")
async def select_citamedicas_user(user_id: str):
    rows = call_procedure('pas_citas_agendadas', user_id)
    return jsonable_encoder(rows)


@app.put("/cancelar_cita_medica/{user_id}")
async def delete_citamedica(user_id: int, fecha: datetime.datetime, especialidad: str):
    rows = call_procedure('pas_delete_cita_medica', user_id, fecha, especialidad)
    return jsonable_encoder(rows)


@app.put("/agendar_cita_medica/{user_id}")
async def add_citamedica(user_id: int, fecha: datetime.datetime, especialidad: str):
    rows = call_procedure('pas_add_cita_medica', user_id, fecha, especialidad)
    return jsonable_encoder(rows)


@app.get("/resultado_cita_medica/{user_id}")
async def check_citamedica(user_id: int):
    rows = call_procedure('pas_check_resultados', user_id)
    return jsonable_encoder(rows)


@app.get("/incapacidades/{user_id}")
async def select_incapacidad(user_id: int):
    rows = call_procedure('pas_view_incapacidad', user_id)
    return jsonable_encoder(rows)


@app.post("/añadir_incapacidad/{user_id}")
async def add_incapacidad(user_id: int, fecha: datetime.datetime, enfermedad: str, dias: int):
    rows = call_procedure('pas_add_incapacidad', user_id, fecha, enfermedad, dias)
    return jsonable_encoder(rows)


@app.put("/modificar_incapacidad/")
async def edit_incapacidad(incapacidad_id: int, fecha: datetime.datetime, enfermedad: str, dias: int):
    rows = call_procedure('pas_edit_incapacidad', incapacidad_id, fecha, enfermedad, dias)
    return jsonable_encoder(rows)


@app.get("/atencionessalud/{user_id}")
async def select_atencionsalud(user_id: int):
    rows = call_procedure('pas_view_atencionsalud', user_id)
    return jsonable_encoder(rows)


@app.get("/añadir_atencionsalud/{user_id}")
async def add_atencionsalud(user_id: int, fecha: datetime.datetime, tipo: str):
    rows = call_procedure('pas_add_atencionsalud', user_id, fecha, tipo)
    return jsonable_encoder(rows)


@app.get("/modificar_atencionsalud/{user_id}")
async def edit_atencionsalud(atencionsalud_id: int, fecha: datetime.datetime, tipo: str):
    rows = call_procedure('pas_edit_atencionsalud', atencionsalud_id, fecha, tipo)
    return jsonable_encoder(rows)


@app.get("/select_perfilriesgo/{user_id}")
async def select_perfilriesgo(user_id: int):
    rows = call_procedure('pas_view_perfilriesgo', user_id)
    return jsonable_encoder(rows)


def call_procedure(procedure: str, *args: Any) -> list:
    """
    Returns a list with all the rows given a procedure and arguments

    :param procedure: function to call
    :param args: arguments for the procedure
    """
    cursor = connection.cursor()
    try:
        cursor.callproc(procedure, list(args))
    except errors.ProgrammingError as e:
        return [e]
    except errors.DatabaseError as e:
        return [e]

    answer = None
    for result in cursor.stored_results():
        answer = result

    rows = []
    if answer is not None:
        for row in answer:
            rows.append(row)
    else:
        rows = ['Done']

    cursor.close()
    connection.commit()

    return rows
