import os
import time
import datetime
from typing import Any
from fastapi import FastAPI
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

# ------------------------------------------------ SALUD ---------------------------------------------------------

@app.get("/citas_medicas_disponibles", tags=["Salud"])
async def select_citamedicas():
    rows = call_procedure('pas_citas_disponibles')
    return jsonable_encoder(rows)


@app.get("/citas_medicas_agendadas/{user_id}", tags=["Salud"])
async def select_citamedicas_user(user_id: str):
    rows = call_procedure('pas_citas_agendadas', user_id)
    return jsonable_encoder(rows)


@app.put("/cancelar_cita_medica/{user_id}", tags=["Salud"])
async def delete_citamedica(user_id: int, fecha: datetime.datetime, especialidad: str):
    rows = call_procedure('pas_delete_cita_medica', user_id, fecha, especialidad)
    if len(rows) == 0:
        return jsonable_encoder({'Key': 0, 'Answer': 'Done'})
    return jsonable_encoder(rows)


@app.put("/agendar_cita_medica/{user_id}", tags=["Salud"])
async def add_citamedica(user_id: int, fecha: datetime.datetime, especialidad: str):
    rows = call_procedure('pas_add_cita_medica', user_id, fecha, especialidad)
    if len(rows) == 0:
        return jsonable_encoder({'Key': 0, 'Answer': 'Done'})
    return jsonable_encoder(rows)


@app.get("/resultado_cita_medica/{user_id}", tags=["Salud"])
async def check_citamedica(user_id: int):
    rows = call_procedure('pas_check_resultados', user_id)
    return jsonable_encoder(rows)


@app.get("/incapacidades/{user_id}", tags=["Salud"])
async def select_incapacidad(user_id: int):
    rows = call_procedure('pas_view_incapacidad', user_id)
    return jsonable_encoder(rows)


@app.post("/añadir_incapacidad/{user_id}", tags=["Salud"])
async def add_incapacidad(user_id: int, fecha: datetime.datetime, enfermedad: str, dias: int):
    rows = call_procedure('pas_add_incapacidad', user_id, fecha, enfermedad, dias)
    if len(rows) == 0:
        return jsonable_encoder({'Key': 0, 'Answer': 'Done'})
    return jsonable_encoder(rows)


@app.put("/modificar_incapacidad/", tags=["Salud"])
async def edit_incapacidad(incapacidad_id: int, fecha: datetime.datetime, enfermedad: str, dias: int):
    rows = call_procedure('pas_edit_incapacidad', incapacidad_id, fecha, enfermedad, dias)
    if len(rows) == 0:
        return jsonable_encoder({'Key': 0, 'Answer': 'Done'})
    return jsonable_encoder(rows)


@app.get("/atencionessalud/{user_id}", tags=["Salud"])
async def select_atencionsalud(user_id: int):
    rows = call_procedure('pas_view_atencionsalud', user_id)
    return jsonable_encoder(rows)


@app.post("/añadir_atencionsalud/{user_id}", tags=["Salud"])
async def add_atencionsalud(user_id: int, fecha: datetime.datetime, tipo: str):
    rows = call_procedure('pas_add_atencionsalud', user_id, fecha, tipo)
    if len(rows) == 0:
        return jsonable_encoder({'Key': 0, 'Answer': 'Done'})
    return jsonable_encoder(rows)


@app.put("/modificar_atencionsalud/{user_id}", tags=["Salud"])
async def edit_atencionsalud(atencionsalud_id: int, fecha: datetime.datetime, tipo: str):
    rows = call_procedure('pas_edit_atencionsalud', atencionsalud_id, fecha, tipo)
    if len(rows) == 0:
        return jsonable_encoder({'Key': 0, 'Answer': 'Done'})
    return jsonable_encoder(rows)


@app.get("/select_perfilriesgo/{user_id}", tags=["Salud"])
async def select_perfilriesgo(user_id: int):
    rows = call_procedure('pas_view_perfilriesgo', user_id)
    return jsonable_encoder(rows)


# ---------------------------------------------- ECONOMICO -------------------------------------------------------

@app.get("/falla_alimentacion/{user_id}", tags=["Económico"])
async def select_falla_alimentacion(user_id: int):
    rows = call_procedure("sp_fallaalimentacion_est", user_id)
    return jsonable_encoder(rows)


@app.get("/actividad_corresponsabilidad/{user_id}", tags=["Económico"])
async def select_actividad_corresponsabilidad(user_id: int):
    rows = call_procedure("sp_actividadcorresp_est", user_id)
    return jsonable_encoder(rows)


@app.get("/horas_corresponsabilidad/{user_id}", tags=["Económico"])
async def select_horas_corresponsabilidad(user_id: int):
    rows = call_procedure("horas_corresponsabilidad_est", user_id)
    return jsonable_encoder(rows)


@app.get("/pbm_estudiante/{user_id}", tags=["Económico"])
async def select_pbm_estudiante(user_id: int):
    rows = call_procedure("pbm_est", user_id)
    return jsonable_encoder(rows)

# Convocatorias --------------------------------------------------------------------------------------------------


@app.get("/conv_fomento_emprendimiento/{user_id}/{tema}", tags=["Económico"])
async def select_conv_fomento_emprendimiento(user_id: int, tema: str):
    rows = call_procedure("sp_convocatoriafomentoemprendimeinto_est", user_id, tema)
    return jsonable_encoder(rows)


@app.get("/conv_fomento_emprendimiento/{user_id}/{nombre}", tags=["Económico"])
async def select_conv_fomento_emprendimiento_nombre(user_id: int, nombre: str):
    rows = call_procedure("sp_convocatoriafomentoemprendimiento_nombre", user_id, nombre)
    return jsonable_encoder(rows)


@app.get("/conv_fomento_emprendimiento/{user_id}", tags=["Económico"])
async def select_conv_fomento_emprendimiento_todo(user_id: int):
    rows = call_procedure("sp_convocatoriafomentoemprendimiento", user_id)
    return jsonable_encoder(rows)
  
  
    #----------------------------------------------------------------------------------------------------------
    
@app.get("/conv_gestion_alimentaria/{user_id}/{comida}/{lugar}", tags=["Económico"])
async def select_conv_gestion_alimentaria(user_id: int, comida: str, lugar: str):
    rows = call_procedure("sp_convocatoriagestionalimentaria_est", user_id, comida, lugar)
    return jsonable_encoder(rows)
    
    
@app.get("/conv_gestion_alimentaria/{user_id}/{comida}", tags=["Económico"])
async def select_conv_gestion_alimentaria_com(user_id: int, comida:str):
    rows = call_procedure("sp_convocatoriagestionalimentaria_com",user_id, comida)
    return jsonable_encoder(rows)


@app.get("/conv_gestion_alimentaria/{user_id}", tags=["Económico"])
async def select_conv_gestion_alimentaria_todo(user_id: int):
    rows = call_procedure("sp_convocatoriagestionalimentaria", user_id)  
    return jsonable_encoder(rows)  
  

    #----------------------------------------------------------------------------------------------------------


@app.get("/conv_gestion_alojamiento/{user_id}/{localidad}/{tipo}", tags=["Económico"])
async def select_conv_gestion_alojamiento(user_id: int, localidad:str, tipo:str):
    rows = call_procedure("sp_convocatoriagestionalojamiento_est",user_id,localidad,tipo)
    return jsonable_encoder(rows)


@app.get("/conv_gestion_alojamiento/{user_id}/{localidad}", tags=["Económico"])
async def select_conv_gestion_alojamiento_loc(user_id: int, localidad:str):
    rows = call_procedure("sp_convocatoriagestionalojamiento_loc",user_id,localidad)
    return jsonable_encoder(rows)


@app.get("/conv_gestion_alojamiento/{user_id}", tags=["Económico"])
async def select_conv_gestion_alojamiento_todo(user_id: int):
    rows = call_procedure("sp_convocatoriagestionalojamiento",user_id)
    return jsonable_encoder(rows)


#----------------------------------------------------------------------------------------------------------


@app.get("/conv_gestion_economica/{user_id}", tags=["Económico"])
async def select_conv_gestion_economica(user_id: int):
    rows = call_procedure("sp_convocatoriagestioneconomica_est",user_id)
    return jsonable_encoder(rows)


@app.get("/conv_gestion_economica/{user_id}/mayor_igual/{filter}", tags=["Económico"])
async def select_conv_gestion_economica_mayor(user_id: int, filter: float):
    rows = call_procedure("sp_convocatoriagestioneconomica_mayor",user_id,filter)
    return jsonable_encoder(rows)


@app.get("/conv_gestion_economica/{user_id}/menor/{filter}", tags=["Económico"])
async def select_conv_gestion_economica_menor(user_id: int, filter:float):
    rows = call_procedure("sp_convocatoriagestioneconomica_menor",user_id,filter)
    return jsonable_encoder(rows)


#----------------------------------------------------------------------------------------------------------

@app.get("/conv_gestion_transporte/{user_id}/{tipo}", tags=["Económico"])
async def select_conv_gestion_transporte(user_id: int, tipo:str):
    rows = call_procedure("sp_convocatoriagestiontransporte_est",user_id,tipo)
    return jsonable_encoder(rows)


@app.get("/conv_gestion_transporte/{user_id}", tags=["Económico"])
async def select_conv_gestion_transporte_todo(user_id: int):
    rows = call_procedure("sp_convocatoriagestiontransporte",user_id)
    return jsonable_encoder(rows)


@app.get("/conv_gestion_transporte/{user_id}", tags=["Económico"])
async def select_conv_gestion_transporte(user_id: int, tipo: str | None):
    if tipo is None:
        rows = call_procedure("sp_convocatoriagestiontransporte",user_id)
    else:
        rows = call_procedure("sp_convocatoriagestiontransporte_est",user_id,tipo)
    return jsonable_encoder(rows)


# Tienda Bienestar UN-------------------------------------------------------------------------------------


@app.get("/info_factura/{user_id}", tags=["Económico"])
async def select_info_factura_tienda(user_id: int, tienda_id:int):
    rows = call_procedure("sp_info_factura_per",user_id, tienda_id)
    return jsonable_encoder(rows)





def call_procedure(procedure: str, *args: Any) -> list[dict]:
    """
    Returns a list with all the rows given a procedure and arguments

    :param procedure: function to call
    :param args: arguments for the procedure
    """
    cursor = connection.cursor()
    try:
        cursor.callproc(procedure, list(args))
    except errors.ProgrammingError as e:
        return [{'Key': 0, 'Answer': e}]
    except errors.DatabaseError as e:
        return [{'Key': 0, 'Answer': e}]

    column_names, rows = [], []
    for answer in cursor.stored_results():
        column_names.extend(answer.column_names)
        rows.extend(answer.fetchall())

    result = []
    if len(rows) != 0:
        for key, row in enumerate(rows):
            data = {'Key': key}
            for i, value in enumerate(row):
                data[column_names[i]] = value
            result.append(data)

    cursor.close()
    connection.commit()

    return result
