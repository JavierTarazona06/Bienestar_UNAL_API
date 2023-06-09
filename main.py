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
from fastapi.middleware.cors import CORSMiddleware

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

# CORS configuration
origins = ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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


# ------------------------------------------------ GENERAL ---------------------------------------------------------


@app.post("/est_toma_conv", tags=["General"])
async def estudiante_toma_convocatoria(est_id: int | None, conv_id: int, fecha: str):
    rows = call_procedure("sp_insertar_est_tm_conv_est", est_id, conv_id, fecha)
    if len(rows) == 0:
        return jsonable_encoder({'Key': 0, 'Answer': 'Done'})
    return jsonable_encoder(rows)


@app.get("/programa_area_convocatoria", tags=["General"])
async def select_programa_y_area_de_convocatoria(id_conv: int):
    rows = call_procedure('programa_area_convocatoria', id_conv)
    return jsonable_encoder(rows)

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


@app.post("/insertar_incapacidad/{user_id}", tags=["Salud"])
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


@app.post("/insertar_atencionsalud/{user_id}", tags=["Salud"])
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

# ----------------------------------------------- DEPORTE --------------------------------------------------------


@app.get("/torneosinternos", tags=["Deporte"])
async def select_torneosinternos():
    rows = call_procedure('sp_consultar_torneos_internos')
    return jsonable_encoder(rows)


@app.get("/cursoslibres", tags=["Deporte"])
async def select_cursoslibres():
    rows = call_procedure('sp_consultar_convocatoria_cursos_libres')
    return jsonable_encoder(rows)


@app.get("/taller", tags=["Deporte"])
async def select_taller(id_eve_ta: int):
    rows = call_procedure('pas_consultar_info_eventoTaller', id_eve_ta)
    return jsonable_encoder(rows)


@app.get("/proyecto", tags=["Deporte"])
async def select_proyecto(id_proy: int):
    rows = call_procedure('pas_consultar_info_proyecto', id_proy)
    return jsonable_encoder(rows)


@app.post("/participar_convocatoria", tags=["Deporte"])
async def add_convocatoria(cedula: int, id_conv: int, fecha_inscripcion: datetime.datetime):
    rows = call_procedure('pas_participar_convocatoria', cedula, id_conv, fecha_inscripcion)
    if len(rows) == 0:
        return jsonable_encoder({'Key': 0, 'Answer': 'Done'})
    return jsonable_encoder(rows)


@app.get("/convocatorias_usuario", tags=["Deporte"])
async def select_convocatorias_usuario(cedula: int):
    rows = call_procedure('sp_consultar_mis_convocatorias', cedula)
    return jsonable_encoder(rows)


@app.get("/taller_evento_proyecto_deportes", tags=["Deporte"])
async def select_all_deportes(id_programa: int):
    rows = call_procedure('pas_consultar_eventoTaller_programa', id_programa)
    return jsonable_encoder(rows)


@app.get("/convocatorias_deporte", tags=["Deporte"])
async def select_conv_deportes(id_programa: int):
    rows = call_procedure('sp_consultar_convocatorias_deporte', id_programa)
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
    rows = call_procedure("sp_horas_corresponsabilidad_est", user_id)
    return jsonable_encoder(rows)


@app.get("/pbm_estudiante/{user_id}", tags=["Económico"])
async def select_pbm_estudiante(user_id: int):
    rows = call_procedure("sp_pbm_est", user_id)
    return jsonable_encoder(rows)

# Convocatorias


@app.get("/conv_fomento_emprendimiento/{user_id}", tags=["Económico"])
async def select_conv_fomento_emprendimiento_filtro(user_id: int, nombre: str = None, tema: str = None):
    rows = call_procedure("sp_convocatoriafomentoemprendimiento_filtro", nombre, tema)
    return jsonable_encoder(rows)
  
# ----------------------------------------------------------------------------------------------------------


@app.get("/conv_gestion_alimentaria/{user_id}", tags=["Económico"])
async def select_conv_gestion_alimentaria_filtro(user_id: int, comida: str = None, lugar: str = None):
    rows = call_procedure("sp_convocatoriagestionalimentaria_filtro", user_id, comida, lugar)
    return jsonable_encoder(rows)
  

# ----------------------------------------------------------------------------------------------------------

@app.get("/conv_gestion_alojamiento/{user_id}", tags=["Económico"])
async def select_conv_gestion_alojamiento_filtro(user_id: int, localidad: str = None, tipo: str = None):
    rows = call_procedure("sp_convocatoriagestionalojamiento_filtro", user_id, localidad, tipo)
    return jsonable_encoder(rows)

# ----------------------------------------------------------------------------------------------------------


@app.get("/conv_gestion_economica/{user_id}", tags=["Económico"])
async def select_conv_gestion_economica_filtro(user_id: int, filter_min: float = None, filter_max: float = None):
    rows = call_procedure("sp_convocatoriagestioneconomica_filtro", user_id, filter_min, filter_max)
    return jsonable_encoder(rows)

# ----------------------------------------------------------------------------------------------------------


@app.get("/conv_gestion_transporte/{user_id}", tags=["Económico"])
async def select_conv_gestion_transporte_filtro(user_id: int, tipo: str = None):
    rows = call_procedure("sp_convocatoriagestiontransporte_filtro", user_id, tipo)
    return jsonable_encoder(rows)


# Tienda Bienestar UN-------------------------------------------------------------------------------------


@app.get("/info_factura/{user_id}", tags=["Económico-Tienda"])
async def select_info_factura_tienda(user_id: int, tienda_id: int = None, factura_id: int = None):
    rows = call_procedure("sp_info_factura_per", user_id, tienda_id, factura_id)
    return jsonable_encoder(rows)


@app.get("/productos_tienda", tags=["Económico-Tienda"])
async def select_productos_tienda(tienda_id: int = None):
    rows = call_procedure("sp_productos_tienda", tienda_id)
    return jsonable_encoder(rows)


@app.get("/tiendas_producto", tags=["Económico-Tienda"])
async def select_tiendas_producto(producto_id: int | None):
    rows = call_procedure("sp_tiendas_ofrece_producto", producto_id)
    return jsonable_encoder(rows)


@app.post("/insertar_factura", tags=["Económico-Tienda"])
async def insertar_factura(cliente_ID: int | None, detalle: str = "N.A", tienda_ID: int = 1):
    fact_ID = 0
    rows = call_procedure("insertar_factura", detalle, tienda_ID, cliente_ID, fact_ID)
    if len(rows) == 0:
        return jsonable_encoder({'Key': 0, 'Answer': 'Done'})
    return jsonable_encoder(rows)


@app.post("/insertar_producto_factura", tags=["Económico-Tienda"])
async def insertar_producto_en_factura(factura_ID: int | None, producto_ID: int | None):
    rows = call_procedure("sp_insertar_prod_factura", factura_ID, producto_ID)
    if len(rows) == 0:
        return jsonable_encoder({'Key': 0, 'Answer': 'Done'})
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
