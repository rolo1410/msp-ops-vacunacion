import logging
from lake.init_lake import add_new_elements_to_lake
from lake.load_lake import load_data, load_data_paginated
from process.clean_transform.add_data import add_data_orchester
from process.clean_transform.clean_global import clean_orchester
from process.clean_transform.dim_persona import persona_orchester
from process.clean_transform.dim_vacunacion import vacunacion_orchester
from process.clean_transform.imp_fase import fases_orchester
from process.clean_transform.pre_configuracion import configuracion_orchester
from process.clean_transform.pre_functions import agregar_funciones_utilitarias
from process.clean_transform.tra_duplicados import eliminar_duplicados_orchester, eliminar_duplicados_orchester_final
from process.clean_transform.tra_fechas import fechas_tratamiento_orchester
from process.clean_transform.utils import ejecutar_query
from process.clean_transform.tra_establecimientos import establecimientos_orchester
from process.clean_transform.tra_dosis import dosis_orchester

tabla_pesos = {
    "id_vac_depu": 0,
    "anio_aplicacion": 1.9996,
    "mes_aplicacion": 0.9998,
    "dia_aplicacion": 0.9998,
    "fecha_aplicacion": 3,
    "punto_vacunacion": 0,
    "unicodigo": 3,
    "uni_nombre": 1,
    "zona": 1,
    "distrito": 1,
    "provincia": 1,
    "canton": 1,
    "apellidos": 0,
    "nombres": 0,
    "nombres_completos": 0,
    "tipo_iden": 2,
    "num_iden": 2.9997,
    "sexo": 2,
    "anio_nacimiento": 0.9999,
    "mes_nacimiento": 1,
    "dia_nacimiento": 1,
    "fecha_nacimiento": 3,
    "nacionalidad": 0,
    "etnia": 1.9264,
    "pobla_vacuna": 1.1958,
    "grupo_riesgo": 0.0856,
    "nombre_vacuna": 2.9994,
    "lote_vacuna": 1.9902,
    "dosis_aplicada": 2.9718,
    "profesional_aplica": 0.9967,
    "iden_profesional_aplica": 0.9839,
    "fase_vacuna": 0,
    "fase_vacuna_depurada": 1.9236,
    "grupo_riesgo_depurada": 1.2026,
    "edad_anios": 0,
    "sistema": 2,
    "registro_civil": 0,   
    "id_vac_cons": 0,
}

def _asignar_pesos_completitud():
    """Crear una columna con el peso de completitud de los datos"""
    query = """
    ALTER TABLE db_vacunacion_covid
    ADD COLUMN peso_completitud FLOAT;
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query
    )
    
    # Construir la query usando los valores del diccionario
    case_statements = []
    for column, weight in tabla_pesos.items():
        case_statements.append(f"CASE WHEN {column} IS NOT NULL THEN {weight} ELSE 0 END")
    
    query_update = f"""
    UPDATE db_vacunacion_covid
    SET peso_completitud = COALESCE({' + '.join(case_statements)}, 0);
    """
    
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query_update
    )    

def _conteo_registros():
    query_inicio = """
    SELECT COUNT(*) FROM db_vacunacion_covid;
    """
    result_inicio = ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query_inicio
    )
    
    logging.info(f"|-- Total de registros: {result_inicio[0][0]}")
    return result_inicio[0][0]
    

def process_all_data_paginated(desde, hasta):
    configuracion_orchester()
    
    ##
    total_inicio = _conteo_registros()
    establecimientos_orchester()
    fechas_tratamiento_orchester(desde, hasta)
    eliminar_duplicados_orchester()
    persona_orchester()
    _asignar_pesos_completitud()
    fases_orchester(desde, hasta)
    clean_orchester(desde, hasta)
    add_data_orchester()
    eliminar_duplicados_orchester_final()
    dosis_orchester()
    
    ##
    total_inicio_final = _conteo_registros()
    logging.info(f"|-- Total de registros: {total_inicio_final} (antes: {total_inicio}) diferencia: {total_inicio_final - total_inicio})")