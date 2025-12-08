from extract.extraccion_oracle_simple import logger
import logging
from process.clean_transform.utils import crear_colulmna_en_tabla, crear_columna_en_tabla_si_no_existe, ejecutar_query

DIC_FASES = [
    {'nombre': 'FASE 0', 'inicio': '2021-01-18', 'fin': '2021-05-23'},
    {'nombre': 'FASE 1', 'inicio': '2021-05-24', 'fin': '2021-06-14'},
    {'nombre': 'FASE 2', 'inicio': '2021-06-15', 'fin': '2021-07-11'},
    {'nombre': 'FASE 3', 'inicio': '2021-07-12', 'fin': '2021-09-05'},
    {'nombre': 'FASE 4', 'inicio': '2021-09-06', 'fin': '2021-09-12'},    
    ## aquí hay un salto, 15 DE OCTUBRE DE 2021, AL 30 DE MARZO DE 2022 NO HAY FASES DEFINIDAS
    {'nombre': 'PRIMERA REFUERZO', 'inicio': '2021-10-15', 'fin': '2022-03-30'},
    {'nombre': 'SEGUNDO REFUERZO', 'inicio': '2022-04-01', 'fin': '2022-12-31'},
    ## aquí hay un salto
    {'nombre': 'VACUNACIÓN BIVALENTE 2023', 'inicio': '2023-01-01', 'fin': '2023-12-31'},
    {'nombre': 'VACUNACIÓN ESTACIONARIA CONTRA COVID-19, 2024', 'inicio': '2024-01-01', 'fin': '2024-12-31'},
    {'nombre': 'VACUNACIÓN ESTACIONARIA CONTRA COVID 19, 2025', 'inicio': '2025-01-01', 'fin': '2025-12-31'}
]

def _imputar_fases_orchester():
    logging.info("|-- Imputando fases de vacunación")
    crear_columna_en_tabla_si_no_existe(
        db_name='resources/data_lake/vacunacion.duckdb',
        tabla='db_vacunacion_covid',
        columna='fase_depurada_2',
        tipo='VARCHAR'
    )
    # Construir el query con un CASE para cada fase
    cases = []
    for fase in DIC_FASES:
        cases.append(f"WHEN fecha_aplicacion >= DATE '{fase['inicio']}' AND fecha_aplicacion <= DATE '{fase['fin']}' THEN '{fase['nombre']}'")
    
    cases_str = "\n        ".join(cases)
    
    query = f"""
    UPDATE db_vacunacion_covid
    SET fase_depurada_2 = CASE 
        {cases_str}
        ELSE fase_depurada_2 
    END
    WHERE fecha_aplicacion IS NOT NULL
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query
    )

def _organizar_columnas():
    logging.info("|-- Organizando columnas de fase de vacunación")
    query = """
    ALTER TABLE db_vacunacion_covid
    DROP COLUMN IF EXISTS fase_vacuna;
    ALTER TABLE db_vacunacion_covid
    DROP COLUMN IF EXISTS fase_vacuna_depurada;
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query
    )
    # renombrar la nueva columna
    query_rename = """
    ALTER TABLE db_vacunacion_covid
    RENAME COLUMN fase_depurada_2 TO fase_vacuna_depurada;
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query_rename
    )

def fases_orchester(desde: str, hasta: str):
    """Imputar fases de vacunación basadas en rangos de fechas"""
    logging.info("|- TRATAMIENTO DE FASES DE VACUNACIÓN")
    _imputar_fases_orchester()
    _organizar_columnas()