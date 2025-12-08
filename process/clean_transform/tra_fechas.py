import logging

from process.clean_transform.utils import ejecutar_query

logger = logging.getLogger(__name__)


def _tratamiento_registros_1900_rows():    
    logger.info("|-- Tratando registros con fecha_aplicacion en 1900")
    query = f"""
    WITH fecha_establecimiento_moda AS (
       SELECT unicodigo,
               mode(fecha_aplicacion) as fecha_aplicacion
        FROM db_vacunacion_covid
        WHERE fecha_aplicacion >= '2021-01-01' AND fecha_aplicacion < '2022-12-31'
        GROUP BY unicodigo
    )
    UPDATE db_vacunacion_covid 
    SET fecha_aplicacion = f.fecha_aplicacion,
        anio_aplicacion = EXTRACT(YEAR FROM f.fecha_aplicacion),
        mes_aplicacion = EXTRACT(MONTH FROM f.fecha_aplicacion),
        dia_aplicacion = EXTRACT(DAY FROM f.fecha_aplicacion),
        proceso_auditoria = concat(proceso_auditoria, '| TRA_1900')
    FROM fecha_establecimiento_moda f
    WHERE db_vacunacion_covid.unicodigo = f.unicodigo
    AND (db_vacunacion_covid.fecha_aplicacion < '2021-01-01' OR db_vacunacion_covid.fecha_aplicacion > '2025-01-01')
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query
    )

def _completar_anio_mes_dia_aplicacion():
    logger.info("|-- Completando anio, mes y dia de aplicacion a partir de fecha_aplicacion")
    query = f"""
    UPDATE db_vacunacion_covid
    SET anio_aplicacion = EXTRACT(YEAR FROM fecha_aplicacion),
        mes_aplicacion = EXTRACT(MONTH FROM fecha_aplicacion),
        dia_aplicacion = EXTRACT(DAY FROM fecha_aplicacion),
        proceso_auditoria = concat(proceso_auditoria, '| TRA_FECHA_001')
    WHERE (anio_aplicacion IS NULL OR mes_aplicacion IS NULL OR dia_aplicacion IS NULL)
    AND fecha_aplicacion IS NOT NULL;
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query
    )

def _eliminar_registros_sin_fecha_aplicacion():
    logger.info("|-- Eliminando registros sin fecha_aplicacion válida")
    query = f"""
    DELETE FROM db_vacunacion_covid
    WHERE fecha_aplicacion IS NULL or TRIM(fecha_aplicacion::varchar) = '';
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query
    )
    
def _eliminar_fecha_aplicacion_none():
    logger.info("|-- Eliminando registros con fecha_aplicacion None")
    query = """
    DELETE FROM db_vacunacion_covid
    WHERE fecha_aplicacion IS NULL;
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query
    )

def _tratamiento_maximo_dia_mes():
    logger.info("|-- Tratando días y meses fuera de rango")
    query_mes = f"""
    UPDATE db_vacunacion_covid
    SET mes_aplicacion = CASE 
            WHEN mes_aplicacion > 12 THEN 12
            WHEN mes_aplicacion < 1 THEN 1
            ELSE mes_aplicacion
        END,
        proceso_auditoria = concat(proceso_auditoria, '| TRA_FECHA_002_MES')
    WHERE mes_aplicacion IS NOT NULL AND (mes_aplicacion > 12 OR mes_aplicacion < 1);
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query_mes
    )
    
    # Luego corregir días fuera de rango para cada mes
    query_dia = f"""
    UPDATE db_vacunacion_covid
    SET dia_aplicacion = CASE 
            WHEN dia_aplicacion > DAY(LAST_DAY(MAKE_DATE(CAST(anio_aplicacion AS BIGINT), CAST(mes_aplicacion AS BIGINT), 1))) 
            THEN DAY(LAST_DAY(MAKE_DATE(CAST(anio_aplicacion AS BIGINT), CAST(mes_aplicacion AS BIGINT), 1)))
            WHEN dia_aplicacion < 1 THEN 1
            ELSE dia_aplicacion
        END,
        proceso_auditoria = concat(proceso_auditoria, '| TRA_FECHA_002_DIA')
    WHERE dia_aplicacion IS NOT NULL 
           AND anio_aplicacion IS NOT NULL 
           AND mes_aplicacion IS NOT NULL
           AND mes_aplicacion BETWEEN 1 AND 12
           AND anio_aplicacion BETWEEN 1900 AND 2100
           AND (dia_aplicacion < 1 
                OR dia_aplicacion > DAY(LAST_DAY(MAKE_DATE(CAST(anio_aplicacion AS BIGINT), CAST(mes_aplicacion AS BIGINT), 1))));
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query_dia
    )

def _asignar_fecha_aplicacion_desde_componentes():
    logger.info("|-- Asignando fecha_aplicacion desde anio, mes y dia de aplicacion")
    query = f"""
    UPDATE db_vacunacion_covid
    SET fecha_aplicacion = TRY_CAST(
        LPAD(anio_aplicacion::varchar, 4, '0') || '-' ||
        LPAD(mes_aplicacion::varchar, 2, '0') || '-' ||
        LPAD(dia_aplicacion::varchar, 2, '0') AS DATE)
    WHERE (fecha_aplicacion IS NULL or TRIM(fecha_aplicacion::varchar) = '' or fecha_aplicacion = '1900-01-01')
    AND (anio_aplicacion IS NOT NULL 
    AND mes_aplicacion IS NOT NULL 
    AND dia_aplicacion IS NOT NULL
    AND anio_aplicacion BETWEEN 1900 AND 2100
    AND mes_aplicacion BETWEEN 1 AND 12
    AND dia_aplicacion BETWEEN 1 AND 31);
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query
    )

def _eliminar_dhis2_registros_1900():
    logger.info("|-- Eliminando registros DHIS2 con anio_aplicacion en 1900")
    query = f"""
    DELETE FROM db_vacunacion_covid
    WHERE (anio_aplicacion = 1900 OR anio_aplicacion IS NULL);
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query
    )
    
def _remove_invalid_dates():
    logger.info("|-- Removing records with invalid dates")
    query = f"""
    DELETE FROM db_vacunacion_covid
    WHERE fecha_aplicacion > '2025-01-01' OR fecha_aplicacion > CURRENT_DATE;
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query
    )

def fechas_tratamiento_orchester(since: str, until: str):
    logger.info("|- TRATAMIENTO FECHAS")
    _remove_invalid_dates()
    _eliminar_fecha_aplicacion_none()
    _tratamiento_maximo_dia_mes()
    _asignar_fecha_aplicacion_desde_componentes()
    _eliminar_registros_sin_fecha_aplicacion()
    _tratamiento_registros_1900_rows()   
    _completar_anio_mes_dia_aplicacion()
    _eliminar_dhis2_registros_1900()