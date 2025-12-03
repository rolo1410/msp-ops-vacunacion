from venv import logger

from process.clean_transform.utils import ejecutar_query


def _compute_nombre_comercial_vacuna():
    query_create_colum= "alter table db_vacunacion_covid add column vacuna_nom_comercial varchar;"
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query_create_colum
    )
    logger.info("Adición de columna vacuna_nom_comercial.")
    query ="""
         UPDATE db_vacunacion_covid
        SET vacuna_nom_comercial = CASE
            WHEN nombre_vacuna ILIKE '%OMICROM,PFIZER%' THEN 'PFIZER'
            WHEN nombre_vacuna ILIKE '%SINOPHARM,SINOPHARM%' THEN 'SINOPHARM'
            WHEN nombre_vacuna ILIKE '%SPIKEVAX 6MESES A 4AÑOS%' THEN 'SPIKEVAX'
            WHEN nombre_vacuna ILIKE '%SPUTNIK V%' THEN 'SPUTNIK V'
            WHEN nombre_vacuna ILIKE '%SPIKEVAX MAYOR DE 5 AÑOS%' THEN 'SPIKEVAX'
            WHEN nombre_vacuna ILIKE '%REFUERZO 2%' THEN 'DESCONOCE'
            WHEN nombre_vacuna ILIKE '%SOBERANA 02%' THEN 'SOBERANA 02'
            WHEN nombre_vacuna ILIKE '%HB ADULTO%' THEN 'NO APLICA'
            WHEN nombre_vacuna ILIKE '%SPIKEVAX 6 MESES CON INMUNOSUPRESIÓN%' THEN 'SPIKEVAX'
            WHEN nombre_vacuna ILIKE '%SOBERANA 02 PLUS%' THEN 'SOBERANA 02 PLUS'
            WHEN nombre_vacuna ILIKE '%OTRA VACUNA%' THEN 'OTRA VACUNA'
            WHEN nombre_vacuna ILIKE '%MODERNA EXTERIOR%' THEN 'SPIKEVAX'
            WHEN nombre_vacuna ILIKE '%CORONAVAC SINOVAC%' THEN 'SINOVAC'
            WHEN nombre_vacuna ILIKE '%CANSINO%' THEN 'CANSINO'
            WHEN nombre_vacuna ILIKE '%JANSSEN%' THEN 'JANSSEN'
            WHEN nombre_vacuna ILIKE '%SPIKEVAX CON HISTORIAL VACUNAL%' THEN 'SPIKEVAX'
            WHEN nombre_vacuna ILIKE '%ASTRAZENECA%' THEN 'ASTRAZENECA'
            WHEN nombre_vacuna ILIKE '%BNT162B2 PFIZER%' THEN 'PFIZER'
            WHEN nombre_vacuna ILIKE '%SPIKEVAX 3 A 11 AÑOS CON HISTORIAL VACUNAL CRÓNICOS%' THEN 'SPIKEVAX'
            WHEN nombre_vacuna ILIKE '%ABDALA CIGB66%' THEN 'ABDALA CIGB66'
            WHEN nombre_vacuna IS NULL OR nombre_vacuna = '' THEN 'DESCONOCE'
            WHEN nombre_vacuna ILIKE '%COMIRNATY BIVALENTE%' THEN 'PFIZER'
            WHEN nombre_vacuna ILIKE '%REFUERZO 1%' THEN 'DESCONOCE'
            WHEN nombre_vacuna ILIKE '%HB ADULTO%' THEN 'SINOVAC'
            ELSE 'DESCONOCE'
            END
        WHERE vacuna_nom_comercial IS NULL;
    """   
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query
    )
    logger.info("Población de columna vacuna_nom_comercial completada.")
    
def _computue_descompose_fecha_aplicacion(since: str, until: str):
    """
    Docstring para _computue_descompose_fecha_aplicacion
    
    :param since: Descripción
    :type since: str
    :param until: Descripción
    :type until: str
    """
    logger.info(f"""Iniciando descomposición de fecha_aplicacion entre {since} y {until}""")
    query= """
        UPDATE db_vacunacion_covid
        SET
            anio_aplicacion = EXTRACT(YEAR FROM fecha_aplicacion)::INT,
            mes_aplicacion = EXTRACT(MONTH FROM fecha_aplicacion)::INT,
            dia_aplicacion = EXTRACT(DAY FROM fecha_aplicacion)::INT,
            proceso_auditoria = concat(proceso_auditoria, '| VAC_001, calcular fecha_aplicacion descompuesta')
        WHERE fecha_aplicacion BETWEEN DATE '{since}' AND DATE '{until}';
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query
    )
    logger.info("""Descomposición de fecha_aplicacion completada. Año, mes y día creados.""")
    
def vacunacion_orchester(since: str, until: str):
    _compute_nombre_comercial_vacuna()
    _computue_descompose_fecha_aplicacion(since, until)