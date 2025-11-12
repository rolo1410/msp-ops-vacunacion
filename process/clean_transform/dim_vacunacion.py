from venv import logger

import polars

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
            ELSE 'DESCONOCE'
            END
        WHERE vacuna_nom_comercial IS NULL;
    """   
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query
    )
    logger.info("Población de columna vacuna_nom_comercial completada.")

def vacunacion_orchester():
    _compute_nombre_comercial_vacuna()