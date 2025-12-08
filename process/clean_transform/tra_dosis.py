import logging
from process.clean_transform.utils import ejecutar_query


def _homogeniziar_dosis():
    logging.info("|-- Homogenizando valores de dosis_aplicada")
    query = """
        UPDATE db_vacunacion_covid
        SET dosis_aplicada = CASE 
                WHEN UPPER(dosis_aplicada) = 'PRIMERA' THEN '1A DOSIS'
                WHEN UPPER(dosis_aplicada) = 'SEGUNDA' THEN '2A DOSIS'
                ELSE dosis_aplicada
            END,
            proceso_auditoria = CONCAT(proceso_auditoria, '| DOS_001')
        WHERE UPPER(dosis_aplicada) IN ('PRIMERA', 'SEGUNDA');
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query
    )
    
def _update_dosis_tercer_refuerzo():
    logging.info("|-- Actualizando dosis_aplicada para tercer refuerzo y dosis bivalente")
    query = """
        UPDATE db_vacunacion_covid
        SET dosis_aplicada = CASE 
                WHEN (dosis_aplicada = 'REFUERZO 3' OR dosis_aplicada = 'PENDIENTE' or dosis_aplicada = 'UNICA') 
                    AND EXTRACT(YEAR FROM fecha_aplicacion) = 2023 
                    THEN 'BIVALENTE'
                WHEN dosis_aplicada = 'DOSIS ADICIONAL' 
                    AND EXTRACT(YEAR FROM fecha_aplicacion) = 2022 
                    THEN 'BIVALENTE'
                WHEN extract(year from fecha_aplicacion) = 2024 and (dosis_aplicada = 'REFUERZO 3' or dosis_aplicada is null)
                    THEN 'ESTACIONARIA 2024'
                ELSE dosis_aplicada 
            END,
            proceso_auditoria = CONCAT(proceso_auditoria, '| DOS_002')
        WHERE dosis_aplicada IN ('TERCER REFUERZO', 'PENDIENTE', 'REFUERZO 3', 'UNICA', 'DOSIS ADICIONAL') or dosis_aplicada is null;
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query
    )
    
    
def _update_dosis_dosis_2():
    logging.info("|-- Actualizando dosis_aplicada para segunda dosis")
    """
    Actualiza el campo dosis_aplicada del registro 2 a '2A DOSIS' cuando:
    - El campo dosis_aplicada del registro 1 es '1A DOSIS'
    - El campo dosis_aplicada del registro 2 es NO es '2A DOSIS'
    - El campo dosis_aplicada del registro 3 es 'REFUERZO 1'
    Ordenados por fecha_aplicacion de forma ascendente por num_iden.
    solo los registros que cumplen con estas condiciones.
    """
    query = """
        WITH RankedDosis AS (
            SELECT 
                rowid,
                dosis_aplicada,
                ROW_NUMBER() OVER (PARTITION BY num_iden ORDER BY fecha_aplicacion ASC) AS rn,
                LEAD(dosis_aplicada, 1) OVER (PARTITION BY num_iden ORDER BY fecha_aplicacion ASC) AS siguiente_dosis,
                LAG(dosis_aplicada, 1) OVER (PARTITION BY num_iden ORDER BY fecha_aplicacion ASC) AS anterior_dosis
            FROM db_vacunacion_covid
        )
        UPDATE db_vacunacion_covid
        SET dosis_aplicada = '2A DOSIS',
            proceso_auditoria = CONCAT(proceso_auditoria, '| DOS_003')
        WHERE rowid IN (
            SELECT rowid
            FROM RankedDosis
            WHERE dosis_aplicada != '2A DOSIS'
                AND anterior_dosis = '1A DOSIS'
                AND siguiente_dosis = 'REFUERZO 1'
        );
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query
    )
   

def _update_dosis_refuerzo_1():
    logging.info("|-- Actualizando dosis_aplicada para primer refuerzo")
    """
    Actualiza el campo dosis_aplicada del registro 2 a 'REFUERZO 1' cuando:
    - El campo dosis_aplicada del registro 1 es '2A DOSIS'
    - El campo dosis_aplicada del registro 2 es NO es 'REFUERZO 1'
    - El campo dosis_aplicada del registro 3 es 'REFUERZO 2'
    Ordenados por fecha_aplicacion de forma ascendente por num_iden.
    solo los registros que cumplen con estas condiciones.
    """
    query = """
        WITH RankedDosis AS (
            SELECT 
                rowid,
                dosis_aplicada,
                ROW_NUMBER() OVER (PARTITION BY num_iden ORDER BY fecha_aplicacion ASC) AS rn,
                LEAD(dosis_aplicada, 1) OVER (PARTITION BY num_iden ORDER BY fecha_aplicacion ASC) AS siguiente_dosis,
                LAG(dosis_aplicada, 1) OVER (PARTITION BY num_iden ORDER BY fecha_aplicacion ASC) AS anterior_dosis
            FROM db_vacunacion_covid
        )
        UPDATE db_vacunacion_covid
        SET dosis_aplicada = 'REFUERZO 1',
            proceso_auditoria = CONCAT(proceso_auditoria, '| DOS_004')
        WHERE rowid IN (
            SELECT rowid
            FROM RankedDosis
            WHERE dosis_aplicada != 'REFUERZO 1'
                AND anterior_dosis = '2A DOSIS'
                AND siguiente_dosis = 'REFUERZO 2'
        );
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query
    )

def _update_dosis_refuerzo_2():
    """
    Actualiza el campo dosis_aplicada del registro 2 a 'REFUERZO 2' cuando:
    - El campo dosis_aplicada del registro 1 es 'REFUERZO 1'
    - El campo dosis_aplicada del registro 2 es NO es 'REFUERZO 2'
    - El campo dosis_aplicada del registro 3 es 'BIVALENTE'
    Ordenados por fecha_aplicacion de forma ascendente por num_iden.
    solo los registros que cumplen con estas condiciones.
    """
    logging.info("|-- Actualizando dosis_aplicada para segundo refuerzo")
    query = """
        WITH RankedDosis AS (
            SELECT 
                rowid,
                dosis_aplicada,
                ROW_NUMBER() OVER (PARTITION BY num_iden ORDER BY fecha_aplicacion ASC) AS rn,
                LEAD(dosis_aplicada, 1) OVER (PARTITION BY num_iden ORDER BY fecha_aplicacion ASC) AS siguiente_dosis,
                LAG(dosis_aplicada, 1) OVER (PARTITION BY num_iden ORDER BY fecha_aplicacion ASC) AS anterior_dosis
            FROM db_vacunacion_covid
        )
        UPDATE db_vacunacion_covid
        SET dosis_aplicada = 'REFUERZO 2',
            proceso_auditoria = CONCAT(proceso_auditoria, '| DOS_005')
        WHERE rowid IN (
            SELECT rowid
            FROM RankedDosis
            WHERE dosis_aplicada != 'REFUERZO 2'
                AND anterior_dosis = 'REFUERZO 1'
                AND siguiente_dosis = 'BIVALENTE'
        );
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query
    )

def dosis_orchester():
    _homogeniziar_dosis()
    _update_dosis_tercer_refuerzo()
    _update_dosis_dosis_2()
    _update_dosis_refuerzo_1()
    _update_dosis_refuerzo_2()
