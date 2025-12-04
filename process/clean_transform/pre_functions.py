
from venv import logger

import duckdb


def _es_cedula_valida() -> bool:
    logger.info("|-- Agregando función es_cedula_valida a DuckDB...")  
    query = f"""
            CREATE OR REPLACE MACRO es_cedula_valida(cedula_str) AS
            CASE 
                WHEN cedula_str IS NULL THEN FALSE
                WHEN TRIM(cedula_str) = '' THEN FALSE
                WHEN length(TRIM(cedula_str)) != 10 THEN FALSE
                WHEN try_cast(TRIM(cedula_str) AS BIGINT) IS NULL THEN FALSE
                WHEN try_cast(substr(TRIM(cedula_str), 1, 2) AS INT) IS NULL THEN FALSE
                WHEN try_cast(substr(TRIM(cedula_str), 1, 2) AS INT) NOT BETWEEN 1 AND 24 
                    AND try_cast(substr(TRIM(cedula_str), 1, 2) AS INT) != 30 THEN FALSE
                WHEN try_cast(substr(TRIM(cedula_str), 3, 1) AS INT) IS NULL THEN FALSE
                WHEN try_cast(substr(TRIM(cedula_str), 3, 1) AS INT) >= 6 THEN FALSE
                ELSE 
                    (
                        (10 - (
                            (
                                -- Posiciones Impares (1,3,5,7,9) se multiplican por 2. Si es >= 10, se resta 9.
                                (CASE WHEN try_cast(substr(TRIM(cedula_str), 1, 1) as int) * 2 >= 10 THEN try_cast(substr(TRIM(cedula_str), 1, 1) as int) * 2 - 9 ELSE try_cast(substr(TRIM(cedula_str), 1, 1) as int) * 2 END) +
                                (CASE WHEN try_cast(substr(TRIM(cedula_str), 3, 1) as int) * 2 >= 10 THEN try_cast(substr(TRIM(cedula_str), 3, 1) as int) * 2 - 9 ELSE try_cast(substr(TRIM(cedula_str), 3, 1) as int) * 2 END) +
                                (CASE WHEN try_cast(substr(TRIM(cedula_str), 5, 1) as int) * 2 >= 10 THEN try_cast(substr(TRIM(cedula_str), 5, 1) as int) * 2 - 9 ELSE try_cast(substr(TRIM(cedula_str), 5, 1) as int) * 2 END) +
                                (CASE WHEN try_cast(substr(TRIM(cedula_str), 7, 1) as int) * 2 >= 10 THEN try_cast(substr(TRIM(cedula_str), 7, 1) as int) * 2 - 9 ELSE try_cast(substr(TRIM(cedula_str), 7, 1) as int) * 2 END) +
                                (CASE WHEN try_cast(substr(TRIM(cedula_str), 9, 1) as int) * 2 >= 10 THEN try_cast(substr(TRIM(cedula_str), 9, 1) as int) * 2 - 9 ELSE try_cast(substr(TRIM(cedula_str), 9, 1) as int) * 2 END) +
                                -- Posiciones Pares (2,4,6,8) se suman tal cual
                                try_cast(substr(TRIM(cedula_str), 2, 1) as int) +
                                try_cast(substr(TRIM(cedula_str), 4, 1) as int) +
                                try_cast(substr(TRIM(cedula_str), 6, 1) as int) +
                                try_cast(substr(TRIM(cedula_str), 8, 1) as int)
                            ) % 10
                        )) % 10
                    ) = try_cast(substr(TRIM(cedula_str), 10, 1) as int)
            END;
        """
    conn = duckdb.connect('resources/data_lake/vacunacion.duckdb')
    conn.execute(query)
    
def _fn_eliminar_caracteres_especiales() -> str:
    logger.info("|-- Agregando función eliminar_caracteres_especiales a DuckDB...")  
    query = """
        CREATE OR REPLACE MACRO eliminar_caracteres_especiales(input_str) AS
        UPPER(
            REGEXP_REPLACE(
            REGEXP_REPLACE(input_str, '[^a-zA-ZáéíóúÁÉÍÓÚñÑüÜ\\s]', '', 'g'),
            '\\s+', ' ', 'g'
            )
        );
        """
    conn = duckdb.connect('resources/data_lake/vacunacion.duckdb')
    conn.execute(query)
    
    
def _fn_obtener_solo_numeros():
    logger.info("|-- Agregando función obtener_solo_numeros a DuckDB...")
    query = """
        CREATE OR REPLACE MACRO obtener_solo_numeros(input_str) AS
        REGEXP_REPLACE(input_str, '[^0-9]', '', 'g');
        """
    conn = duckdb.connect('resources/data_lake/vacunacion.duckdb')
    conn.execute(query)
    
    
def agregar_funciones_utilitarias():
    logger.info("|- AGREGANDO FUNCIONES UTILITARIAS")
    _es_cedula_valida()
    _fn_eliminar_caracteres_especiales()
    _fn_obtener_solo_numeros()