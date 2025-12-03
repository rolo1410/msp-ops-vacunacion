
from venv import logger

import duckdb


def _es_cedula_valida() -> bool:    
    query = f"""
            CREATE OR REPLACE MACRO es_cedula_valida(cedula_str) AS
            CASE 
                WHEN length(cedula_str) != 10 OR try_cast(cedula_str AS BIGINT) IS NULL THEN FALSE
                WHEN cast(substr(cedula_str, 1, 2) AS INT) NOT BETWEEN 1 AND 24 
                    AND cast(substr(cedula_str, 1, 2) AS INT) != 30 THEN FALSE
                WHEN cast(substr(cedula_str, 3, 1) AS INT) >= 6 THEN FALSE
                ELSE 
                    (
                        (10 - (
                            (
                                -- Posiciones Impares (1,3,5,7,9) se multiplican por 2. Si es >= 10, se resta 9.
                                (CASE WHEN cast(substr(cedula_str, 1, 1) as int) * 2 >= 10 THEN cast(substr(cedula_str, 1, 1) as int) * 2 - 9 ELSE cast(substr(cedula_str, 1, 1) as int) * 2 END) +
                                (CASE WHEN cast(substr(cedula_str, 3, 1) as int) * 2 >= 10 THEN cast(substr(cedula_str, 3, 1) as int) * 2 - 9 ELSE cast(substr(cedula_str, 3, 1) as int) * 2 END) +
                                (CASE WHEN cast(substr(cedula_str, 5, 1) as int) * 2 >= 10 THEN cast(substr(cedula_str, 5, 1) as int) * 2 - 9 ELSE cast(substr(cedula_str, 5, 1) as int) * 2 END) +
                                (CASE WHEN cast(substr(cedula_str, 7, 1) as int) * 2 >= 10 THEN cast(substr(cedula_str, 7, 1) as int) * 2 - 9 ELSE cast(substr(cedula_str, 7, 1) as int) * 2 END) +
                                (CASE WHEN cast(substr(cedula_str, 9, 1) as int) * 2 >= 10 THEN cast(substr(cedula_str, 9, 1) as int) * 2 - 9 ELSE cast(substr(cedula_str, 9, 1) as int) * 2 END) +
                                -- Posiciones Pares (2,4,6,8) se suman tal cual
                                cast(substr(cedula_str, 2, 1) as int) +
                                cast(substr(cedula_str, 4, 1) as int) +
                                cast(substr(cedula_str, 6, 1) as int) +
                                cast(substr(cedula_str, 8, 1) as int)
                            ) % 10
                        )) % 10
                    ) = cast(substr(cedula_str, 10, 1) as int)
            END;
        """
    conn = duckdb.connect('resources/data_lake/vacunacion.duckdb')
    conn.execute(query)
    
def _fn_eliminar_caracteres_especiales() -> str:
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
    query = """
        CREATE OR REPLACE MACRO obtener_solo_numeros(input_str) AS
        REGEXP_REPLACE(input_str, '[^0-9]', '', 'g');
        """
    conn = duckdb.connect('resources/data_lake/vacunacion.duckdb')
    conn.execute(query)
    
    
def agregar_funciones_utilitarias():
    logger.info("Agregando funciones utilitarias a DuckDB...")
    _es_cedula_valida()
    _fn_eliminar_caracteres_especiales()
    _fn_obtener_solo_numeros()