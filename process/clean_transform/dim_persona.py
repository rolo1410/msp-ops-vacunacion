
import logging

import duckdb
import polars as pl

from process.clean_transform.utils import crear_columna_en_tabla_si_no_existe, ejecutar_query


def _limpiar_columnas_texto(cols: list[str] = []):
    logging.info("|- EST Limpiando columnas de texto")
    query = f"""
        update db_vacunacion_covid 
        set 
        {','.join([f"{col} = eliminar_caracteres_especiales({col})" for col in cols])},
        proceso_auditoria = concat(proceso_auditoria, '| PER_001')
        where true;
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query
    )

def _limpiar_identificacion():
    logging.info("|- LIM Limpiando columnas identificación")
    
    ## Eliminar registros sin cédula
    query = """
    DELETE FROM db_vacunacion_covid
    WHERE num_iden IS NULL OR TRIM(num_iden) = '';
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query
    )

    ## si el registro es cedula y tiene 10 digitos
    logging.debug(f" |- EST Completando cedulas que tienen menos de 10 digitos con un 0 a la izquierda")
    query = """
    UPDATE db_vacunacion_covid
    SET num_iden = LPAD(num_iden, 10, '0'), 
        proceso_auditoria = CONCAT(proceso_auditoria, '| PER_002')
    WHERE tipo_iden = 'CÉDULA DE IDENTIDAD' AND LENGTH(num_iden) < 10;
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query
    )
       
    ## valida si las cédulas cumple con el digito verfificador crear una columna nueva
    logging.debug(f" |- Identificando cédulas válidas e inválidas")
    crear_columna_en_tabla_si_no_existe(
        db_name='resources/data_lake/vacunacion.duckdb',
        tabla='db_vacunacion_covid',
        columna='cedula_es_valida',
        tipo='boolean'
    )
    query_update= """
        UPDATE db_vacunacion_covid
        SET cedula_es_valida = es_cedula_valida(num_iden),
            proceso_auditoria = CONCAT(proceso_auditoria, '| PER_003')
        WHERE tipo_iden = 'CÉDULA DE IDENTIDAD';
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query_update
    )

def _calcular_edad():
    crear_columna_en_tabla_si_no_existe(
        db_name='resources/data_lake/vacunacion.duckdb',
        tabla='db_vacunacion_covid',
        columna='edad_anios',
        tipo='INT'
    )
    query_update= """
    UPDATE db_vacunacion_covid
    SET edad_anios = EXTRACT(YEAR FROM AGE(fecha_aplicacion, fecha_nacimiento::DATE))::INT,
        proceso_auditoria = CONCAT(proceso_auditoria, '| PER_004')
    WHERE fecha_nacimiento IS NOT NULL AND fecha_aplicacion IS NOT NULL;
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query_update
    )

def _homologar_nacionalidad():
    ## valor_origina, valor_homologado
    nacionalidades = pl.read_csv('resources/homologations/per_nacionalidad.csv')
    query = """
    UPDATE db_vacunacion_covid
    SET nacionalidad = CASE
    """
    for row in nacionalidades.iter_rows():
        valor_original = row[0]
        valor_homologado = row[1]
        query += f"WHEN nacionalidad = '{valor_original}' THEN '{valor_homologado}'\n"
    query += """
    ELSE nacionalidad
    END,
    proceso_auditoria = CONCAT(proceso_auditoria, '| PER_005')
    WHERE nacionalidad IS NOT NULL;
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query
    )
    
def _calcular_grupo_etario():
    crear_columna_en_tabla_si_no_existe(
        db_name='resources/data_lake/vacunacion.duckdb',
        tabla='db_vacunacion_covid',
        columna='grupo_etario',
        tipo='VARCHAR'
    )
    logging.debug(" |- Calculando grupos etarios basados en EDAD_ANIOS")
    query = """
    UPDATE db_vacunacion_covid
    SET grupo_etario = CASE
        WHEN edad_anios IS NULL THEN 'NO DEFINIDO'
        WHEN edad_anios <= 1 THEN 'MENOR DE 1 AÑO'
        WHEN edad_anios > 1 AND  edad_anios <= 4 THEN 'DE 1 A 4 AÑOS'
        WHEN edad_anios >= 5 AND  edad_anios <= 9 THEN 'DE 5 A 9 AÑOS'
        WHEN edad_anios >= 10 AND  edad_anios <= 14 THEN 'DE 10 A 14 AÑOS'
        WHEN edad_anios >= 15 AND  edad_anios <= 19 THEN 'DE 15 A 19 AÑOS'
        WHEN edad_anios >= 20 AND  edad_anios <= 64 THEN 'DE 20 A 64 AÑOS'
        WHEN edad_anios >= 65 THEN 'DE 65 AÑOS Y MÁS'
        ELSE 'NO DEFINIDO'
    END,
    proceso_auditoria = CONCAT(proceso_auditoria, '| PER_006')
    WHERE TRUE;
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query
    )

def _homologar_etnia():
    ## valor_origina, valor_homologado
    etnias = pl.read_csv('resources/homologations/per_etnias.csv')
    query = """
    UPDATE db_vacunacion_covid
    SET etnia = CASE
    """
    for row in etnias.iter_rows():
        valor_original = row[0]
        valor_homologado = row[1]
        query += f"WHEN etnia = '{valor_original}' THEN '{valor_homologado}'\n"
    query += """
    ELSE etnia
    END,
    proceso_auditoria = CONCAT(proceso_auditoria, '| PER_007')
    WHERE etnia IS NOT NULL;
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query
    )

def _homologar_tipo_identificacion():
    ## valor_origina, valor_homologado
    tipos_iden = pl.read_csv('resources/homologations/per_tipo_identificacion.csv')
    query = """
    UPDATE db_vacunacion_covid
    SET tipo_iden = CASE
    """
    for row in tipos_iden.iter_rows():
        valor_original = row[0]
        valor_homologado = row[1]
        query += f"WHEN tipo_iden = '{valor_original}' THEN '{valor_homologado}'\n"
    query += """
    ELSE tipo_iden
    END,
    proceso_auditoria = CONCAT(proceso_auditoria, '| PER_008')
    WHERE tipo_iden IS NOT NULL;
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query
    )

def persona_orchester():
    _limpiar_columnas_texto( cols=["tipo_iden",  "apellidos", "nombres","nombres_completos", "sexo", "etnia", "nacionalidad"])
    ##_limpiar_identificacion()
    _homologar_nacionalidad()
    _homologar_tipo_identificacion()
    _calcular_edad()
    _calcular_grupo_etario()
    _homologar_etnia()
