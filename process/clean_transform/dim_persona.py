
import logging

import polars as pl

from process.clean_transform.utils import crear_columna_en_tabla_si_no_existe, ejecutar_query


def _limpiar_num_iden():
    logging.info("|-- Limpiando num_iden eliminando espacios en blanco y caracteres especiales")
   
    caracteres = [ '>',	'_','}','¨','°','½','Ç','Í','â','ç','é','í','ï','ú','‘','’','€','™', ':', '¿', '/']
    queries = []
    for char in caracteres:
        query = """
            UPDATE db_vacunacion_covid
            SET 
                num_iden = UPPER(REPLACE(num_iden, '""" + char + """', '')),
                proceso_auditoria = CONCAT(proceso_auditoria, '| PER_001A')
            WHERE num_iden IS NOT NULL AND num_iden LIKE '%""" + char + """%'
        """
        queries.append(query)
    query = ";".join(queries)  
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query
    )
    
def _reemplazar_caracter_en_num_iden():
    logging.info("|-- Reemplazando caracteres especiales en num_iden")
    caracteres = [
            {'val': 'Á', 'to': 'A'}, 
            {'val': 'À', 'to': 'A'}, 
            {'val': 'Â', 'to': 'A'}, 
            {'val': 'É', 'to': 'E'}, 
            {'val': 'Ï', 'to': 'I'}, 
            {'val': 'Ñ', 'to': 'N'},
            {'val': 'Ó', 'to': 'O'},
            {'val': 'Ú', 'to': 'U'}
        ]
    queries = []
    for char in caracteres:
        query = f"""
            UPDATE db_vacunacion_covid
            SET num_iden = REPLACE(num_iden, '{char['val']}', '{char['to']}'),
                proceso_auditoria = CONCAT(proceso_auditoria, '| PER_001C')
            WHERE num_iden IS NOT NULL AND num_iden LIKE '%{char['val']}%';
            """
        queries.append(query)
    query = ";".join(queries)
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query
    )

def _recalcular_anio_mes_dia_nacimiento():
    logging.info("|-- Recalculando AÑO_NACIMIENTO, MES_NACIMIENTO, DIA_NACIMIENTO")
    query = """
        UPDATE db_vacunacion_covid
        SET 
            anio_nacimiento = EXTRACT(YEAR FROM fecha_nacimiento::DATE)::INT,
            mes_nacimiento = EXTRACT(MONTH FROM fecha_nacimiento::DATE)::INT,
            dia_nacimiento = EXTRACT(DAY FROM fecha_nacimiento::DATE)::INT,
            proceso_auditoria = CONCAT(proceso_auditoria, '| PER_002')
        WHERE fecha_nacimiento IS NOT NULL 
            AND (anio_nacimiento <> EXTRACT(YEAR FROM fecha_nacimiento::DATE)::INT
                OR mes_nacimiento <> EXTRACT(MONTH FROM fecha_nacimiento::DATE)::INT
                OR dia_nacimiento <> EXTRACT(DAY FROM fecha_nacimiento::DATE)::INT);
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query
    )

def _limpiar_num_iden_espacios():
    """Limpia espacios en blanco del campo num_iden y elimina registros vacíos"""
    logging.info("|-- Eliminando registros con num_iden vacío o solo espacios")
    query_delete = """
    DELETE FROM db_vacunacion_covid
    WHERE num_iden IS NULL 
       OR TRIM(num_iden::VARCHAR) = ''
       OR LOWER(TRIM(num_iden::VARCHAR)) IN ('null', 'sin informacion', 'no tiene', 'no aplica', 's/n', 's.i', '0000000000');
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query_delete
    )
    
    logging.info("|-- Limpiando todos los espacios en blanco de num_iden")
    query_clean = """
    UPDATE db_vacunacion_covid
    SET num_iden = UPPER(REGEXP_REPLACE(TRIM(num_iden::VARCHAR), '\\s+', '', 'g')),
        proceso_auditoria = CONCAT(proceso_auditoria, '| PER_001B')
    WHERE num_iden IS NOT NULL;
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query_clean
    )

def _limpiar_identificacion():
    logging.info("|-- Completando cédulas que tienen menos de 10 dígitos con un 0 a la izquierda")
    query = """
    UPDATE db_vacunacion_covid
    SET num_iden =UPPER(LPAD(num_iden, 10, '0')), 
        proceso_auditoria = CONCAT(proceso_auditoria, '| PER_003')
    WHERE tipo_iden = 'CÉDULA DE IDENTIDAD' 
    AND num_iden IS NOT NULL
    AND LENGTH(num_iden) < 10
    AND LENGTH(num_iden) > 0;
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query
    )
    ## valida si las cédulas cumple con el digito verfificador crear una columna nueva
    crear_columna_en_tabla_si_no_existe(
        db_name='resources/data_lake/vacunacion.duckdb',
        tabla='db_vacunacion_covid',
        columna='cedula_es_valida',
        tipo='boolean'
    )
    logging.info(f"|-- Identificando cédulas válidas e inválidas")
    query_update= """
        UPDATE db_vacunacion_covid
        SET cedula_es_valida = es_cedula_valida(num_iden),
            proceso_auditoria = CONCAT(proceso_auditoria, '| PER_004')
        WHERE tipo_iden = 'CÉDULA DE IDENTIDAD'
        AND num_iden IS NOT NULL
        AND LENGTH(num_iden) > 0;
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query_update
    )
    
def _asignar_tipo_cedula_a_cedulas_validas():
    logging.info("|-- Asignando tipo de identificación 'CÉDULA DE IDENTIDAD' a cédulas válidas")
    query_update= """
    UPDATE db_vacunacion_covid
    SET tipo_iden = 
        CASE 
        WHEN tipo_iden IS NULL AND es_cedula_valida(num_iden) = TRUE 
        THEN 'CÉDULA DE IDENTIDAD'
        ELSE tipo_iden
        END,
        proceso_auditoria = CONCAT(proceso_auditoria, '| PER_005')
    WHERE tipo_iden IS NULL 
    AND num_iden IS NOT NULL 
    AND LENGTH(num_iden) > 0
    AND es_cedula_valida(num_iden) = TRUE;
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query_update
    )

def _asignar_tipo_no_identificado_a_num_iden_no_nulos():
    logging.info("|-- Asignando tipo de identificación 'NO IDENTIFICADO' a num_iden no nulos y tipo_iden nulos")
    query_update= """
    UPDATE db_vacunacion_covid
    SET tipo_iden = 
        CASE 
        WHEN tipo_iden IS NULL AND num_iden IS NOT NULL AND LENGTH(num_iden) > 0
        THEN 'NO IDENTIFICADO'
        ELSE tipo_iden
        END,
        proceso_auditoria = CONCAT(proceso_auditoria, '| PER_006')
    WHERE tipo_iden IS NULL 
    AND num_iden IS NOT NULL 
    AND LENGTH(num_iden) > 0;
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query_update
    )

def _asignar_tipo_cedula_a_17_digitos():
    logging.info("|-- Asignando tipo de identificación 'CÉDULA DE IDENTIDAD' a cédulas válidas")
    query_update= """
    UPDATE db_vacunacion_covid
    SET tipo_iden = 
        CASE 
        WHEN tipo_iden IS NULL AND es_cedula_valida(num_iden) = TRUE 
        THEN 'CÉDULA DE IDENTIDAD'
        ELSE tipo_iden
        END,
        proceso_auditoria = CONCAT(proceso_auditoria, '| PER_007')
    WHERE tipo_iden IS NULL 
    AND num_iden IS NOT NULL 
    AND LENGTH(num_iden) > 0
    AND es_cedula_valida(num_iden) = TRUE;
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query_update
    )

def _calcular_edad():
    logging.info("|-- Calculando EDAD_ANIOS")
    crear_columna_en_tabla_si_no_existe(
        db_name='resources/data_lake/vacunacion.duckdb',
        tabla='db_vacunacion_covid',
        columna='edad_anios',
        tipo='INT'
    )
    query_update= """
    UPDATE db_vacunacion_covid
    SET edad_anios = EXTRACT(YEAR FROM AGE(fecha_aplicacion, fecha_nacimiento::DATE))::INT,
        proceso_auditoria = CONCAT(proceso_auditoria, '| PER_008')
    WHERE fecha_nacimiento IS NOT NULL AND fecha_aplicacion IS NOT NULL;
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query_update
    )

def _homologar_nacionalidad():
    logging.info("|-- Homologando nacionalidades")
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
    proceso_auditoria = CONCAT(proceso_auditoria, '| PER_009')
    WHERE nacionalidad IS NOT NULL;
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query
    )
    
def _calcular_grupo_etario():
    logging.info("|-- Calculando GRUPO_ETARIO")
    crear_columna_en_tabla_si_no_existe(
        db_name='resources/data_lake/vacunacion.duckdb',
        tabla='db_vacunacion_covid',
        columna='grupo_etario',
        tipo='VARCHAR'
    )
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
    proceso_auditoria = CONCAT(proceso_auditoria, '| PER_010')
    WHERE TRUE;
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query
    )

def _homologar_etnia():
    logging.info("|-- Homologando ETNIAS")
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
    proceso_auditoria = CONCAT(proceso_auditoria, '| PER_011')
    WHERE etnia IS NOT NULL;
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query
    )

def _homologar_tipo_identificacion():
    ## valor_origina, valor_homologado
    logging.info("|-- Homologando tipos de identificación")
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
    proceso_auditoria = CONCAT(proceso_auditoria, '| PER_012')
    WHERE tipo_iden IS NOT NULL;
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query
    )

def _eliminar_caracter_porcentaje():
    logging.info("|-- Eliminando caracteres de porcentaje en num_iden")
    query = """
    UPDATE db_vacunacion_covid
    SET num_iden = REPLACE(num_iden, '%', ''),
        proceso_auditoria = CONCAT(proceso_auditoria, '| PER_013')
    WHERE num_iden IS NOT NULL;
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query
    )

def _eliminar_multiples_ceros_al_inicio_num_iden():
    logging.info("|-- Eliminando múltiples ceros al inicio de num_iden")
    query = """
        UPDATE db_vacunacion_covid
        SET num_iden = TRIM(LEADING '0' FROM num_iden)
        WHERE num_iden LIKE '00%';
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query
    )

def _eliminar_comilla():
    logging.info("|-- Eliminando comillas simples y dobles de num_iden")
    query = """
    UPDATE db_vacunacion_covid
    SET num_iden = REPLACE(REPLACE(num_iden, '''', ''), '"', ''),
        proceso_auditoria = CONCAT(proceso_auditoria, '| PER_014')
    WHERE num_iden IS NOT NULL;
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query
    )

def persona_orchester():
   # _limpiar_columnas_texto()
    _limpiar_num_iden_espacios()
    _limpiar_num_iden()
    _reemplazar_caracter_en_num_iden()
    _eliminar_caracter_porcentaje()
    _eliminar_comilla()
    _eliminar_multiples_ceros_al_inicio_num_iden()
    _limpiar_identificacion()
    _homologar_nacionalidad()
    _homologar_tipo_identificacion()
    _asignar_tipo_cedula_a_cedulas_validas()
    _asignar_tipo_no_identificado_a_num_iden_no_nulos()
    _asignar_tipo_cedula_a_17_digitos()
    _recalcular_anio_mes_dia_nacimiento()
    _calcular_edad()
    _calcular_grupo_etario()
    _homologar_etnia()
