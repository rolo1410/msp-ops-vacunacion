from extract.extraccion_oracle_simple import logger
from process.clean_transform.utils import crear_columna_en_tabla_si_no_existe, ejecutar_query


def add_diff_dias_fecha_aplicacion():
    logger.info("Agregando columna diff_dias_fecha_aplicacion")
    '''
    Agrega la columna de diferencia de días desde la fecha de aplicación a la fecha actual
    '''
    crear_columna_en_tabla_si_no_existe(
        db_name='resources/data_lake/vacunacion.duckdb',
        tabla='db_vacunacion_covid',
        columna='diff_dias_fecha_aplicacion',
        tipo='INTEGER'
    )
    
    query = f"""
                UPDATE db_vacunacion_covid
                SET diff_dias_fecha_aplicacion = T2.diferencia_dias
                FROM (
                WITH FechasUnicasCalculadas AS (
                SELECT DISTINCT
                num_iden,
                fecha_aplicacion,
                DATE_DIFF('day', 
                    LAG(fecha_aplicacion, 1) OVER (
                        PARTITION BY num_iden
                        ORDER BY fecha_aplicacion
                    ), 
                    fecha_aplicacion
                ) AS diferencia_dias
                FROM
                db_vacunacion_covid
                ORDER BY num_iden, fecha_aplicacion
                )
                SELECT * FROM FechasUnicasCalculadas
                ) AS T2
                WHERE db_vacunacion_covid.num_iden = T2.num_iden
                AND db_vacunacion_covid.fecha_aplicacion = T2.fecha_aplicacion;
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query
    )   
    logger.info("Columna diff_dias_fecha_aplicacion agregada y calculada.")

## TODO: CALCULAR BIEN L SEMANA EPIDEMIOLOGICA SEGUN AÑO Y FECHA
def add_semana_epidemiologica():
    """ en funcion de la fecha_aplicacion, calcular la semana apidemiologica y agregarla a la tabla
    a semana epidemiológica se calcula dividiendo el año en ciclos de 7 días que comienzan un domingo y terminan un sábado, con la primera semana del año definida por la primera semana de enero que contenga al menos 4 días de ese mes. La primera semana del año debe contener al menos cuatro días del mes de enero; de lo contrario, se considera parte del año anterior. El resto del año se divide en 52 o 53 semanas de esta manera. 
    """
    create_colum_query = """ alter table db_vacunacion_covid
    add column if not exists semana_epidemiologica INTEGER;
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=create_colum_query
    )       
    query = """
    UPDATE db_vacunacion_covid
    SET semana_epidemiologica = CAST( (CAST(strftime(fecha_aplicacion, '%j') AS INTEGER) + 6 - CAST(strftime(date_trunc('year', fecha_aplicacion), '%w') AS INTEGER)) / 7 AS INTEGER),
    proceso_auditoria = concat(proceso_auditoria, '| C001, semana_epidemiologica')
    WHERE fecha_aplicacion IS NOT NULL;
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query
    )
    pass


def add_grupo_etario_depurada():
    create_colum_query = """
    ALTER TABLE db_vacunacion_covid
    ADD COLUMN IF NOT EXISTS grupo_etario_depurada VARCHAR;
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=create_colum_query
    )
    query = """
    UPDATE db_vacunacion_covid
    SET grupo_etario_depurada =
    CASE
        WHEN edad_anios IS NULL THEN 'NO DEFINIDO'
        WHEN edad_anios < 1 THEN 'MENOR DE 1 AÑO'
        WHEN edad_anios BETWEEN 1 AND 4 THEN 'DE 1 A 4 AÑOS'
        WHEN edad_anios BETWEEN 5 AND 9 THEN 'DE 5 A 9 AÑOS'
        WHEN edad_anios BETWEEN 10 AND 14 THEN 'DE 10 A 14 AÑOS'
        WHEN edad_anios BETWEEN 15 AND 19 THEN 'DE 15 A 19 AÑOS'
        WHEN edad_anios BETWEEN 20 AND 64 THEN 'DE 20 A 64 AÑOS'
        WHEN edad_anios >= 65 THEN 'DE 65 AÑOS Y MÁS'
        ELSE 'NO DEFINIDO'
    END,
    proceso_auditoria = concat(proceso_auditoria, '| C002, grupo_etario_depurada')
    WHERE edad_anios IS NOT NULL;
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query
    )   

def add_data_orchester():
    '''
    Agrega datos adicionales a la tabla de vacunación
    '''
    add_semana_epidemiologica()
    add_grupo_etario_depurada()
    add_diff_dias_fecha_aplicacion()