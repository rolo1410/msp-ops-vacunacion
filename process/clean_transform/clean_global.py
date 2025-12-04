from extract.extraccion_oracle_simple import logger
from process.clean_transform.dim_vacunacion import vacunacion_orchester
from process.clean_transform.utils import crear_columna_en_tabla_si_no_existe, ejecutar_query


def add_0_when_cedula_9_chars():
    logger.info("|-- Adición de cero inicial en cédulas de 9 caracteres completada.")
    query = """
    UPDATE db_vacunacion_covid
    SET num_iden = '0' || num_iden,
        proceso_auditoria = concat(proceso_auditoria, '| C006, num_iden')
    WHERE tipo_iden LIKE 'CÉDULA DE IDENTIDAD%' AND LENGTH(num_iden) = 9;
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query
    )

def clean_00_in_iden():
    logger.info("|-- Limpieza de ceros iniciales en num_iden completada.")
    query = """
    UPDATE db_vacunacion_covid
    SET num_iden = REGEXP_REPLACE(num_iden, '^0+', ''),
        proceso_auditoria = concat(proceso_auditoria, '| C005')
    WHERE num_iden IS NOT NULL AND num_iden LIKE '00%';
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query
    )


def clean_especial_characters():
    caracteres_especiales = ['$', '%', '&', '/', '(', ')', '=', '?', '¡', '¿', '´', '`', '^', '~', '<', '>', ';', ':', '[', ']', '{', '}',  '|', '*', '+', '-', ',', '.', '!', '@', 'ï', '¿', '½', "'", '\n', '\r', '\t', 'Â', 'Á', 'Ó', 'É', 'Ç', '"', 'Í', '’', '_', 'ç', '¨', '°', 'é', 'í', 'â', '€', '™', 'À', '‘', 'ú']
    columns = ['num_iden', 'unicodigo', 'sistema']
    
    for col in columns:
        logger.info(f"|-- Limpieza de caracteres especiales completada para la columna {col}.")
        # Construct a REPLACE chain for all special characters for this column
        replace_expression = col  # Start with column name
        for char in caracteres_especiales:
            # Escape single quotes in SQL by doubling them
            escaped_char = char.replace("'", "''")
            replace_expression = f"REPLACE({replace_expression}, '{escaped_char}', '')"
        
        # Create WHERE condition to check if any special character exists
        like_conditions = []
        for char in caracteres_especiales:
            escaped_char = char.replace("'", "''")
            like_conditions.append(f"{col} LIKE '%{escaped_char}%'")
        where_condition = " OR ".join(like_conditions)
        
        query = f"""
        UPDATE db_vacunacion_covid
        SET {col} = {replace_expression},
            proceso_auditoria = concat(proceso_auditoria, '| C002, {col}')
        WHERE {col} IS NOT NULL AND ({where_condition});
        """
        ejecutar_query(
            db_name='resources/data_lake/vacunacion.duckdb',
            query=query
        )
    

def clean_espacios():
    columns =['num_iden', 'unicodigo', 'sistema']
    for col in columns:
        query = f"""
        UPDATE db_vacunacion_covid
        SET {col} = TRIM({col}),
            proceso_auditoria= concat(proceso_auditoria, '| C001, {col}')
        WHERE {col} IS NOT NULL;
        """
        ejecutar_query(
            db_name='resources/data_lake/vacunacion.duckdb',
            query=query
        )
    logger.info("Limpieza de espacios completada.")



def remove_duplicates_query():
    logger.info("Iniciando eliminación de duplicados.")
    ## COMO asignar la accion realizadas
    columnas =[
                "anio_aplicacion",
                "mes_aplicacion",
                "dia_aplicacion",
                "fecha_aplicacion",
                "punto_vacunacion",
                "unicodigo",
                "uni_nombre",
                "zona",
                "distrito",
                "provincia",
                "canton",
                "apellidos",
                "nombres",
                "nombres_completos",
                "tipo_iden",
                "num_iden",
                "sexo",
                "anio_nacimiento",
                "mes_nacimiento",
                "dia_nacimiento",
                "fecha_nacimiento",
                "nacionalidad",
                "etnia",
                "pobla_vacuna",
                "grupo_riesgo",
                "nombre_vacuna",
                "lote_vacuna",
                "dosis_aplicada",
                "profesional_aplica",
                "iden_profesional_aplica",
                "fase_vacuna",
                "fase_vacuna_depurada",
                "grupo_riesgo_depurada",
                "edad_anios",
                "sistema",
                "registro_civil"]
    query="""
    DELETE FROM db_vacunacion_covid a
    WHERE rowid NOT IN (
        SELECT MIN(rowid)
        FROM db_vacunacion_covid
        GROUP BY """ + ", ".join(columnas) + """
    );
    """
    result =ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query
    )
    logger.info(f"Eliminación de duplicados completada. {result.rowcount} filas eliminadas.")


def update_fases():
    ##
#"*FASE0 18-enero al 23 mayo del 2021 -> 2021-01-18 a 2021-05-23
#*FASE1 24 de mayo al 14 junio de 2021 -> 2021-05-24 a 2021-06-14
#*FASE2 15 de junio al 11 de julio de 2021 -> 2021-06-15 a 2021-07-11
#*FASE3 11 de julio al 05 de septiembre de 2021 -> 2021-07-11 a 2021-09-05
#*FASE4 06-12 de septiembre 2021 -> 2021-09-06 a 2021-09-12
#Primer refuerzo 15 de octubre del 2021 -> 2021-10-15 a 2021-03-30
#Segundo refuerzo 30 de marzo del 2022 -> 2022-03-30 a 2023-12-31
#Vacunación bivalente 2023 -> 2023-01-01 a 2023-12-31
#Vacunación estacionaria contra COVID-19, 2024 -> 2024-01-01 a 2024-12-31
#Vacunación estacionaria contra COVID 19, 2025 -> 2025-01-01 a 2025-12-31

    query="""
    UPDATE db_vacunacion_covid
    SET fase_vacuna_depurada = 
    CASE
        WHEN fecha_aplicacion BETWEEN '2021-01-18' AND '2021-05-23' THEN 'FASE 0'
        WHEN fecha_aplicacion BETWEEN '2021-05-24' AND '2021-06-14' THEN 'FASE 1'
        WHEN fecha_aplicacion BETWEEN '2021-06-15' AND '2021-07-11' THEN 'FASE 2'
        WHEN fecha_aplicacion BETWEEN '2021-07-11' AND '2021-09-05' THEN 'FASE 3'
        WHEN fecha_aplicacion BETWEEN '2021-09-06' AND '2021-09-12' THEN 'FASE 4'
        WHEN fecha_aplicacion BETWEEN '2021-10-15' AND '2021-03-30' THEN 'Primer refuerzo'
        WHEN fecha_aplicacion BETWEEN '2022-03-30' AND '2023-12-31' THEN 'Segundo refuerzo'
        WHEN fecha_aplicacion BETWEEN '2023-01-01' AND '2023-12-31' THEN 'Vacunación bivalente 2023'
        WHEN fecha_aplicacion BETWEEN '2024-01-01' AND '2024-12-31' THEN 'Vacunación estacionaria contra COVID-19, 2024'
        WHEN fecha_aplicacion BETWEEN '2025-01-01' AND '2025-12-31' THEN 'Vacunación estacionaria contra COVID 19, 2025'
        ELSE fase_vacuna_depurada
    END,
    proceso_auditoria = concat(proceso_auditoria, '| C003')
    WHERE fecha_aplicacion IS NOT NULL;
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query
    )
    
def create_la_diferencia_en_dias_entre_vacunas():
    crear_columna_en_tabla_si_no_existe(
        db_name='resources/data_lake/vacunacion.duckdb',
        tabla='db_vacunacion_covid',
        columna='diferencia_dias_entre_dosis',
        tipo='INTEGER'
    )
    query = """
    update db_vacunacion_covid a
    set diferencia_dias_entre_dosis =
    (
        select 
            CASE 
                WHEN MIN(b.fecha_aplicacion) IS NOT NULL THEN 
                    CAST((date_diff('day', a.fecha_aplicacion, MIN(b.fecha_aplicacion))) AS INTEGER)
                ELSE NULL
            END
        from db_vacunacion_covid b
        where a.num_iden = b.num_iden
          and b.fecha_aplicacion > a.fecha_aplicacion
    )
    where a.fecha_aplicacion IS NOT NULL;
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query
    )
    logger.info("Cálculo de diferencia de días entre dosis completado.")


def unir_un_grupo_riesgo_depurada():
    ## los registros que son distintos en funcion del grupo de riesgo grupo_riesgo_depurada, se vuelven un solo registro con la concatencacion de todos los grupos de riesgo de los otros registros 
    query="""
    UPDATE db_vacunacion_covid a
    SET grupo_riesgo_depurada = (
        SELECT STRING_AGG(DISTINCT b.grupo_riesgo_depurada, '| ')
        FROM db_vacunacion_covid b
        WHERE a.num_iden = b.num_iden
    ),
    proceso_auditoria = concat(proceso_auditoria, '| C004, grupo_riesgo_depurada')
    WHERE EXISTS (
        SELECT 1
        FROM db_vacunacion_covid b
        WHERE a.num_iden = b.num_iden
          AND a.grupo_riesgo_depurada <> b.grupo_riesgo_depurada
    );
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query
    )
    ## eliminar los registros duplicados que quedaron despues de la union
    query="""
    DELETE FROM db_vacunacion_covid a
    WHERE rowid NOT IN (
        SELECT MIN(rowid)
        FROM db_vacunacion_covid
        GROUP BY 
         "anio_aplicacion",
 "mes_aplicacion",
 "dia_aplicacion",
 "fecha_aplicacion",
 "punto_vacunacion",
 "unicodigo",
 "uni_nombre",
 "zona",
 "distrito",
    "provincia",    
    "canton",
    "apellidos",
    
    "nombres",
    "nombres_completos",
    
    "tipo_iden",
    "num_iden",
    "sexo",
    "anio_nacimiento",
    "mes_nacimiento",
    "dia_nacimiento",
    "fecha_nacimiento",
    "nacionalidad",
    "etnia",
    "pobla_vacuna",
    "grupo_riesgo",
    "nombre_vacuna",
    "lote_vacuna",
    "dosis_aplicada",
    "profesional_aplica",
    "iden_profesional_aplica",
    "fase_vacuna",
    "fase_vacuna_depurada",
    "grupo_riesgo_depurada",
    "edad_anios",
    "sistema",
    "registro_civil"
    );
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query
    )
    logger.info("Unión de registros en grupo de prioridad completada.")



def clean_orchester(since: str, until: str):
    clean_especial_characters()
    #clean_espacios()
    #create_la_diferencia_en_dias_entre_vacunas()
    ##remove_duplicates_query()
    #unir_un_grupo_riesgo_depurada()
    clean_00_in_iden()
    add_0_when_cedula_9_chars()
    vacunacion_orchester(since, until)
    #update_fases()
    