import duckdb


def tratamiento_registros_1900_rows():    
    ## TRA_1900: Tratamiento de registros con fecha_aplicacion en 1900
    conn= duckdb.connect(database='resources/data_lake/vacunacion.duckdb')
    duckdb = conn.cursor()
    query = f"""
    with fecha_establecimiento_moda as (
       SELECT unicodigo,
               mode(fecha_aplicacion) as fecha_aplicacion
        FROM db_vacunacion_covid
        WHERE fecha_aplicacion >= '2021-01-01' AND fecha_aplicacion < '2022-12-31'
        GROUP BY unicodigo
    ),
    update db_vacunacion_covid set fecha_aplicacion = f.fecha_aplicacion
    proceso_auditoria = concat(proceso_auditoria, '| TRA_1900')
    from fecha_establecimiento_moda f
    where db_vacunacion_covid.unicodigo = f.unicodigo
    AND db_vacunacion_covid.fecha_aplicacion <= '2021-01-01' AND db_vacunacion_covid.fecha_aplicacion >= '2025-01-01'
    """
    duckdb.query(query)
    duckdb.close()


def fechas_tratamiento_orchester(since: str, until: str):
    tratamiento_registros_1900_rows()   