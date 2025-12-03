import logging

from process.clean_transform.utils import ejecutar_query


def _eliminar_duplicados_completos():
    query = """
    DELETE FROM db_vacunacion_covid a
    WHERE EXISTS (
        SELECT 1
        FROM db_vacunacion_covid b
        WHERE a.num_iden = b.num_iden
          AND a.fecha_aplicacion = b.fecha_aplicacion
          AND a.vacuna = b.vacuna
          AND a.dosis = b.dosis
          AND a.unicodigo = b.unicodigo
          AND a.rowid > b.rowid
    );
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query
    )
    
def _eliminar_duplicados_persona_fecha():
    query = """
    DELETE FROM db_vacunacion_covid a
    WHERE EXISTS (
        SELECT 1
        FROM db_vacunacion_covid b
        WHERE a.num_iden = b.num_iden
          AND a.fecha_aplicacion = b.fecha_aplicacion
          AND a.rowid > b.rowid
    );
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query
    )
    logging.info("Duplicados por persona y fecha eliminados.")

def _eliminar_duplicados_casos_profesionales():
    query = """
    DELETE FROM db_vacunacion_covid a
    WHERE EXISTS (
        SELECT 1
        FROM db_vacunacion_covid b
        WHERE a.num_iden = b.num_iden
          AND a.fecha_aplicacion = b.fecha_aplicacion
          AND a.ocupacion = 'PROFESIONAL DE LA SALUD'
          AND b.ocupacion = 'PROFESIONAL DE LA SALUD'
          AND a.rowid > b.rowid
    );
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query
    )
    logging.info("Duplicados casos por profesionales duplicados")

def _eliminar_duplicados_casos_establecimientos():
    query = """
    DELETE FROM db_vacunacion_covid a
    WHERE EXISTS (
        SELECT 1
        FROM db_vacunacion_covid b
        WHERE a.num_iden = b.num_iden
          AND a.fecha_aplicacion = b.fecha_aplicacion
          AND a.establecimiento = b.establecimiento
          AND a.rowid > b.rowid
    );
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query
    )
    logging.info("Duplicados casos por establecimientos puntos de vacunacion.")


def eliminar_duplicados_orchester():
    _eliminar_duplicados_completos()
    _eliminar_duplicados_persona_fecha()
    _eliminar_duplicados_casos_profesionales()
    _eliminar_duplicados_casos_establecimientos()