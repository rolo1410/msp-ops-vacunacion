import logging

from process.clean_transform.utils import ejecutar_query


def _eliminar_duplicados_completos():
    logging.info("|-- Eliminando duplicados por fecha, num_iden, nombre_vacuna, unicodigo.")
    query = """
        DELETE FROM db_vacunacion_covid
        WHERE rowid NOT IN (
            SELECT MIN(rowid)
            FROM db_vacunacion_covid
            GROUP BY 
                fecha_aplicacion, 
                num_iden, 
                nombre_vacuna, 
                unicodigo
        );
    """
    ejecutar_query(
    db_name='resources/data_lake/vacunacion.duckdb',
    query=query
    )


def _eliminar_duplicados_fecha_establecimiento_vacuna_persona():
    logging.info("|-- Eliminando duplicados completos, cuando todos los campos son iguales.")
    query = """
        DELETE FROM db_vacunacion_covid
        WHERE rowid NOT IN (
            SELECT MIN(rowid)
            FROM db_vacunacion_covid
            GROUP BY
                anio_aplicacion, 
                mes_aplicacion, 
                dia_aplicacion,
                fecha_aplicacion, 
                punto_vacunacion, 
                unicodigo, 
                uni_nombre,
                zona, 
                distrito, 
                provincia, 
                canton, 
                apellidos, 
                nombres,
                nombres_completos, 
                tipo_iden, 
                num_iden, 
                sexo, 
                anio_nacimiento,
                mes_nacimiento, 
                dia_nacimiento, 
                fecha_nacimiento, 
                nacionalidad,
                etnia, 
                pobla_vacuna, 
                grupo_riesgo, 
                nombre_vacuna, 
                lote_vacuna,
                dosis_aplicada, 
                profesional_aplica, 
                iden_profesional_aplica,
                grupo_riesgo_depurada,
                edad_anios, 
                sistema, 
                registro_civil
        );
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query
    )

def eliminar_duplicados_orchester():
    logging.info("|- TRATAMIENTO DE DUPLICADOS")
    _eliminar_duplicados_completos()
    _eliminar_duplicados_fecha_establecimiento_vacuna_persona()