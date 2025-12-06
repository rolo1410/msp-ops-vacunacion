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

def _agrupar_grupo_riesgo():
    logging.info("|-- Agrupando grupo_riesgo y lugar_aplica para registros con mismo num_iden, fecha_aplicacion, unicodigo.")
    query = """
        UPDATE db_vacunacion_covid
        SET 
            grupo_riesgo = subq.grupo_riesgo_concat,
            punto_vacunacion = subq.lugar_aplica_concat
        FROM (
            SELECT 
                num_iden,
                fecha_aplicacion,
                unicodigo,
                STRING_AGG(DISTINCT grupo_riesgo, ', ' ORDER BY grupo_riesgo) as grupo_riesgo_concat,
                STRING_AGG(DISTINCT punto_vacunacion, ', ' ORDER BY punto_vacunacion) as lugar_aplica_concat
            FROM db_vacunacion_covid
            GROUP BY num_iden, fecha_aplicacion, unicodigo
            HAVING COUNT(DISTINCT grupo_riesgo) > 1 OR COUNT(DISTINCT punto_vacunacion) > 1
        ) subq
        WHERE 
            db_vacunacion_covid.num_iden = subq.num_iden
            AND db_vacunacion_covid.fecha_aplicacion = subq.fecha_aplicacion
            AND db_vacunacion_covid.unicodigo = subq.unicodigo;
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query
    )

def eliminar_duplicados_orchester():
    logging.info("|- TRATAMIENTO DE DUPLICADOS")
    _eliminar_duplicados_completos()
    _eliminar_duplicados_fecha_establecimiento_vacuna_persona()