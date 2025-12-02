
from venv import logger

import pandas as pd
import sqlalchemy

from extract.config.sources import DB_VACUNACION_SAID
from lake.init_lake import add_new_elements_to_lake


def load_lake_db_vacunacion_covid_from_sais(since: str, until: str, chunk_size: int = 1000000):
    """
    Carga datos de vacunación COVID en paralelo con persistencia directa en DuckDB
    """
    connection_string = f"""postgresql+psycopg2://{DB_VACUNACION_SAID['user']}:{DB_VACUNACION_SAID['password']}@{DB_VACUNACION_SAID['host']}:{DB_VACUNACION_SAID['port']}/{DB_VACUNACION_SAID['database']}"""
    engine = sqlalchemy.create_engine(connection_string)
    query = f"""
        SELECT
            a.id AS "ID_VAC_DEPU",
            a.fecha_atencion AS "FECHA_APLICACION",
            EXTRACT(YEAR FROM a.fecha_atencion)::INT AS "ANIO_APLICACION",
            EXTRACT(MONTH FROM a.fecha_atencion)::INT AS "MES_APLICACION",
            EXTRACT(DAY FROM a.fecha_atencion)::INT AS "DIA_APLICACION",
            NULL AS "FASE_VACUNA",
            NULL AS "FASE_VACUNA_DEPURADA",
            NULL AS "ID_VAC_CONS",
            gr.grupo_riesg::VARCHAR AS "GRUPO_RIESGO",	
            gr.grupo_riesg::VARCHAR AS "GRUPO_RIESGO_DEPURADA",	
            UPPER(d.dosis) AS "DOSIS_APLICADA",
            e.unicodigo::VARCHAR AS "UNICODIGO",
            e.establecimiento AS "PUNTO_VACUNACION",
            NULL AS "UNI_NOMBRE",
            'ZONA 5' AS "ZONA",
            NULL AS "DISTRITO",
            NULL AS "PROVINCIA",
            NULL AS "CANTON",
            UPPER(LTRIM(CONCAT(p.primer_nombre, ' ', p.segundo_nombre))) AS "NOMBRES",
            UPPER(LTRIM(CONCAT(p.primer_apellido, ' ', p.segundo_apellido))) AS "APELLIDOS",
            UPPER(LTRIM(CONCAT(p.primer_nombre, ' ', p.segundo_nombre, ' ', p.primer_apellido, ' ', p.segundo_apellido))) AS "NOMBRES_COMPLETOS",
            p.fecha_nacimiento::DATE AS "FECHA_NACIMIENTO",
            EXTRACT(YEAR FROM p.fecha_nacimiento::DATE)::INT AS "ANIO_NACIMIENTO",
            EXTRACT(MONTH FROM p.fecha_nacimiento::DATE)::INT AS "MES_NACIMIENTO",
            EXTRACT(DAY FROM p.fecha_nacimiento::DATE)::INT AS "DIA_NACIMIENTO",
            EXTRACT(YEAR FROM AGE(a.fecha_atencion, p.fecha_nacimiento::DATE))::INT AS "EDAD_ANIOS",
            LTRIM(UPPER(s.sexo)) AS "SEXO",
            LTRIM(UPPER(td.documento)) AS "TIPO_IDEN",
            p.numero_identificacion::VARCHAR AS "NUM_IDEN",
            n.nacionalidad::VARCHAR AS "NACIONALIDAD",
            ne.nacetnica::VARCHAR AS "ETNIA",
            NULL AS "POBLA_VACUNA",
            NULL AS "REGISTRO_CIVIL",
            b.biologico::VARCHAR AS "NOMBRE_VACUNA",
            v.lote::VARCHAR AS "LOTE_VACUNA",
            LTRIM(UPPER(CONCAT(u.first_name, ' ', u.last_name))) AS "PROFESIONAL_APLICA",
            UPPER(u.cedula) AS "IDEN_PROFESIONAL_APLICA",
            'SAID' AS "SISTEMA"
        FROM vacunacion.atencion a
        INNER JOIN vacunacion.vacunas v ON v.id = a.id_vacuna 
        INNER JOIN vacunacion.paciente p ON a.id_paciente = p.id
        INNER JOIN vacunacion.brigadas br ON br.id = a.id_brigada
        INNER JOIN public.users u ON u.id = br.id_vacunador
        INNER JOIN vacunacion.biologico b ON b.id = v.id_vacuna
        INNER JOIN vacunacion.dosis d ON a.id_dosis = d.id
        LEFT JOIN public.sexo s ON s.id = p.sexo
        LEFT JOIN public.tipo_documento td ON td.id = p.tipo_identificacion
        INNER JOIN vacunacion.grupo_riesgo gr ON gr.id = p.grupo_riesgo
        INNER JOIN vacunacion.establecimientos e ON e.id = v.id_establecimiento
        LEFT JOIN public.nacionalidad n ON n.id = p.nacionalidad
        LEFT JOIN public.nacionalidad_etnica ne ON ne.id = p.nac_etnica
        WHERE a.fecha_atencion BETWEEN '{since}' AND '{until}'
    """
        
    chunk_size = 10000000 # Procesar en chunks de 50k registros
    count=0
    logger.info(f"Iniciando carga de datos de vacunación COVID desde SAID entre {since} y {until}")
    for chunk_df in pd.read_sql(query, engine, chunksize=chunk_size):
        chunk_df.columns = [col.lower() for col in chunk_df.columns]
        add_new_elements_to_lake('vacunacion', 'lk_vacunacion_covid', ['id_vac_depu','num_iden','fecha_aplicacion', 'tipo_iden','id_vac_cons', 'nombre_vacuna', 'lote_vacuna', 'profesional_aplica','fase_vacuna_depurada'], chunk_df)
        logger.info(f"Procesado chunk con {len(chunk_df)} registros")
        count += 1
    logger.info(f"Archivos data lake creados exitosamente")