from lake.init_lake import add_new_elements_to_lake
from lake.load_lake import load_data, load_data_paginated
from process.clean_transform.add_data import add_data_orchester
from process.clean_transform.clean_global import clean_orchester
from process.clean_transform.dim_persona import persona_orchester
from process.clean_transform.dim_vacuna import vacuna_orchester
from process.clean_transform.dim_vacunacion import vacunacion_orchester
from process.clean_transform.imp_fase import imputar_fases_orchester
from process.clean_transform.utils import ejecutar_query
from process.marquer.no_tranform_persona import clean_cedulas_orchester, marcar_duplicados


def ubicar_registros_1900():
    query = """
    DELETE FROM db_vacunacion_covid
    WHERE fecha_aplicacion = '1900-01-01';
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query
    )

def eliminar_fecha_aplicacion_none():
    query = """
    DELETE FROM db_vacunacion_covid
    WHERE fecha_aplicacion IS NULL;
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query
    )


def prepare_clean_process():
    ## elminiar tabla si existe 
    query = """
    DROP TABLE IF EXISTS db_vacunacion_covid;
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query
    )
    create_table_from = """
    CREATE TABLE db_vacunacion_covid AS
    SELECT * FROM lk_vacunacion_covid;
   
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=create_table_from
    )
    create_column_audit = """
    ALTER TABLE db_vacunacion_covid
    ADD COLUMN proceso_auditoria VARCHAR;
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=create_column_audit
    )


def delete_none_identifications():
    query = """
    DELETE FROM db_vacunacion_covid
    WHERE num_iden IS NULL OR TRIM(num_iden) = '';
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query
    )
    
    """_summary_
    id_vac_depu	0
anio_aplicacion	1,9996
mes_aplicacion	0,9998
dia_aplicacion	0,9998
fecha_aplicacion	3
punto_vacunacion	0
unicodigo	3
uni_nombre	1
zona	1
distrito	1
provincia	1
canton	1
apellidos	0
nombres	0
nombres_completos	0
tipo_iden	2
num_iden	2,9997
sexo	2
anio_nacimiento	0,9999
mes_nacimiento	1
dia_nacimiento	1
fecha_nacimiento	3
nacionalidad	0
etnia	1,9264
pobla_vacuna	1,1958
grupo_riesgo	0,0856
nombre_vacuna	2,9994
lote_vacuna	1,9902
dosis_aplicada	2,9718
profesional_aplica	0,9967
iden_profesional_aplica	0,9839
fase_vacuna	0
fase_vacuna_depurada	1,9236
grupo_riesgo_depurada	1,2026
edad_anios	0
sistema	2
registro_civil	0
id_vac_cons	0
es_vacuna_moda_dia_establecimiento	3
    """
tabla_pesos = {
    "id_vac_depu": 0,
    "anio_aplicacion": 1.9996,
    "mes_aplicacion": 0.9998,
    "dia_aplicacion": 0.9998,
    "fecha_aplicacion": 3,
    "punto_vacunacion": 0,
    "unicodigo": 3,
    "uni_nombre": 1,
    "zona": 1,
    "distrito": 1,
    "provincia": 1,
    "canton": 1,
    "apellidos": 0,
    "nombres": 0,
    "nombres_completos": 0,
    "tipo_iden": 2,
    "num_iden": 2.9997,
    "sexo": 2,
    "anio_nacimiento": 0.9999,
    "mes_nacimiento": 1,
    "dia_nacimiento": 1,
    "fecha_nacimiento": 3,
    "nacionalidad": 0,
    "etnia": 1.9264,
    "pobla_vacuna": 1.1958,
    "grupo_riesgo": 0.0856,
    "nombre_vacuna": 2.9994,
    "lote_vacuna": 1.9902,
    "dosis_aplicada": 2.9718,
    "profesional_aplica": 0.9967,
    "iden_profesional_aplica": 0.9839,
    "fase_vacuna": 0,
    "fase_vacuna_depurada": 1.9236,
    "grupo_riesgo_depurada": 1.2026,
    "edad_anios": 0,
    "sistema": 2,
    "registro_civil": 0,   
    "id_vac_cons": 0,
}


def asignar_pesos_completitud():
    """Crear una columna con el peso de completitud de los datos"""
    query = """
    ALTER TABLE db_vacunacion_covid
    ADD COLUMN peso_completitud FLOAT;
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query
    )
    
    # Construir la query usando los valores del diccionario
    case_statements = []
    for column, weight in tabla_pesos.items():
        case_statements.append(f"CASE WHEN {column} IS NOT NULL THEN {weight} ELSE 0 END")
    
    query_update = f"""
    UPDATE db_vacunacion_covid
    SET peso_completitud = COALESCE({' + '.join(case_statements)}, 0);
    """
    
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query_update
    )    
    
def process_all_data_paginated():
    ubicar_registros_1900()
    eliminar_fecha_aplicacion_none()
    asignar_pesos_completitud()
    prepare_clean_process()
    imputar_fases_orchester()
    delete_none_identifications()
    clean_orchester()
    add_data_orchester()
    