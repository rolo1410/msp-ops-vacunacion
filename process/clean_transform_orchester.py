from lake.init_lake import add_new_elements_to_lake
from lake.load_lake import load_data, load_data_paginated
from process.clean_transform.dim_persona import persona_orchester
from process.clean_transform.dim_vacuna import vacuna_orchester
from process.clean_transform.dim_vacunacion import vacunacion_orchester
from process.marquer.no_tranform_persona import clean_cedulas_orchester, marcar_duplicados


def process_orchester(page, page_size):
    '''
    Orquesta el procesamiento de datos de vacunación
    '''
    df = load_data_paginated('vacunacion', 'lk_vacunacion_covid', page, page_size)
    df = persona_orchester(df)
    print(f"Dim persona - columnas: {len(df)}")
    df = vacuna_orchester(df)
    print(f"Dim vacuna - columnas: {len(df)}")
    df = vacunacion_orchester(df)
    print(f"Dim vacunacion - columnas: {len(df)}")
    df = clean_cedulas_orchester(df)
    print(f"Dim cedulas - columnas: {len(df)}")
    df = marcar_duplicados(df)
    print(f"Dim duplicados - columnas: {len(df)}")
    return df

def process_all_data_paginated():
    '''
    Orquesta el procesamiento de todos los datos de vacunación en páginas
    '''
    page = 0
    page_size = 10000000 # Tamaño de página ajustable
    while True:
        print(f"Procesando página {page}")
        df_page = process_orchester(page, page_size)
        add_new_elements_to_lake('vacunacion', 'db_vacunacion', ['num_iden', 'fecha_aplicacion', 'unicodigo'], df_page)
        if df_page.is_empty():
            break
        # Aquí puedes guardar o procesar df_page según sea necesario
        page += 1