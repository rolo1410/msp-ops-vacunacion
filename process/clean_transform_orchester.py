from lake.load_lake import load_data
from process.clean_transform.dim_persona import persona_orchester
from process.clean_transform.dim_vacuna import vacuna_orchester
from process.clean_transform.dim_vacunacion import vacunacion_orchester
from process.marquer.no_tranform_persona import clean_cedulas_orchester


def process_orchester():
    '''
    Orquesta el procesamiento de datos de vacunación
    '''
    df = load_data('vacunacion', 'lk_vacunacion_covid')
    df = persona_orchester(df)
    df = vacuna_orchester(df)
    df = vacunacion_orchester(df)
    df = clean_cedulas_orchester(df)
    return df