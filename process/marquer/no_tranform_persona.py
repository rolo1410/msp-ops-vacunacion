import logging

from utils.df_utils import validar_cedula_ecuatoriana


def add_0_prefix_cedula(cedula):
    # completar con un 0 a la izquierda si tiene 9 dígitos
    if len(cedula) == 9:
        cedula = '0' + cedula
    if not validar_cedula_ecuatoriana(cedula):
        return cedula
    return cedula

def process_incomplete_cedulas(df:pl.DataFrame) -> pl.DataFrame:
    # a todos los campos cuya identificacion tenga 9 digitos y que tipo identificacion se  cedula, agregarle un 0 a la izquierda
    df = df.with_columns(
        pl.when((pl.col('tipo_iden') == 'CÉDULA DE IDENTIDAD') & (pl.col('num_iden').str.lengths() == 9))
        .then(pl.col('num_iden').apply(add_0_prefix_cedula))
        .otherwise(pl.col('num_iden'))
        .alias('num_iden')
    )
    return df

def marcar_cedula_no_valida(df):
    # crear una columna con la cedulas que no cumplan son el digito verificador 
    df = df.with_columns(
        pl.when((pl.col('tipo_iden') == 'CÉDULA DE IDENTIDAD') & (~pl.col('num_iden').apply(validar_cedula_ecuatoriana)))
        .then(pl.lit(True))
        .otherwise(pl.lit(False))
        .alias('cedula_no_valida')
    )
    return df

def clean_cedulas_orchester(df):
    df = process_incomplete_cedulas(df)
    df = marcar_cedula_no_valida(df)
    return df