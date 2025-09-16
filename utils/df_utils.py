def convertir_columnas_a_minusculas(df):
    """
    Convierte los nombres de las columnas de un DataFrame a minúsculas.

    Parámetros:
    df (pandas.DataFrame): El DataFrame cuyas columnas se desean convertir.

    Retorna:
    pandas.DataFrame: El DataFrame con los nombres de las columnas en minúsculas.
    """
    df.columns = [col.lower() for col in df.columns]
    return df