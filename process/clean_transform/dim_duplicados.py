def remove_duplicates(df):
    '''
    Elimina duplicados del DataFrame basado en columnas clave
    '''
    key_columns = ['num_iden', 'fecha_aplicacion', 'unicodigo', 'id_vac_cons']
    df = df.unique(subset=key_columns)
    return df