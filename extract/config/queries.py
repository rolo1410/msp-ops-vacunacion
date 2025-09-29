COUNT_VACUNACION_COVID_19=f"""
SELECT 
    COUNT(*) AS total_count
FROM HCUE_VACUNACION_DEPURADA.DB_VACUNACION_CONSOLIDADA_DEPURADA_COVID
WHERE 
    FECHA_APLICACION BETWEEN TO_DATE(':since', 'YYYY-MM-DD') 
    AND TO_DATE(':until', 'YYYY-MM-DD')"""
    
SELECT_VACUNACION_COVID_19=f"""
SELECT 
    COUNT(*) AS total_count
FROM HCUE_VACUNACION_DEPURADA.DB_VACUNACION_CONSOLIDADA_DEPURADA_COVID
WHERE 
    FECHA_APLICACION BETWEEN TO_DATE(':since', 'YYYY-MM-DD') 
    AND TO_DATE(':until', 'YYYY-MM-DD')"""
    
def get_source_query(source, tipe):
    if source == 'vacunacion':
        if tipe == 'data':
            return SELECT_VACUNACION_COVID_19
        elif tipe == 'count':
            return COUNT_VACUNACION_COVID_19
    else:
        raise ValueError(f"Fuente desconocida: {source}")