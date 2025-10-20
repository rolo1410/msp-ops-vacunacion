TOTAL_VACUNAS_POR_FECHA = F"""
SELECT
    date_trunc('day', fecha_aplicacion)::date AS fecha_aplicacion,
    date_part('year', fecha_aplicacion)::int AS anio_aplicacion,
    date_part('month', fecha_aplicacion)::int AS mes_aplicacion,
    date_part('day', fecha_aplicacion)::int AS dia_aplicacion,
    COUNT(*) AS total_vacunas
FROM vacunacion.main.db_vacunacion v
GROUP BY 1,2,3,4
ORDER BY fecha_aplicacion;
"""

TOTAL_VACUNAS_POR_FECHA_Y_SEXO = F"""
SELECT
    date_trunc('day', fecha_aplicacion)::date AS fecha_aplicacion,
    date_part('year', fecha_aplicacion)::int AS anio_aplicacion,
    date_part('month', fecha_aplicacion)::int AS mes_aplicacion,        
    date_part('day', fecha_aplicacion)::int AS dia_aplicacion,
    SEXO AS genero,
    COUNT(*) AS total_vacunas
FROM vacunacion.main.db_vacunacion v
GROUP BY 1,2,3,4,5
ORDER BY fecha_aplicacion;
"""

QUERY_VACUNAS_TEMPORAL_FULL = """
SELECT
	unicodigo,
	e.LATGPS uni_lat, 
	e.LONGPS uni_long,
	e.ZON_DESCRIPCION zona,
	e.CIR_CODIGO circuito,
	E.DIS_CODIGO distrito,
	E.PRV_DESCRIPCION provincia,
	E.CAN_DESCRIPCION canton,
	E.PAR_DESCRIPCION parroquia,
	fecha_aplicacion,
	date_part('year', fecha_aplicacion) as anio_aplicacion,
	date_part('month', fecha_aplicacion) as mes_aplicacion,
	date_part('day', fecha_aplicacion) as dia_aplicacion,
	num_iden,
	nombre_vacuna,
	sexo,
	dosis_aplicada
FROM
	vacunacion.main.db_vacunacion v
INNER JOIN vacunacion.main.lk_establecimiento e ON
	v.unicodigo = e.UNI_CODIGO 
    where fecha_aplicacion is not null
    and fecha_aplicacion >= '2021-01-01'
"""

QUERY_TOTAL_VACUNAS_POR_GRUPO_ETARIO_Y_GENERO = """
SELECT
    grupo_etario,
    sexo AS genero,
    COUNT(*) AS total_vacunas,
    COUNT(DISTINCT num_iden) AS total_personas
FROM vacunacion.main.db_vacunacion v
WHERE fecha_aplicacion IS NOT NULL
  AND fecha_aplicacion >= '2021-01-01'
GROUP BY 1,2
ORDER BY 1,2;
"""
QUERY_TOTAL_VACUNAS="""
SELECT
    COUNT(*) AS total_vacunas,
    COUNT(DISTINCT num_iden) AS total_personas
    COUNT(DISTINCT nombre_vacunas) AStotal_vacunas_nombres
FROM vacunacion.main.db_vacunacion v
WHERE fecha_aplicacion IS NOT NULL
  AND fecha_aplicacion >= '2021-01-01';
"""