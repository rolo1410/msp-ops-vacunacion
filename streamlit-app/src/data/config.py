"""
Configuración de datos para la aplicación de vacunación MSP
"""

# Configuración de regiones
REGIONES = {
    "Norte": {
        "codigo": "REG01",
        "poblacion": 1250000,
        "centros_vacunacion": 45,
        "meta_mensual": 62500
    },
    "Sur": {
        "codigo": "REG02", 
        "poblacion": 1580000,
        "centros_vacunacion": 52,
        "meta_mensual": 79000
    },
    "Este": {
        "codigo": "REG03",
        "poblacion": 980000,
        "centros_vacunacion": 32,
        "meta_mensual": 49000
    },
    "Oeste": {
        "codigo": "REG04",
        "poblacion": 1320000,
        "centros_vacunacion": 41,
        "meta_mensual": 66000
    },
    "Centro": {
        "codigo": "REG05",
        "poblacion": 1680000,
        "centros_vacunacion": 58,
        "meta_mensual": 84000
    }
}

# Tipos de vacunas y códigos
TIPOS_VACUNAS = {
    "COVID-19": {
        "codigo": "COV19",
        "dosis_requeridas": 2,
        "intervalo_dias": 21,
        "vigencia_meses": 12
    },
    "Influenza": {
        "codigo": "FLU", 
        "dosis_requeridas": 1,
        "intervalo_dias": 0,
        "vigencia_meses": 12
    },
    "Hepatitis B": {
        "codigo": "HEPB",
        "dosis_requeridas": 3,
        "intervalo_dias": 30,
        "vigencia_meses": 120
    },
    "MMR": {
        "codigo": "MMR",
        "dosis_requeridas": 2,
        "intervalo_dias": 28,
        "vigencia_meses": 600
    }
}

# Grupos de edad
GRUPOS_EDAD = {
    "0-17": {"min_edad": 0, "max_edad": 17, "descripcion": "Niños y adolescentes"},
    "18-39": {"min_edad": 18, "max_edad": 39, "descripcion": "Adultos jóvenes"},
    "40-59": {"min_edad": 40, "max_edad": 59, "descripcion": "Adultos"},
    "60+": {"min_edad": 60, "max_edad": 120, "descripcion": "Adultos mayores"}
}

# Configuración de alertas
CONFIGURACION_ALERTAS = {
    "completitud_minima": 95.0,  # Porcentaje mínimo de completitud
    "precision_minima": 97.0,    # Porcentaje mínimo de precisión
    "duplicados_maximo": 0.5,    # Porcentaje máximo de duplicados
    "tiempo_respuesta_maximo": 5.0,  # Segundos máximos de respuesta
    "centros_inactivos_maximo": 3,   # Número máximo de centros inactivos
}

# Métricas objetivo
METRICAS_OBJETIVO = {
    "cobertura_nacional": 80.0,      # Porcentaje objetivo de cobertura
    "meta_mensual_nacional": 340500,  # Meta mensual nacional
    "centros_activos_minimo": 95,    # Porcentaje mínimo de centros activos
    "tiempo_procesamiento_maximo": 24,  # Horas máximas para procesar datos
}

# Configuración de reportes
CONFIGURACION_REPORTES = {
    "formatos_disponibles": ["PDF", "Excel", "CSV"],
    "frecuencias": ["Diario", "Semanal", "Mensual", "Trimestral"],
    "tipos": [
        "Reporte Completo",
        "Métricas de Calidad", 
        "Errores Detectados",
        "Tendencias",
        "Cobertura por Región"
    ]
}

# Colores para gráficos
COLORES_GRAFICOS = {
    "primario": "#1E88E5",
    "secundario": "#43A047", 
    "alerta": "#FFC107",
    "error": "#F44336",
    "exito": "#4CAF50",
    "info": "#2196F3",
    "escala_azules": ["#E3F2FD", "#BBDEFB", "#90CAF9", "#64B5F6", "#42A5F5", "#2196F3", "#1E88E5", "#1976D2", "#1565C0", "#0D47A1"],
    "escala_verdes": ["#E8F5E8", "#C8E6C9", "#A5D6A7", "#81C784", "#66BB6A", "#4CAF50", "#43A047", "#388E3C", "#2E7D32", "#1B5E20"]
}

# Configuración de base de datos (ejemplo)
DATABASE_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "vacunacion_msp",
    "schema": "public",
    "tables": {
        "vacunaciones": "tbl_vacunaciones",
        "centros": "tbl_centros_vacunacion", 
        "pacientes": "tbl_pacientes",
        "vacunas": "tbl_catalogo_vacunas",
        "personal": "tbl_personal_salud"
    }
}

# Textos de la aplicación
TEXTOS_APP = {
    "titulo_principal": "💉 Sistema de Vacunación MSP",
    "descripcion": "Sistema de análisis y control de vacunación del Ministerio de Salud Pública",
    "menu_items": {
        "general": "🏠 General",
        "calidad": "✅ Calidad", 
        "temporal": "📈 Análisis Temporal"
    },
    "mensajes": {
        "cargando": "Cargando datos...",
        "error_conexion": "Error de conexión con la base de datos",
        "datos_actualizados": "Datos actualizados exitosamente",
        "reporte_generado": "Reporte generado exitosamente",
        "sin_datos": "No hay datos disponibles para el período seleccionado"
    }
}