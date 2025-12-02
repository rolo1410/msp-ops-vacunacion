# MSP-OPS-Vacunación

Sistema de procesamiento y análisis de datos de vacunación COVID-19 para el Ministerio de Salud Pública.

## 📋 Descripción

Este sistema automatiza la extracción, transformación y carga (ETL) de datos de vacunación desde bases de datos Oracle hacia un data lake basado en DuckDB, implementando procesos de limpieza, validación y análisis de calidad de datos.

## 🏗️ Arquitectura de la Solución

### Componentes Principales

```
├── extract/           # Extracción de datos desde Oracle
├── lake/             # Gestión del data lake (DuckDB)
├── process/          # Transformación y limpieza de datos
├── load/             # Carga y perfilado de datos
├── utils/            # Utilidades y funciones auxiliares
└── resources/        # Recursos y datos de salida
```

### Flujo de Datos

1. **Extracción** → Oracle DB → Parquet files
2. **Ingesta** → Data Lake (DuckDB)
3. **Transformación** → Limpieza y validación
4. **Carga** → Datos procesados y perfiles

## 🚀 Instalación y Configuración

### Prerrequisitos

- Python 3.12+
- Oracle Database Client
- Acceso a base de datos Oracle de vacunación

### Instalación

```bash
# Clonar el repositorio
git clone <repository-url>
cd msp-ops-vacunacion

# Crear entorno virtual
python3 -m venv .venv
source .venv/bin/activate

# Instalar dependencias
pip install -r requirements.txt
```

### Configuración

1. **Driver Oracle JDBC**: El sistema descarga automáticamente `ojdbc8-23.3.0.23.09.jar`
2. **Credenciales**: Configurar en los archivos de extracción
3. **Directorios**: Se crean automáticamente en `resources/`

## 🔧 Uso del Sistema

### Comando Principal

```bash
python3 main_full.py --since 1800-01-01 --until 2050-01-31 --chunk-size 1000
```

### Parámetros

- `--since`: Fecha de inicio (formato YYYY-MM-DD)
- `--until`: Fecha de fin (formato YYYY-MM-DD)
- `--chunk-size`: Tamaño de chunks para procesamiento (default: 1000)

### Scripts Disponibles

#### Extracción de Datos
```bash
# Extracción con Spark (datasets grandes)
python3 extract/conectarse_BDD_Oracle_generar_parquet_mejorado.py

# Extracción simplificada con pandas
python3 extract/extraccion_oracle_simple.py
```

#### Procesamiento Individual
```bash
# Solo ingesta
python3 main_load.py

# Solo transformación
python3 process/clean_transform_orchester.py
```

## 📊 Funcionalidades Principales

### 1. Extracción de Datos
- **Múltiples métodos**: Spark, pandas + oracledb
- **Optimización de memoria**: Procesamiento en chunks
- **Formatos de salida**: Parquet con compresión Snappy

### 2. Limpieza y Transformación
- **Validación de cédulas**: Algoritmo de verificación
- **Limpieza de caracteres especiales**: Normalización de texto
- **Detección de duplicados**: Basado en múltiples campos
- **Cálculo de métricas**: Diferencias entre dosis
- **Gestión de fechas**: Corrección de fechas inválidas

### 3. Gestión del Data Lake
- **Base de datos**: DuckDB para almacenamiento analítico
- **Esquemas optimizados**: Tablas dimensionales y de hechos
- **Inserción incremental**: Evita duplicados automáticamente
- **Particionamiento**: Por fechas y regiones

### 4. Análisis y Perfilado
- **Perfiles de calidad**: ydata-profiling / ydata-sdk
- **Métricas estadísticas**: Distribuciones y tendencias
- **Reportes HTML**: Visualizaciones interactivas
- **Detección de anomalías**: Valores atípicos y patrones

## 🛠️ Componentes Técnicos

### Stack Tecnológico
- **Python 3.13**: Lenguaje principal
- **Polars**: Procesamiento de datos de alto rendimiento
- **DuckDB**: Base de datos analítica en proceso
- **Apache Spark**: Procesamiento de big data
- **Oracle DB**: Fuente de datos
- **Pandas**: Análisis de datos complementario

### Librerías Principales
```python
polars>=1.33.1
duckdb>=1.3.2
pyspark>=4.0.1
oracledb>=3.3.0
pandas>=2.3.2
ydata-profiling>=4.17.0
```

## 📁 Estructura de Directorios

```
msp-ops-vacunacion/
├── extract/
│   ├── db_vacunacion_covid.py      # Extracción COVID
│   ├── db_vacunacion_rutinario.py  # Extracción rutinaria
│   ├── geo_salud.py                # Datos geográficos
│   └── ingest_orchester.py         # Orquestador de ingesta
├── lake/
│   ├── init_lake.py                # Inicialización del data lake
│   ├── load_lake.py                # Carga de datos
│   └── sources.py                  # Definición de fuentes
├── process/
│   ├── clean_transform_orchester.py # Orquestador de limpieza
│   ├── clean_transform/            # Módulos de transformación
│   └── marquer/                    # Marcadores y validadores
├── load/
│   ├── generate_profile.py         # Generación de perfiles
│   └── profilers/                  # Perfiladores específicos
└── resources/
    ├── data_lake/                  # Archivos DuckDB
    ├── data_out/                   # Archivos de salida
    └── homologations/              # Tablas de homologación
```

## 🔍 Procesos de Calidad de Datos

### Validaciones Implementadas
- ✅ **Cédulas de identidad**: Algoritmo de dígito verificador
- ✅ **Fechas de aplicación**: Rangos válidos y coherencia
- ✅ **Códigos geográficos**: Validación contra catálogos
- ✅ **Duplicados**: Detección por múltiples campos clave
- ✅ **Caracteres especiales**: Limpieza y normalización
- ✅ **Valores nulos**: Identificación y tratamiento

### Métricas de Calidad
- **Completitud**: % de campos completos
- **Validez**: % de valores válidos según reglas
- **Consistencia**: Coherencia entre campos relacionados
- **Unicidad**: Detección de duplicados
- **Precisión**: Exactitud de los datos

## 📈 Monitoreo y Logs

### Sistema de Logging
```python
# Configuración de logs
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
```

### Métricas de Rendimiento
- Tiempo de procesamiento por chunk
- Memoria utilizada
- Registros procesados por minuto
- Tasa de errores y reintentos

## 🚨 Solución de Problemas Comunes

### Error: "pkg_resources not found"
```bash
# Actualizar setuptools
pip install --upgrade setuptools pip
```

### Error: "julianday does not exist"
- ✅ **Solucionado**: Migrado a `date_diff()` de DuckDB

### Error: "DataFrame object has no attribute 'to_pandas'"
- ✅ **Solucionado**: Detección automática de tipo de DataFrame

### Error de sintaxis SQL con comillas
- ✅ **Solucionado**: Escape correcto de caracteres especiales

## 🔧 Problemas Resueltos Durante el Desarrollo

### 1. Configuración de Spark
**Problema**: Rutas de Windows en sistema Linux, configuración incorrecta de JARs
**Solución**: 
- Migración de rutas `C:/` a rutas Linux apropiadas
- Descarga automática de `ojdbc8-23.3.0.23.09.jar`
- Optimización de configuración de memoria de Spark

### 2. Compatibilidad de DataFrames
**Problema**: Confusión entre DataFrames de pandas y Polars
**Solución**: 
- Detección automática del tipo de DataFrame
- Conversión automática según el contexto

### 3. Funciones SQL Incompatibles
**Problema**: `julianday()` no existe en DuckDB
**Solución**: 
- Migración a `date_diff()` función nativa de DuckDB
- Validación de sintaxis SQL específica para DuckDB

### 4. Manejo de Caracteres Especiales
**Problema**: Escape incorrecto de comillas en SQL
**Solución**: 
- Implementación de escape automático de caracteres especiales
- Validación de sintaxis SQL generada dinámicamente

## 📊 Métricas del Proyecto

### Rendimiento
- **Procesamiento**: 50,000+ registros por chunk
- **Memoria optimizada**: Uso eficiente con chunks
- **Formatos eficientes**: Parquet con compresión Snappy

### Calidad de Código
- **Linting**: flake8, black, isort configurados
- **Tipo de hints**: Tipado gradual implementado
- **Logging**: Sistema completo de trazabilidad
- **Manejo de errores**: Try/catch comprehensivo

## 📄 Licencia

Este proyecto está desarrollado para el Ministerio de Salud Pública del Ecuador.

## 👥 Contribuidores

- **rolo4336@gmail.com**: Desarrollador principal
- **Equipo MSP**: Análisis de requerimientos y validación

## 📞 Soporte

Para soporte técnico o preguntas sobre la implementación, contactar al equipo de desarrollo del MSP.

---

**Última actualización**: Noviembre 2025  
**Versión**: 1.0.0