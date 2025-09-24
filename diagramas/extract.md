# Diagrama de Flujo - Proceso de Extracción (`extract/`)

## Flujo Principal del Orquestador

```mermaid
graph TD
    A[🚀 INICIO - ingest_orchester] --> B{use_cache?}
    
    %% Rama con cache
    B -->|Sí| C[📁 get_db_vacunaciones_cached]
    C --> D{¿Existe cache?}
    D -->|Sí| E[📖 Cargar desde Parquet]
    D -->|No| F[🔄 get_db_vacunaciones_parallel]
    
    %% Rama sin cache
    B -->|No| F[🔄 get_db_vacunaciones_parallel]
    F --> G[💾 Persistir directamente en DuckDB]
    G --> H[📊 load_data desde lago]
    
    %% Convergencia
    E --> I[🧹 Limpiar NUM_IDEN]
    H --> I
    I --> J[💾 add_new_elements_to_lake - vacunacion]
    
    %% Procesamiento MPI
    J --> K[🔍 Extraer identificaciones únicas]
    K --> L[📊 get_mpi_data - Registro Civil]
    L --> M[💾 add_new_elements_to_lake - persona]
    
    %% Procesamiento GeoSalud
    M --> N[🗺️ get_geo_salud_data]
    N --> O[💾 add_new_elements_to_lake - establecimiento]
    
    O --> P[✅ FIN - Retornar DataFrame]

    %% Estilos
    classDef startEnd fill:#e1f5fe
    classDef process fill:#f3e5f5
    classDef decision fill:#fff3e0
    classDef database fill:#e8f5e8
    classDef cache fill:#fff8e1
    
    class A,P startEnd
    class C,F,L,N process
    class B,D decision
    class G,J,M,O database
    class E cache
```

## Detalle del Procesamiento Paralelo de Vacunación

```mermaid
graph TD
    A[🔄 get_db_vacunaciones_parallel] --> B[📊 get_count_db_vacunacion]
    B --> C[📝 Calcular chunks necesarios]
    C --> D[🚀 Crear ThreadPoolExecutor]
    
    D --> E[📋 Cola de persistencia]
    E --> F[🧵 Thread de persistencia]
    
    D --> G[👷 Workers de descarga]
    G --> H[📥 _fetch_chunk_worker]
    H --> I[🔍 get_db_vacunacion_optimized]
    I --> J[📊 Aplicar VACUNACION_SCHEMA]
    
    J --> K[📤 Enviar a cola]
    K --> L[💾 _persistence_worker]
    L --> M[🧹 Limpiar NUM_IDEN]
    M --> N[💾 add_new_elements_to_lake]
    
    F --> O[⏳ Esperar finalización]
    N --> O
    O --> P[✅ Procesamiento completado]

    %% Estilos
    classDef worker fill:#e3f2fd
    classDef queue fill:#f1f8e9
    classDef database fill:#fce4ec
    
    class G,H,L worker
    class E,K queue
    class B,I,N database
```

## Detalle de Extracción MPI (Registro Civil)

```mermaid
graph TD
    A[📋 Lista de identificaciones] --> B[📊 get_mpi_data]
    B --> C[📏 Dividir en chunks de 999]
    C --> D[🔄 Loop por cada chunk]
    
    D --> E[🔍 get_mpi_data_chunk]
    E --> F[🏛️ Conectar a Oracle MPI]
    F --> G[📊 SELECT datos demográficos]
    G --> H[📋 Procesar chunk]
    
    H --> I{¿Más chunks?}
    I -->|Sí| D
    I -->|No| J[🔗 pl.concat todos los DataFrames]
    
    J --> K[📊 DataFrame consolidado MPI]

    %% Campos extraídos
    G --> L[👤 IDENTIFIER_VALUE<br/>GENDER<br/>BIRTHDATE<br/>MARITAL_STATUS<br/>EC_ETHNICITY<br/>NAME_FAMILY<br/>NAME_GIVEN]
    
    %% Estilos
    classDef process fill:#e8f5e8
    classDef database fill:#e3f2fd
    classDef data fill:#fff3e0
    
    class B,E,J process
    class F,G database
    class A,K,L data
```

## Detalle de Extracción GeoSalud

```mermaid
graph TD
    A[🗺️ get_geo_salud_data] --> B[🏛️ Conectar a Oracle GeoSalud]
    B --> C[📊 SELECT establecimientos]
    C --> D[📋 Datos geográficos]
    
    %% Campos extraídos
    C --> E[🏥 UNI_CODIGO<br/>UNI_NOMBRE<br/>PRV_CODIGO<br/>PRV_DESCRIPCION<br/>CAN_CODIGO<br/>CAN_DESCRIPCION<br/>PAR_CODIGO<br/>PAR_DESCRIPCION<br/>TIPO_ESTABLECEMIENTO<br/>LONGPS, LATGPS<br/>ZONADEFRONTERA]
    
    D --> F[📊 DataFrame GeoSalud]

    %% Estilos
    classDef process fill:#e8f5e8
    classDef database fill:#e3f2fd
    classDef data fill:#fff3e0
    
    class A process
    class B,C database
    class D,E,F data
```

## Configuración de Conexiones

```mermaid
graph LR
    A[🔧 sources.py] --> B[🏛️ DB_VACUNACION]
    A --> C[👥 DB_MIP]
    A --> D[🗺️ DB_GEOSALUD]
    A --> E[📋 DB_REPLICA]
    
    B --> F[Oracle: HCUE_VACUNACION_DEPURADA]
    C --> G[Oracle: MPI.PERSON]
    D --> H[Oracle: SYAPP.VM_ESTABLECIMIENTOS]
    E --> I[Oracle: Replicación]
    
    %% Variables de entorno
    J[🌍 Variables .env] --> A
    J --> K[CNN_ORACLE_DB_*_HOST<br/>CNN_ORACLE_DB_*_PORT<br/>CNN_ORACLE_DB_*_USER<br/>CNN_ORACLE_DB_*_PASSWORD<br/>CNN_ORACLE_DB_*_SID]

    %% Estilos
    classDef config fill:#fff3e0
    classDef database fill:#e3f2fd
    classDef env fill:#f1f8e9
    
    class A,K config
    class B,C,D,E,F,G,H,I database
    class J env
```

## Flujo de Optimizaciones

```mermaid
graph TD
    A[⚡ Optimizaciones Implementadas] --> B[🔄 Paralelización]
    A --> C[💾 Cache Inteligente]
    A --> D[📊 Streaming por Chunks]
    A --> E[🧵 Persistencia Asíncrona]
    
    B --> F[ThreadPoolExecutor<br/>max_workers configurable]
    C --> G[Archivos Parquet<br/>Validación temporal]
    D --> H[Chunks de 500K registros<br/>Control de memoria]
    E --> I[Cola con límite<br/>Worker dedicado]
    
    %% Beneficios
    F --> J[⚡ Velocidad 3-5x]
    G --> K[💾 Evita re-consultas]
    H --> L[🧠 Uso eficiente RAM]
    I --> M[🔄 Procesamiento continuo]

    %% Estilos
    classDef optimization fill:#e8f5e8
    classDef technique fill:#e3f2fd
    classDef benefit fill:#fff3e0
    
    class A optimization
    class B,C,D,E,F,G,H,I technique
    class J,K,L,M benefit
```

---

## 📊 Métricas del Proceso

- **Volumen típico**: Millones de registros de vacunación
- **Chunk size**: 500,000 registros por defecto
- **Workers paralelos**: 4 por defecto (configurable)
- **Límite MPI**: 999 identificaciones por consulta
- **Cache**: Archivos Parquet para evitar re-consultas
- **Bases de datos**: 3 fuentes Oracle independientes

## 🎯 Resultado Final

El proceso genera tres tablas en el data lake (DuckDB):
- `lk_vacunacion` - Eventos de vacunación
- `lk_persona` - Datos demográficos del registro civil  
- `lk_establecimiento` - Información geográfica de centros de salud

## 📁 Archivos del Directorio Extract

### `ingest_orchester.py`
- **Función principal**: Coordina todo el proceso de extracción
- **Parámetros**: `since`, `until`, `chunk_size`, `max_workers`, `use_cache`
- **Flujo**: Vacunación → MPI → GeoSalud → Persistencia

### `db_vacunacion.py` 
- **Función principal**: Extracción masiva de datos de vacunación
- **Optimizaciones**: Paralelo, cache, streaming, persistencia asíncrona
- **Schema**: 41 campos definidos con tipos Polars
- **Conexión**: Oracle HCUE_VACUNACION_DEPURADA

### `mpi.py`
- **Función principal**: Extracción de datos demográficos
- **Limitación**: Chunks de 999 identificaciones por consulta Oracle
- **Conexión**: Oracle MPI.PERSON
- **Campos**: Identificación, género, nacimiento, etnia, nombres

### `geo_salud.py`
- **Función principal**: Extracción de datos geográficos
- **Conexión**: Oracle SYAPP.VM_ESTABLECIMIENTOS_INGRESADOS  
- **Campos**: Códigos, nombres, coordenadas, tipos, zona frontera

### `config/sources.py`
- **Función principal**: Configuración de conexiones Oracle
- **Variables**: Cargadas desde archivo `.env`
- **Motores**: SQLAlchemy engines para cada base de datos