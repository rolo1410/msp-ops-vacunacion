# 💉 Sistema de Vacunación MSP

Este proyecto es una aplicación web construida con Streamlit para el análisis y control del sistema de vacunación del Ministerio de Salud Pública.

## 🚀 Características

- **📊 Vista General**: Dashboard principal con métricas clave del sistema
- **✅ Control de Calidad**: Validación de datos y alertas de calidad
- **📈 Análisis Temporal**: Tendencias, patrones y pronósticos de vacunación

## 📁 Estructura del Proyecto

```
streamlit-app/
├── src/
│   ├── app.py                          # Punto de entrada principal
│   ├── components/
│   │   ├── __init__.py
│   │   ├── general.py                  # Componente de vista general
│   │   ├── calidad.py                  # Componente de control de calidad
│   │   └── analisis_temporal.py        # Componente de análisis temporal
│   ├── data/
│   │   └── __init__.py                 # Manejo de datos
│   └── utils/
│       ├── __init__.py
│       └── helpers.py                  # Funciones utilitarias
├── requirements.txt                     # Dependencias del proyecto
├── config.toml                         # Configuración de Streamlit
├── run_app.sh                          # Script de ejecución
└── README.md                           # Documentación
```

## 🛠️ Instalación y Configuración

### Prerrequisitos

- Python 3.8 o superior
- pip (gestor de paquetes de Python)

### Instalación

1. **Clonar o navegar al directorio del proyecto:**
   ```bash
   cd streamlit-app
   ```

2. **Instalar dependencias:**
   ```bash
   pip install -r requirements.txt
   ```

## 🎮 Uso

### Opción 1: Usando el script de ejecución (Recomendado)
```bash
./run_app.sh
```

### Opción 2: Ejecución manual
```bash
cd src
streamlit run app.py
```

### Opción 3: Con configuración personalizada
```bash
cd src
streamlit run app.py --server.port=8501 --server.address=localhost
```

Una vez ejecutado, la aplicación estará disponible en: `http://localhost:8501`

## 📊 Funcionalidades

### 🏠 Vista General
- Métricas principales del sistema de vacunación
- Gráficos de progreso y tendencias
- Indicadores clave de rendimiento
- Alertas y notificaciones del sistema

### ✅ Control de Calidad
- **Métricas de Calidad**: Completitud, precisión y duplicados
- **Validación de Datos**: Reglas de validación y errores detectados
- **Sistema de Alertas**: Alertas categorizadas por severidad
- **Generador de Reportes**: Reportes personalizables en PDF, Excel y CSV

### 📈 Análisis Temporal
- **Tendencias Generales**: Evolución temporal de vacunaciones
- **Patrones Temporales**: Análisis por día, semana, mes y hora
- **Pronósticos**: Proyecciones basadas en datos históricos
- **Análisis Detallado**: Segmentación por región, tipo de vacuna y grupo de edad

## ⚙️ Configuración

El archivo `config.toml` contiene la configuración de la aplicación:

- **Tema**: Colores y tipografía personalizados
- **Servidor**: Puerto y configuraciones de red
- **Cliente**: Configuraciones de interfaz de usuario

## 📦 Dependencias

- `streamlit`: Framework de aplicaciones web
- `pandas`: Manipulación y análisis de datos
- `numpy`: Computación numérica
- `plotly`: Visualizaciones interactivas
- `matplotlib`: Gráficos estáticos
- `scikit-learn`: Herramientas de machine learning
- `seaborn`: Visualizaciones estadísticas

## 🤝 Contribución

1. Fork el proyecto
2. Crear una rama para tu feature (`git checkout -b feature/AmazingFeature`)
3. Commit tus cambios (`git commit -m 'Add some AmazingFeature'`)
4. Push a la rama (`git push origin feature/AmazingFeature`)
5. Crear un Pull Request

## 📄 Licencia

Este proyecto está bajo la licencia MIT. Ver el archivo `LICENSE` para más detalles.

## 📞 Soporte

Para soporte técnico o preguntas, contactar al equipo de desarrollo del MSP.

---

**Desarrollado para el Ministerio de Salud Pública** 🏥

## Instalación

Para instalar las dependencias necesarias, ejecute el siguiente comando:

```
pip install -r requirements.txt
```

## Ejecución

Para ejecutar la aplicación, utilice el siguiente comando:

```
streamlit run src/app.py
```

## Contribuciones

Las contribuciones son bienvenidas. Si desea contribuir, por favor abra un issue o envíe un pull request.