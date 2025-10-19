import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from data.source import get_duck_db_data


def show_geografico():
    """
    Muestra el análisis geográfico de las vacunas aplicadas
    """
    st.header("🗺️ Análisis Geográfico")
    st.markdown("Distribución geográfica de las vacunas aplicadas por región, provincia y cantón.")
    
    # Cargar datos
    with st.spinner("Cargando datos geográficos..."):
        # Query básica para obtener datos geográficos
        query_geografico = """
       SELECT
	le.PRV_DESCRIPCION  provincia,
	le.CAN_DESCRIPCION  canton,
	le.DIS_CODIGO distrito,
	COUNT(*) as total_vacunas,
	COUNT(DISTINCT unicodigo) as total_establecimientos,
	COUNT(DISTINCT v.num_iden ) as personas_vacunadas
FROM
	vacunacion.main.db_vacunacion v
inner join vacunacion.main.lk_establecimiento le on
	le.UNI_CODIGO = v.unicodigo
WHERE
	provincia IS NOT NULL
	AND canton IS NOT NULL
GROUP BY
	provincia,
	canton,
	distrito
ORDER BY
	total_vacunas DESC
        """
        
        df_geo = get_duck_db_data(query_geografico)
    
    if df_geo.empty:
        st.error("No se pudieron cargar los datos geográficos.")
        return
    
    # Filtros en el sidebar
    st.sidebar.header("🔍 Filtros Geográficos")
    
    # Filtro por provincia
    provincias = sorted(df_geo['provincia'].unique())
    provincia_seleccionada = st.sidebar.selectbox(
        "Seleccionar Provincia:",
        options=["Todas"] + list(provincias),
        index=0
    )
    
    # Filtrar datos según provincia seleccionada
    if provincia_seleccionada != "Todas":
        df_filtered = df_geo[df_geo['provincia'] == provincia_seleccionada]
    else:
        df_filtered = df_geo.copy()
    
    # Métricas principales
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_vacunas = df_filtered['total_vacunas'].sum()
        st.metric("Total Vacunas", f"{total_vacunas:,}")
    
    with col2:
        total_establecimientos = df_filtered['total_establecimientos'].sum()
        st.metric("Establecimientos", f"{total_establecimientos:,}")
    
    with col3:
        total_personas = df_filtered['personas_vacunadas'].sum()
        st.metric("Personas Vacunadas", f"{total_personas:,}")
    
    with col4:
        promedio_por_establecimiento = total_vacunas / total_establecimientos if total_establecimientos > 0 else 0
        st.metric("Promedio por Establecimiento", f"{promedio_por_establecimiento:.1f}")
    
    # Crear dos columnas para los gráficos
    col1, col2 = st.columns(2)
    
    with col1:
        # Gráfico de barras por provincia (si se muestran todas las provincias)
        if provincia_seleccionada == "Todas":
            st.subheader("📊 Distribución por Provincia")
            df_provincia = df_geo.groupby('provincia').agg({
                'total_vacunas': 'sum',
                'total_establecimientos': 'sum',
                'personas_vacunadas': 'sum'
            }).reset_index()
            
            fig_provincia = px.bar(
                df_provincia.head(15),
                x='total_vacunas',
                y='provincia',
                orientation='h',
                title="Top 15 Provincias por Vacunas Aplicadas",
                labels={'total_vacunas': 'Total de Vacunas', 'provincia': 'Provincia'},
                color='total_vacunas',
                color_continuous_scale='viridis'
            )
            fig_provincia.update_layout(height=500)
            st.plotly_chart(fig_provincia, use_container_width=True)
        else:
            # Gráfico por cantones de la provincia seleccionada
            st.subheader(f"📊 Distribución por Cantón - {provincia_seleccionada}")
            df_canton = df_filtered.groupby('canton').agg({
                'total_vacunas': 'sum',
                'total_establecimientos': 'sum',
                'personas_vacunadas': 'sum'
            }).reset_index()
            
            fig_canton = px.bar(
                df_canton.head(15),
                x='total_vacunas',
                y='canton',
                orientation='h',
                title=f"Top 15 Cantones - {provincia_seleccionada}",
                labels={'total_vacunas': 'Total de Vacunas', 'canton': 'Cantón'},
                color='total_vacunas',
                color_continuous_scale='plasma'
            )
            fig_canton.update_layout(height=500)
            st.plotly_chart(fig_canton, use_container_width=True)
    
    with col2:
        # Gráfico de dispersión: Establecimientos vs Vacunas
        st.subheader("📈 Relación Establecimientos vs Vacunas")
        
        if provincia_seleccionada == "Todas":
            df_scatter = df_geo.groupby('provincia').agg({
                'total_vacunas': 'sum',
                'total_establecimientos': 'sum'
            }).reset_index()
            hover_data = ['provincia']
        else:
            df_scatter = df_filtered.groupby('canton').agg({
                'total_vacunas': 'sum',
                'total_establecimientos': 'sum'
            }).reset_index()
            hover_data = ['canton']
        
        fig_scatter = px.scatter(
            df_scatter,
            x='total_establecimientos',
            y='total_vacunas',
            size='total_vacunas',
            hover_data=hover_data,
            title="Relación entre Establecimientos y Vacunas",
            labels={
                'total_establecimientos': 'Total de Establecimientos',
                'total_vacunas': 'Total de Vacunas'
            }
        )
        fig_scatter.update_layout(height=500)
        st.plotly_chart(fig_scatter, use_container_width=True)
    
    # Tabla detallada
    st.subheader("📋 Tabla Detallada")
    
    # Preparar datos para la tabla
    if provincia_seleccionada == "Todas":
        df_tabla = df_geo.groupby(['provincia', 'canton']).agg({
            'total_vacunas': 'sum',
            'total_establecimientos': 'sum',
            'personas_vacunadas': 'sum'
        }).reset_index()
        df_tabla = df_tabla.sort_values('total_vacunas', ascending=False)
    else:
        df_tabla = df_filtered.copy()
        df_tabla = df_tabla.sort_values('total_vacunas', ascending=False)
    
    # Agregar columna de eficiencia (vacunas por establecimiento)
    df_tabla['vacunas_por_establecimiento'] = (
        df_tabla['total_vacunas'] / df_tabla['total_establecimientos']
    ).round(1)
    
    # Configurar columnas para mostrar
    if provincia_seleccionada == "Todas":
        columnas_mostrar = ['provincia', 'canton', 'total_vacunas', 'total_establecimientos', 
                           'personas_vacunadas', 'vacunas_por_establecimiento']
    else:
        columnas_mostrar = ['canton', 'distrito', 'total_vacunas', 'total_establecimientos', 
                           'personas_vacunadas', 'vacunas_por_establecimiento']
    
    # Mostrar tabla con paginación
    st.dataframe(
        df_tabla[columnas_mostrar],
        use_container_width=True,
        hide_index=True,
        column_config={
            "total_vacunas": st.column_config.NumberColumn(
                "Total Vacunas",
                format="%d"
            ),
            "total_establecimientos": st.column_config.NumberColumn(
                "Establecimientos",
                format="%d"
            ),
            "personas_vacunadas": st.column_config.NumberColumn(
                "Personas Vacunadas",
                format="%d"
            ),
            "vacunas_por_establecimiento": st.column_config.NumberColumn(
                "Vacunas/Establecimiento",
                format="%.1f"
            )
        }
    )
    
    # Información adicional
    with st.expander("ℹ️ Información sobre el Análisis Geográfico"):
        st.markdown("""
        **Descripción del Análisis:**
        
        - **Total Vacunas**: Número total de vacunas aplicadas en cada ubicación geográfica
        - **Establecimientos**: Cantidad de establecimientos de salud únicos por área
        - **Personas Vacunadas**: Número de personas únicas que han recibido al menos una vacuna
        - **Vacunas por Establecimiento**: Indicador de eficiencia que muestra el promedio de vacunas aplicadas por establecimiento
        
        **Filtros Disponibles:**
        - Puede filtrar por provincia específica para ver el detalle por cantones y distritos
        - La vista "Todas" muestra el resumen por provincias
        
        **Interpretación:**
        - Areas con mayor densidad de establecimientos tienden a tener más vacunas aplicadas
        - La relación vacunas/establecimiento indica la eficiencia de cada área geográfica
        """)