import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import folium
from streamlit_folium import st_folium
import numpy as np
from data.source import get_duck_db_data


def create_vaccination_map(df_geo, max_establishments=500):
    """
    Crea un mapa interactivo con burbujas que muestran la cantidad de vacunas por establecimiento
    """
    # Filtrar datos válidos
    df_map = df_geo.copy()
    df_map = df_map.dropna(subset=['latitud', 'longitud'])
    
    # Convertir coordenadas a float
    df_map['latitud'] = pd.to_numeric(df_map['latitud'], errors='coerce')
    df_map['longitud'] = pd.to_numeric(df_map['longitud'], errors='coerce')
    
    # Filtrar coordenadas válidas para Ecuador
    df_map = df_map[
        (df_map['latitud'].between(-5, 2)) & 
        (df_map['longitud'].between(-92, -75))
    ]
    
    if df_map.empty:
        return None
    
    # Limitar a los top establecimientos para mejorar rendimiento
    original_count = len(df_map)
    if len(df_map) > max_establishments:
        df_map = df_map.nlargest(max_establishments, 'total_vacunas')
    
    # Calcular centro del mapa dinámicamente
    center_lat = df_map['latitud'].mean() if not df_map.empty else -1.8312
    center_lon = df_map['longitud'].mean() if not df_map.empty else -78.1834
    
    # Ajustar zoom según la dispersión de los datos
    lat_range = df_map['latitud'].max() - df_map['latitud'].min()
    lon_range = df_map['longitud'].max() - df_map['longitud'].min()
    
    # Determinar zoom inicial basado en el rango de coordenadas
    if lat_range < 0.5 and lon_range < 0.5:
        zoom_start = 11  # Zoom alto para área pequeña
    elif lat_range < 1.5 and lon_range < 1.5:
        zoom_start = 9   # Zoom medio para área mediana
    else:
        zoom_start = 7   # Zoom bajo para área grande
    
    # Crear el mapa base
    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=zoom_start,
        tiles='OpenStreetMap'
    )
    
    # Calcular el tamaño de las burbujas basado en la cantidad de vacunas
    min_vacunas = df_map['total_vacunas'].min()
    max_vacunas = df_map['total_vacunas'].max()
    
    # Normalizar tamaños de burbujas (entre 8 y 40 píxeles)
    if max_vacunas > min_vacunas:
        df_map['bubble_size'] = 8 + 32 * (df_map['total_vacunas'] - min_vacunas) / (max_vacunas - min_vacunas)
    else:
        df_map['bubble_size'] = 15
    
    # Definir colores según la cantidad de vacunas
    def get_color(vacunas):
        if vacunas < 100:
            return '#2E8B57'  # Verde
        elif vacunas < 500:
            return '#FFD700'  # Amarillo
        elif vacunas < 1000:
            return '#FF8C00'  # Naranja
        else:
            return '#DC143C'  # Rojo
    
    # Agregar marcadores para cada establecimiento
    for idx, row in df_map.iterrows():
        # Crear popup con información detallada
        popup_html = f"""
        <div style="font-family: Arial; width: 280px; line-height: 1.4;">
            <h4 style="margin: 0 0 8px 0; color: #1f77b4; font-size: 14px;">{row['nombre_establecimiento']}</h4>
            <hr style="margin: 5px 0; border: 1px solid #ccc;">
            
            <div style="margin-bottom: 8px;">
                <b style="color: #333;">📍 Ubicación:</b><br>
                <span style="margin-left: 10px; font-size: 12px;">
                    Provincia: <b>{row['provincia']}</b><br>
                    Cantón: <b>{row['canton']}</b>
                </span>
            </div>
            
            <div style="margin-bottom: 8px;">
                <b style="color: #333;">💉 Vacunación:</b><br>
                <span style="margin-left: 10px; font-size: 12px;">
                    Total vacunas: <span style="color: #d62728; font-weight: bold; font-size: 14px;">{row['total_vacunas']:,}</span><br>
                    Personas vacunadas: <b>{row['personas_vacunadas']:,}</b>
                </span>
            </div>
            
            <div style="margin-bottom: 5px;">
                <b style="color: #333;">🏥 Establecimiento:</b><br>
                <span style="margin-left: 10px; font-size: 12px;">
                    Tipo: <b>{row['tipo_establecimiento']}</b><br>
                    Código: <b>{row['unicodigo']}</b>
                </span>
            </div>
        </div>
        """
        
        # Agregar marcador circular
        folium.CircleMarker(
            location=[row['latitud'], row['longitud']],
            radius=row['bubble_size'],
            popup=folium.Popup(popup_html, max_width=320),
            color='#333333',
            weight=1.5,
            fillColor=get_color(row['total_vacunas']),
            fillOpacity=0.8,
            tooltip=folium.Tooltip(
                f"<b>{row['nombre_establecimiento']}</b><br>{row['total_vacunas']:,} vacunas aplicadas",
                style="font-size: 12px; font-family: Arial;"
            )
        ).add_to(m)
    
    # Agregar leyenda mejorada
    legend_html = '''
    <div style="position: fixed; 
                bottom: 50px; left: 50px; width: 180px; height: 140px; 
                background-color: white; border:2px solid grey; z-index:9999; 
                font-size:12px; padding: 12px; border-radius: 5px; box-shadow: 0 0 15px rgba(0,0,0,0.2);">
    <h4 style="margin: 0 0 8px 0; color: #333; font-size: 14px;">📊 Cantidad de Vacunas</h4>
    <div style="margin-bottom: 4px;"><span style="color:#2E8B57; font-size: 16px;">●</span> &lt; 100 vacunas</div>
    <div style="margin-bottom: 4px;"><span style="color:#FFD700; font-size: 16px;">●</span> 100 - 500 vacunas</div>
    <div style="margin-bottom: 4px;"><span style="color:#FF8C00; font-size: 16px;">●</span> 500 - 1,000 vacunas</div>
    <div style="margin-bottom: 4px;"><span style="color:#DC143C; font-size: 16px;">●</span> &gt; 1,000 vacunas</div>
    <hr style="margin: 8px 0; border: 1px solid #ddd;">
    <div style="font-size: 10px; color: #666;">💡 Tamaño = cantidad de vacunas</div>
    </div>
    '''
    m.get_root().html.add_child(folium.Element(legend_html))
    
    return m, len(df_map), original_count


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
        le.UNI_CODIGO as unicodigo,
        le.UNI_NOMBRE as nombre_establecimiento,
        le.PRV_DESCRIPCION as provincia,
        le.CAN_DESCRIPCION as canton,
        le.DIS_CODIGO as distrito,
        le.TIPO_ESTABLECEMIENTO as tipo_establecimiento,
        le.LATGPS as latitud,
        le.LONGPS as longitud,
        COUNT(v.num_iden) as total_vacunas,
        COUNT(DISTINCT unicodigo) as total_establecimientos,
        COUNT(DISTINCT v.num_iden) as personas_vacunadas
    FROM 
        vacunacion.main.lk_establecimiento le
    LEFT JOIN 
        vacunacion.main.db_vacunacion v ON le.UNI_CODIGO = v.unicodigo
    WHERE 
        le.LATGPS IS NOT NULL 
        AND TRY_CAST(le.LONGPS as DOUBLE) IS NOT NULL
        AND TRY_CAST(le.LATGPS as DOUBLE) != 0 
        AND TRY_CAST(le.LONGPS as DOUBLE) != 0
        AND TRY_CAST(le.LATGPS as DOUBLE) BETWEEN -5 AND 2  
        AND TRY_CAST(le.LONGPS as DOUBLE) BETWEEN -92 AND -75
    GROUP BY 
        le.UNI_CODIGO, le.UNI_NOMBRE, le.PRV_DESCRIPCION, 
        le.CAN_DESCRIPCION, le.DIS_CODIGO, le.TIPO_ESTABLECEMIENTO,
        le.LATGPS, le.LONGPS
    ORDER BY 
        total_vacunas DESC limit 10000
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
    
    # Configuración del mapa
    st.sidebar.header("⚙️ Configuración del Mapa")
    
    # Filtro por cantidad mínima de vacunas
    min_vacunas_filter = st.sidebar.number_input(
        "Vacunas mínimas para mostrar:",
        min_value=0,
        max_value=int(df_geo['total_vacunas'].max()),
        value=0,
        step=50,
        help="Solo mostrar establecimientos con al menos esta cantidad de vacunas"
    )
    
    # Opción de mostrar solo top establecimientos
    show_top_only = st.sidebar.checkbox(
        "Mostrar solo top establecimientos",
        value=False,
        help="Mejora el rendimiento mostrando solo los establecimientos con más vacunas"
    )
    
    if show_top_only:
        top_count = st.sidebar.slider(
            "Cantidad de establecimientos:",
            min_value=50,
            max_value=min(500, len(df_geo)),
            value=min(200, len(df_geo)),
            step=25
        )
    
    # Filtrar datos según provincia seleccionada
    if provincia_seleccionada != "Todas":
        df_filtered = df_geo[df_geo['provincia'] == provincia_seleccionada]
    else:
        df_filtered = df_geo.copy()
    
    # Aplicar filtro de vacunas mínimas
    if min_vacunas_filter > 0:
        df_filtered = df_filtered[df_filtered['total_vacunas'] >= min_vacunas_filter]
    
    # Aplicar filtro de top establecimientos
    if show_top_only:
        df_filtered = df_filtered.nlargest(top_count, 'total_vacunas')
    
    if df_filtered.empty:
        st.warning("⚠️ No hay datos que cumplan con los filtros seleccionados.")
        return
    
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
    
    # Mapa interactivo con burbujas
    st.subheader("🗺️ Mapa de Vacunación por Establecimiento")
    st.markdown("**Mapa interactivo que muestra la distribución geográfica de vacunas aplicadas. El tamaño de las burbujas representa la cantidad de vacunas.**")
    
    # Crear el mapa
    map_result = create_vaccination_map(df_filtered, max_establishments=500)
    
    if map_result is not None:
        vaccination_map, shown_count, total_count = map_result
        
        # Mostrar información sobre el filtrado
        if shown_count < total_count:
            st.info(f"ℹ️ Mostrando {shown_count} de {total_count} establecimientos para optimizar rendimiento. Usa los filtros para refinar la vista.")
        
        # Mostrar el mapa
        st_folium(vaccination_map, width=1000, height=500)
        
        # Estadísticas del mapa
        col_map1, col_map2, col_map3 = st.columns(3)
        with col_map1:
            st.info(f"📍 **{shown_count}** establecimientos mostrados en el mapa")
        with col_map2:
            max_vacunas_establecimiento = df_filtered['total_vacunas'].max()
            st.info(f"💉 **{max_vacunas_establecimiento:,}** vacunas máximas por establecimiento")
        with col_map3:
            promedio_coords = df_filtered.dropna(subset=['latitud', 'longitud'])['total_vacunas'].mean()
            st.info(f"📊 **{promedio_coords:.1f}** promedio de vacunas por establecimiento")
    else:
        st.warning("No se pudieron cargar datos geográficos válidos para el mapa.")
    
    st.markdown("---")
    
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
        
        **Mapa Interactivo:**
        - **Burbujas**: El tamaño representa la cantidad de vacunas aplicadas en cada establecimiento
        - **Colores**: 
          - 🟢 Verde: < 100 vacunas
          - 🟡 Amarillo: 100 - 500 vacunas  
          - 🟠 Naranja: 500 - 1,000 vacunas
          - 🔴 Rojo: > 1,000 vacunas
        - **Interactividad**: Haz clic en las burbujas para ver información detallada del establecimiento
        
        **Filtros Disponibles:**
        - Puede filtrar por provincia específica para ver el detalle por cantones y distritos
        - La vista "Todas" muestra el resumen por provincias
        - Los filtros afectan tanto los gráficos como el mapa interactivo
        
        **Interpretación:**
        - Areas con mayor densidad de establecimientos tienden a tener más vacunas aplicadas
        - La relación vacunas/establecimiento indica la eficiencia de cada área geográfica
        - El mapa permite identificar visualmente patrones geográficos y concentraciones de vacunación
        """)
        
        st.markdown("**Nota Técnica:** Solo se muestran establecimientos con coordenadas válidas dentro del territorio ecuatoriano.")