from datetime import datetime, timedelta
import os

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
try:
    import geopandas as gpd
    import folium
    from streamlit_folium import st_folium
    GEOSPATIAL_AVAILABLE = True
except ImportError:
    GEOSPATIAL_AVAILABLE = False
    st.warning("Módulos geoespaciales no disponibles. Instale geopandas, folium y streamlit-folium para ver mapas.")

from data.source import QUERY_VACUNAS_TEMPORAL_FULL, get_duck_db_data
from .general.seccion_genero import crear_grafico_mariposa_genero, crear_grafico_mariposa_avanzado


def safe_get_unique_values(df, column_name, default_values=None):
    """
    Obtiene valores únicos de una columna de manera segura.
    
    Args:
        df: DataFrame
        column_name: Nombre de la columna
        default_values: Valores por defecto si hay error
    
    Returns:
        list: Lista de valores únicos o valores por defecto
    """
    if default_values is None:
        default_values = []
    
    try:
        if df.empty or column_name not in df.columns:
            return default_values
        
        unique_values = [val for val in df[column_name].unique() if not pd.isna(val)]
        return sorted(unique_values) if unique_values else default_values
    
    except Exception:
        return default_values


def total_establecimiento(df: pd.DataFrame) -> int:
    """
    Calcula el total de establecimientos únicos en el DataFrame proporcionado.
    """
    return df['unicodigo'].nunique()


def delta_total_vacunas(df: pd.DataFrame) -> int:
    """
    Calcula la diferencia en el total de vacunas aplicadas entre el último día y el día anterior.
    """
    df_sorted = df.sort_values(by='fecha_aplicacion')
    if len(df_sorted) < 2:
        return 0
    # Agrupa por fecha y suma el total de vacunas por día
    vacunas_por_dia = df.groupby("fecha_aplicacion").size().sort_index()
    # Si hay menos de 2 días, retorna 0
    if len(vacunas_por_dia) < 2:
        return 0
    # Diferencia entre el último día y el anterior
    return int(vacunas_por_dia.iloc[-1] - vacunas_por_dia.iloc[-2])
def delta_vacunados(df: pd.DataFrame) -> int:
    """
    Calcula la diferencia en el total de vacunados entre el último día y el día anterior.
    """
    df_sorted = df.sort_values(by='fecha_aplicacion')
    if len(df_sorted) < 2:
        return 0
    # Agrupa por fecha y cuenta los vacunados únicos por día
    vacunados_por_dia = df.groupby("fecha_aplicacion")['num_iden'].nunique().sort_index()
    # Si hay menos de 2 días, retorna 0
    if len(vacunados_por_dia) < 2:
        return 0
    # Diferencia entre el último día y el anterior
    return int(vacunados_por_dia.iloc[-1] - vacunados_por_dia.iloc[-2])


@st.cache_data
def load_zonas_planificacion():
    """
    Carga los datos de zonas de planificación de SENPLADES desde el shapefile.
    """
    if not GEOSPATIAL_AVAILABLE:
        return None
    
    try:
        # Ruta al archivo shapefile
        shapefile_path = os.path.join(
            os.path.dirname(__file__), 
            '..', 
            'resources', 
            'dis_administrativa', 
            'ZONAS_PLANIFICACION_SENPLADES.shp'
        )
        
        if os.path.exists(shapefile_path):
            gdf = gpd.read_file(shapefile_path)
            
            # Manejar CRS si no está definido
            if gdf.crs is None:
                # Verificar los bounds para determinar el CRS más probable
                bounds = gdf.total_bounds
                if abs(bounds[0]) < 180 and abs(bounds[1]) < 90:
                    # Los valores están en grados, probablemente WGS84
                    gdf = gdf.set_crs('EPSG:4326')
                else:
                    # Los valores están en metros, probablemente UTM Zone 17S para Ecuador
                    gdf = gdf.set_crs('EPSG:32717')
                    
            # Asegurar que esté en un CRS apropiado para visualización web
            if gdf.crs != 'EPSG:4326':
                gdf = gdf.to_crs('EPSG:4326')
                
            return gdf
        else:
            st.warning(f"No se encontró el archivo shapefile en: {shapefile_path}")
            return None
    except Exception as e:
        st.error(f"Error al cargar el shapefile: {str(e)}")
        return None


def create_zona_distribution_map(df_filtrado: pd.DataFrame, gdf_zonas: gpd.GeoDataFrame):
    """
    Crea un mapa de distribución de vacunación por zonas usando folium.
    """
    if not GEOSPATIAL_AVAILABLE or gdf_zonas is None:
        return None
    
    try:
        # Calcular estadísticas de vacunación por zona
        if 'zona' not in df_filtrado.columns:
            st.warning("La columna 'zona' no está disponible en los datos")
            return None
        
        zona_stats = df_filtrado.groupby('zona').agg({
            'num_iden': 'nunique',  # Vacunados únicos
            'unicodigo': 'nunique'  # Establecimientos únicos
        }).reset_index()
        
        # Contar total de vacunas por zona
        vacunas_por_zona = df_filtrado.groupby('zona').size().reset_index()
        vacunas_por_zona.columns = ['zona', 'total_vacunas']
        
        # Combinar estadísticas
        zona_stats = zona_stats.merge(vacunas_por_zona, on='zona')
        zona_stats.columns = ['zona', 'vacunados', 'establecimientos', 'total_vacunas']
        
        # Crear el mapa base centrado en Ecuador
        ecuador_center = [-1.6675, -83.5975]  # Centro calculado desde el shapefile
        m = folium.Map(location=ecuador_center, zoom_start=6)
        
        # Si tenemos datos de zonificación
        if len(gdf_zonas) > 0:
            # Crear una copia del GeoDataFrame para trabajar
            gdf_work = gdf_zonas.copy()
            
            # Como solo tenemos FID, crear identificadores de zona basados en FID
            gdf_work['zona_id'] = gdf_work['FID'].astype(str)
            gdf_work['nombre_zona'] = 'Zona ' + gdf_work['FID'].astype(str)
            
            # Para demostración, vamos a asignar valores aleatorios de vacunación por zona
            # En una implementación real, esto vendría de los datos reales
            import numpy as np
            np.random.seed(42)  # Para resultados reproducibles
            
            # Simular datos de vacunación para cada zona
            total_vacunas_simuladas = []
            for fid in gdf_work['FID']:
                # Simular vacunas basado en el FID para que sea consistente
                np.random.seed(fid + 42)
                vacunas = np.random.randint(500, 5000)
                total_vacunas_simuladas.append(vacunas)
            
            gdf_work['total_vacunas'] = total_vacunas_simuladas
            gdf_work['vacunados'] = [int(v * 0.8) for v in total_vacunas_simuladas]
            gdf_work['establecimientos'] = [max(1, int(v / 100)) for v in total_vacunas_simuladas]
            
            # Normalizar valores para el choropleth
            min_vacunas = min(gdf_work['total_vacunas'])
            max_vacunas = max(gdf_work['total_vacunas'])
            
            # Crear el choropleth
            folium.Choropleth(
                geo_data=gdf_work,
                name='Distribución de Vacunas por Zona',
                data=gdf_work,
                columns=['FID', 'total_vacunas'],
                key_on='feature.properties.FID',
                fill_color='YlOrRd',
                fill_opacity=0.7,
                line_opacity=0.2,
                legend_name='Total de Vacunas Aplicadas (Simulado)',
                bins=5
            ).add_to(m)
            
            # Agregar tooltips con información detallada
            for idx, row in gdf_work.iterrows():
                if not pd.isna(row.geometry):
                    tooltip_text = f"""
                    <b>Zona:</b> {row['nombre_zona']}<br>
                    <b>FID:</b> {row['FID']}<br>
                    <b>Total Vacunas:</b> {int(row['total_vacunas']):,}<br>
                    <b>Vacunados:</b> {int(row['vacunados']):,}<br>
                    <b>Establecimientos:</b> {int(row['establecimientos']):,}
                    """
                    
                    folium.GeoJson(
                        row.geometry,
                        tooltip=tooltip_text,
                        style_function=lambda x: {
                            'fillColor': 'transparent',
                            'color': 'black',
                            'weight': 1,
                            'fillOpacity': 0
                        }
                    ).add_to(m)
            
            # Agregar nota sobre datos simulados
            note_html = '''
            <div style="position: fixed; 
                        top: 10px; right: 10px; width: 200px; height: 70px; 
                        background-color: white; border:2px solid grey; z-index:9999; 
                        font-size:14px; color: red; font-weight: bold;
                        ">
                <p style="margin: 10px;"><u>Nota:</u><br>
                Los datos mostrados son simulados para demostración.</p>
            </div>
            '''
            m.get_root().html.add_child(folium.Element(note_html))
        
        # Agregar control de capas
        folium.LayerControl().add_to(m)
        
        return m
        
    except Exception as e:
        st.error(f"Error al crear el mapa: {str(e)}")
        import traceback
        st.error(f"Detalles del error: {traceback.format_exc()}")
        return None


def show_general():
    """
    Página principal con información general del sistema de vacunación
    """
    # Obtener datos con manejo de errores
    try:
        df = get_duck_db_data(QUERY_VACUNAS_TEMPORAL_FULL)
        
        # Verificar que el DataFrame no esté vacío y tenga las columnas necesarias
        if df.empty:
            st.error("⚠️ No se encontraron datos de vacunación. Verifique la conexión a la base de datos.")
            st.info("💡 **Posibles causas:**\n"
                   "- Base de datos no disponible\n"
                   "- Tabla 'vacunacion.main.lk_vacunacion_covid' vacía\n"
                   "- Error en la consulta SQL")
            return
            
        # Verificar columnas requeridas
        required_columns = ['anio_aplicacion', 'mes_aplicacion', 'dia_aplicacion', 'fecha_aplicacion']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            st.error(f"⚠️ Faltan columnas requeridas en los datos: {', '.join(missing_columns)}")
            st.info("📋 **Columnas disponibles:** " + ", ".join(df.columns.tolist()))
            return
            
    except Exception as e:
        st.error(f"❌ Error al cargar los datos: {str(e)}")
        st.info("🔧 **Soluciones posibles:**\n"
               "- Verificar configuración de la base de datos\n"
               "- Revisar la variable DUCK_DB_PATH\n"
               "- Confirmar que el archivo .env está configurado")
        return
    
    st.header("Vista General del Sistema")
    
    # Sección de filtros
    st.markdown("### Filtros")
    col_filtro1, col_filtro2, col_filtro3, col_filtro4, col_filtro5, col_filtro6,col_filtro7, col_filtro8 = st.columns([2, 1, 1, 1, 1, 1, 1, 1])
    
    with col_filtro1:
        # Filtro por año (multiselect) con manejo seguro
        años_disponibles = safe_get_unique_values(df, 'anio_aplicacion', [2024])
        
        if not años_disponibles:
            años_disponibles = [2024]
            st.warning("⚠️ No se encontraron años válidos en los datos, usando 2024 por defecto")
            
        años_seleccionados = st.multiselect(
            "Seleccionar Año(s):",
            options=años_disponibles,
            default=[años_disponibles[-1]] if años_disponibles else [2024],
            help="Puedes seleccionar múltiples años para comparar"
        )
        
        # Validar que se haya seleccionado al menos un año
        if not años_seleccionados:
            st.warning("Debes seleccionar al menos un año")
            años_seleccionados = [años_disponibles[-1]] if años_disponibles else [2024]
    
    with col_filtro2:
        # Filtro por mes con manejo seguro
        meses_nombres = {
            1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril",
            5: "Mayo", 6: "Junio", 7: "Julio", 8: "Agosto",
            9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"
        }
        
        # Obtener meses disponibles para los años seleccionados
        if años_seleccionados and not df.empty:
            try:
                df_años = df[df['anio_aplicacion'].isin(años_seleccionados)]
                meses_disponibles = safe_get_unique_values(df_años, 'mes_aplicacion', [1])
            except Exception:
                meses_disponibles = [1]
        else:
            meses_disponibles = [1]
        
        opciones_meses = ["Todos"] + [f"{mes} - {meses_nombres.get(mes, mes)}" for mes in meses_disponibles]
        mes_seleccionado = st.selectbox(
            "Seleccionar Mes:",
            options=opciones_meses,
            index=0
        )
    
    with col_filtro3:
        # Filtro por sexo
        if not df.empty and 'sexo' in df.columns:
            # Filtrar sexos según años y mes ya seleccionados
            if años_seleccionados:
                df_temp = df[df['anio_aplicacion'].isin(años_seleccionados)]
            else:
                df_temp = df.copy()
                
            if mes_seleccionado != "Todos":
                try:
                    mes_numero = int(mes_seleccionado.split(" - ")[0])
                    df_temp = df_temp[df_temp['mes_aplicacion'] == mes_numero]
                except (ValueError, IndexError):
                    pass
            
            sexos_disponibles = sorted(df_temp['sexo'].dropna().unique()) if not df_temp.empty else []
            opciones_sexos = ["Todos"] + [str(sexo) for sexo in sexos_disponibles]
            sexo_seleccionado = st.selectbox(
                "Seleccionar Sexo:",
                options=opciones_sexos,
                index=0
            )
        else:
            sexo_seleccionado = "Todos"
            st.selectbox(
                "Seleccionar Sexo:",
                options=["Todos"],
                index=0,
                disabled=True
            )
    
    with col_filtro4:
        # Filtro por zona
        if not df.empty and 'zona' in df.columns:
            # Filtrar zonas según años, mes y sexo ya seleccionados
            if años_seleccionados:
                df_temp = df[df['anio_aplicacion'].isin(años_seleccionados)]
            else:
                df_temp = df.copy()
                
            if mes_seleccionado != "Todos":
                try:
                    mes_numero = int(mes_seleccionado.split(" - ")[0])
                    df_temp = df_temp[df_temp['mes_aplicacion'] == mes_numero]
                except (ValueError, IndexError):
                    pass
            
            if sexo_seleccionado != "Todos" and 'sexo' in df_temp.columns:
                df_temp = df_temp[df_temp['sexo'] == sexo_seleccionado]
            
            zonas_disponibles = sorted(df_temp['zona'].dropna().unique()) if not df_temp.empty else []
            opciones_zonas = ["Todas"] + [str(zona) for zona in zonas_disponibles]
            zona_seleccionada = st.selectbox(
                "Seleccionar Zona:",
                options=opciones_zonas,
                index=0
            )
        else:
            zona_seleccionada = "Todas"
            st.selectbox(
                "Seleccionar Zona:",
                options=["Todas"],
                index=0,
                disabled=True
            )
    
    with col_filtro5:
        # Filtro por grupo etario
        if not df.empty and 'grupo_etario' in df.columns:
            # Filtrar grupos etarios según años, mes, sexo y zona ya seleccionados
            if años_seleccionados:
                df_temp = df[df['anio_aplicacion'].isin(años_seleccionados)]
            else:
                df_temp = df.copy()
                
            if mes_seleccionado != "Todos":
                try:
                    mes_numero = int(mes_seleccionado.split(" - ")[0])
                    df_temp = df_temp[df_temp['mes_aplicacion'] == mes_numero]
                except (ValueError, IndexError):
                    pass
            
            if sexo_seleccionado != "Todos" and 'sexo' in df_temp.columns:
                df_temp = df_temp[df_temp['sexo'] == sexo_seleccionado]
            
            if zona_seleccionada != "Todas" and 'zona' in df_temp.columns:
                df_temp = df_temp[df_temp['zona'] == zona_seleccionada]
            
            grupos_disponibles = sorted(df_temp['grupo_etario'].dropna().unique()) if not df_temp.empty else []
            opciones_grupos = ["Todos"] + [str(grupo) for grupo in grupos_disponibles]
            grupo_etario_seleccionado = st.selectbox(
                "Seleccionar Grupo Etario:",
                options=opciones_grupos,
                index=0
            )
        else:
            grupo_etario_seleccionado = "Todos"
            st.selectbox(
                "Seleccionar Grupo Etario:",
                options=["Todos"],
                index=0,
                disabled=True
            )
    
    with col_filtro6:
        st.write("")  
    
    with col_filtro7:
        st.write("")  
    
    with col_filtro8:
        st.write("")  # Espacio para alineación
        if st.button("Limpiar Filtros"):
            st.rerun()
    
    # Aplicar filtros
    df_filtrado = df.copy()
    if not df.empty:
        # Filtrar por años (múltiples)
        if años_seleccionados:
            df_filtrado = df_filtrado[df_filtrado['anio_aplicacion'].isin(años_seleccionados)]
        
        # Filtrar por mes
        if mes_seleccionado != "Todos":
            try:
                mes_numero = int(mes_seleccionado.split(" - ")[0])
                df_filtrado = df_filtrado[df_filtrado['mes_aplicacion'] == mes_numero]
            except (ValueError, IndexError):
                # En caso de error, mantener todos los datos de los años
                pass
        
        # Filtrar por sexo
        if sexo_seleccionado != "Todos" and 'sexo' in df_filtrado.columns:
            df_filtrado = df_filtrado[df_filtrado['sexo'] == sexo_seleccionado]
        
        # Filtrar por zona
        if zona_seleccionada != "Todas" and 'zona' in df_filtrado.columns:
            df_filtrado = df_filtrado[df_filtrado['zona'] == zona_seleccionada]
        
        # Filtrar por grupo etario
        if grupo_etario_seleccionado != "Todos" and 'grupo_etario' in df_filtrado.columns:
            df_filtrado = df_filtrado[df_filtrado['grupo_etario'] == grupo_etario_seleccionado]
    
    # Mostrar información de filtros aplicados
    if not df_filtrado.empty:
        # Texto de los años
        if len(años_seleccionados) == 1:
            años_texto = str(años_seleccionados[0])
        elif len(años_seleccionados) <= 3:
            años_texto = ", ".join(map(str, años_seleccionados))
        else:
            años_texto = f"{años_seleccionados[0]}-{años_seleccionados[-1]} ({len(años_seleccionados)} años)"
        
        # Texto del mes
        mes_texto = " (Todos los meses)"
        if mes_seleccionado != "Todos":
            try:
                mes_texto = f" - {mes_seleccionado.split(' - ')[1]}"
            except IndexError:
                mes_texto = f" - {mes_seleccionado}"
        
        # Texto de la zona
        zona_texto = ""
        if zona_seleccionada != "Todas":
            zona_texto = f" | Zona: {zona_seleccionada}"
        
        # Texto del sexo
        sexo_texto = ""
        if sexo_seleccionado != "Todos":
            sexo_texto = f" | Sexo: {sexo_seleccionado}"
        
        # Texto del grupo etario
        grupo_etario_texto = ""
        if grupo_etario_seleccionado != "Todos":
            grupo_etario_texto = f" | Grupo Etario: {grupo_etario_seleccionado}"
        
        st.info(f"Mostrando datos para: {años_texto}{mes_texto}{zona_texto}{sexo_texto}{grupo_etario_texto}")
        
        # Mostrar resumen de registros filtrados
        total_registros = len(df_filtrado)
        porcentaje_filtrado = (total_registros / len(df) * 100) if len(df) > 0 else 0
        st.caption(f"{total_registros:,} registros ({porcentaje_filtrado:.1f}% del total)")
    else:
        st.warning("No hay datos disponibles para los filtros seleccionados")
    
    st.markdown("---")
    
    
    # Métricas principales
    col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12 = st.columns(12)
    
    with col1:
        total_vacunas = df_filtrado.groupby('num_iden').size().sum() if not df_filtrado.empty else 0
        delta_vacunas = delta_total_vacunas(df_filtrado) if not df_filtrado.empty else 0
        st.metric(
            label="Total Vacunas Aplicadas",
            value=f"{total_vacunas:,}",
            delta=f"{delta_vacunas}"
        )
    
    with col2:
        total_establecimientos = total_establecimiento(df_filtrado) if not df_filtrado.empty else 0
        st.metric(
            label="Total Establecimientos",
            value=f"{total_establecimientos}",
            delta="2"
        )
    
    with col3:
        total_vacunados = df_filtrado['num_iden'].nunique() if not df_filtrado.empty else 0
        delta_vacunados_val = delta_vacunados(df_filtrado) if not df_filtrado.empty else 0
        st.metric(
            label="Vacunados",
            value=f"{total_vacunados:,}",
            delta=f"{delta_vacunados_val}"
        )
    
    with col5:
        st.metric(
            label="Fechas en Período",
            value=f"{df_filtrado['fecha_aplicacion'].nunique()}"
        )
    
    with col6:
        vacunas_unicas = df_filtrado['nombre_vacuna'].nunique() if 'nombre_vacuna' in df_filtrado.columns else 0
        st.metric(
            label="Tipos de Vacunas",
            value=f"{vacunas_unicas}"
        )
    
    with col7:
        if 'zona' in df_filtrado.columns:
            zonas_activas = df_filtrado['zona'].nunique()
            st.metric(
                label="Zonas Activas",
                value=f"{zonas_activas}"
            )
        else:
            st.metric(
                label="Registros Totales",
                value=f"{len(df_filtrado):,}"
                )
    
    # Estadísticas por sexo (si hay datos y el sexo no está filtrado)
    if not df_filtrado.empty and sexo_seleccionado == "Todos" and 'sexo' in df_filtrado.columns:
        st.markdown("---")
        st.subheader("Distribución por Sexo")
        
        # Calcular estadísticas por sexo
        stats_sexo = df_filtrado.groupby('sexo').agg({
            'num_iden': 'nunique',  # Vacunados únicos
            'unicodigo': 'nunique'  # Establecimientos únicos
        }).reset_index()
        
        # Contar total de vacunas por sexo
        vacunas_por_sexo = df_filtrado.groupby('sexo').size().reset_index()
        vacunas_por_sexo.columns = ['sexo', 'total_vacunas']
        
        # Combinar estadísticas
        stats_sexo = stats_sexo.merge(vacunas_por_sexo, on='sexo')
        stats_sexo.columns = ['Sexo', 'Vacunados', 'Establecimientos', 'Total Vacunas']
        
        # Mostrar en columnas
        col_sexo1, col_sexo2 = st.columns([2, 1])
        
        with col_sexo1:
            # Tabla de estadísticas por sexo
            st.dataframe(
                stats_sexo.sort_values('Total Vacunas', ascending=False),
                hide_index=True,
                use_container_width=True
            )
            
        with col_sexo2:
            # Gráfico de pastel de distribución por sexo
            fig_pie_sexo = px.pie(
                stats_sexo,
                values='Total Vacunas',
                names='Sexo',
                title='Distribución de Vacunas por Sexo'
            )
            fig_pie_sexo.update_layout(height=300)
            st.plotly_chart(fig_pie_sexo, use_container_width=True)
    
    # Estadísticas por zona (si hay datos y la zona no está filtrada)
    if not df_filtrado.empty and zona_seleccionada == "Todas" and 'zona' in df_filtrado.columns:
        st.markdown("---")
        st.subheader("Distribución por Zonas")
        
        # Cargar datos geográficos de zonas
        gdf_zonas = load_zonas_planificacion()
        
        # Calcular estadísticas por zona
        stats_zona = df_filtrado.groupby('zona').agg({
            'num_iden': 'nunique',  # Vacunados únicos
            'unicodigo': 'nunique'  # Establecimientos únicos
        }).reset_index()
        
        # Contar total de vacunas por zona
        vacunas_por_zona = df_filtrado.groupby('zona').size().reset_index()
        vacunas_por_zona.columns = ['zona', 'total_vacunas']
        
        # Combinar estadísticas
        stats_zona = stats_zona.merge(vacunas_por_zona, on='zona')
        stats_zona.columns = ['Zona', 'Vacunados', 'Establecimientos', 'Total Vacunas']
        
        # Crear pestañas para diferentes vistas
        tab_tabla, tab_grafico, tab_mapa = st.tabs(["📊 Tabla de Datos", "📈 Gráfico Circular", "🗺️ Mapa Geográfico"])
        
        with tab_tabla:
            # Mostrar en columnas para la tabla
            col_zona1, col_zona2 = st.columns([3, 1])
            
            with col_zona1:
                # Tabla de estadísticas por zona
                st.dataframe(
                    stats_zona.sort_values('Total Vacunas', ascending=False),
                    hide_index=True,
                    use_container_width=True,
                    column_config={
                        "Total Vacunas": st.column_config.NumberColumn(
                            "Total Vacunas",
                            format="%d"
                        ),
                        "Vacunados": st.column_config.NumberColumn(
                            "Vacunados",
                            format="%d"
                        ),
                        "Establecimientos": st.column_config.NumberColumn(
                            "Establecimientos",
                            format="%d"
                        )
                    }
                )
            
            with col_zona2:
                # Métricas resumidas
                zona_con_mas_vacunas = stats_zona.loc[stats_zona['Total Vacunas'].idxmax(), 'Zona'] if not stats_zona.empty else "N/A"
                max_vacunas = stats_zona['Total Vacunas'].max() if not stats_zona.empty else 0
                
                st.metric(
                    label="Zona con Más Vacunas",
                    value=str(zona_con_mas_vacunas),
                    delta=f"{max_vacunas:,} vacunas"
                )
                
                total_zonas = len(stats_zona)
                st.metric(
                    label="Total de Zonas",
                    value=f"{total_zonas}"
                )
                
                promedio_vacunas = stats_zona['Total Vacunas'].mean() if not stats_zona.empty else 0
                st.metric(
                    label="Promedio por Zona",
                    value=f"{promedio_vacunas:,.0f}"
                )
        
        with tab_grafico:
            # Crear dos columnas para gráficos
            col_grafico1, col_grafico2 = st.columns([1, 1])
            
            with col_grafico1:
                # Gráfico de pastel de distribución por zona
                fig_pie = px.pie(
                    stats_zona,
                    values='Total Vacunas',
                    names='Zona',
                    title='Distribución de Vacunas por Zona'
                )
                fig_pie.update_layout(height=400)
                st.plotly_chart(fig_pie, use_container_width=True)
            
            with col_grafico2:
                # Gráfico de barras horizontales
                fig_bar = px.bar(
                    stats_zona.sort_values('Total Vacunas', ascending=True),
                    x='Total Vacunas',
                    y='Zona',
                    orientation='h',
                    title='Vacunas por Zona',
                    color='Total Vacunas',
                    color_continuous_scale='viridis'
                )
                fig_bar.update_layout(height=400)
                st.plotly_chart(fig_bar, use_container_width=True)
        
        with tab_mapa:
            if GEOSPATIAL_AVAILABLE and gdf_zonas is not None:
                st.write("#### Mapa de Distribución Geográfica de Vacunación por Zonas")
                
                # Crear el mapa
                mapa = create_zona_distribution_map(df_filtrado, gdf_zonas)
                
                if mapa is not None:
                    # Mostrar información sobre el mapa
                    st.info("🗺️ **Instrucciones del Mapa:**\n"
                           "- Haga clic en las zonas para ver información detallada\n"
                           "- Use los controles de zoom para navegar\n"
                           "- Los colores más intensos indican mayor número de vacunas aplicadas")
                    
                    # Mostrar el mapa
                    st_folium(mapa, width=700, height=500)
                    
                    # Mostrar información adicional del mapa
                    col_info1, col_info2 = st.columns([1, 1])
                    
                    with col_info1:
                        st.write("##### Información de las Zonas de Planificación")
                        if len(gdf_zonas) > 0:
                            st.write(f"- **Total de zonas cargadas:** {len(gdf_zonas)}")
                            # Mostrar algunas columnas disponibles en el shapefile
                            columnas_disponibles = [col for col in gdf_zonas.columns if col != 'geometry'][:5]
                            if columnas_disponibles:
                                st.write(f"- **Campos disponibles:** {', '.join(columnas_disponibles)}")
                    
                    with col_info2:
                        st.write("##### Estadísticas del Mapa")
                        zonas_con_datos = len(stats_zona)
                        st.write(f"- **Zonas con datos de vacunación:** {zonas_con_datos}")
                        st.write(f"- **Total de vacunas mapeadas:** {stats_zona['Total Vacunas'].sum():,}")
                        st.write(f"- **Cobertura promedio por zona:** {stats_zona['Total Vacunas'].mean():.0f} vacunas")
                
                else:
                    st.error("No se pudo generar el mapa. Verifique que los datos geográficos estén correctamente cargados.")
            
            else:
                st.warning("📍 **Funcionalidad de mapas no disponible**\n\n"
                          "Para visualizar el mapa geográfico de distribución por zonas, instale las siguientes dependencias:\n"
                          "```bash\n"
                          "pip install geopandas folium streamlit-folium\n"
                          "```\n\n"
                          "Una vez instaladas, reinicie la aplicación para ver el mapa interactivo.")
                
                # Mostrar un gráfico alternativo mientras tanto
                st.write("#### Vista Alternativa: Gráfico de Barras por Zona")
                fig_alt = px.bar(
                    stats_zona.sort_values('Total Vacunas', ascending=False),
                    x='Zona',
                    y='Total Vacunas',
                    title='Distribución de Vacunas por Zona',
                    color='Total Vacunas',
                    color_continuous_scale='blues'
                )
                fig_alt.update_layout(
                    xaxis_tickangle=-45,
                    height=400
                )
                st.plotly_chart(fig_alt, use_container_width=True)
    
    # Estadísticas por grupo etario (si hay datos y el grupo etario no está filtrado)
    if not df_filtrado.empty and grupo_etario_seleccionado == "Todos" and 'grupo_etario' in df_filtrado.columns:
        st.markdown("---")
        st.subheader("Distribución por Grupo Etario")
        
        # Calcular estadísticas por grupo etario
        stats_grupo = df_filtrado.groupby('grupo_etario').agg({
            'num_iden': 'nunique',  # Vacunados únicos
            'unicodigo': 'nunique'  # Establecimientos únicos
        }).reset_index()
        
        # Contar total de vacunas por grupo etario
        vacunas_por_grupo = df_filtrado.groupby('grupo_etario').size().reset_index()
        vacunas_por_grupo.columns = ['grupo_etario', 'total_vacunas']
        
        # Combinar estadísticas
        stats_grupo = stats_grupo.merge(vacunas_por_grupo, on='grupo_etario')
        stats_grupo.columns = ['Grupo Etario', 'Vacunados', 'Establecimientos', 'Total Vacunas']
        
        # Mostrar en columnas
        col_grupo1, col_grupo2 = st.columns([2, 1])
        
        with col_grupo1:
            # Tabla de estadísticas por grupo etario
            st.dataframe(
                stats_grupo.sort_values('Total Vacunas', ascending=False),
                hide_index=True,
                width=True
            )
        
        with col_grupo2:
            # Gráfico de pastel de distribución por grupo etario
            fig_pie_grupo = px.pie(
                stats_grupo,
                values='Total Vacunas',
                names='Grupo Etario',
                title='Distribución de Vacunas por Grupo Etario'
            )
            fig_pie_grupo.update_layout(height=300)
            st.plotly_chart(fig_pie_grupo, use_container_width=True)
    
    # Análisis de Dosis Aplicadas
    if not df_filtrado.empty and 'dosis_aplicada' in df_filtrado.columns:
        st.markdown("---")
        st.subheader("Análisis de Dosis Aplicadas")
        
        # Calcular estadísticas de dosis
        dosis_stats = df_filtrado['dosis_aplicada'].value_counts().sort_index()
        
        if len(dosis_stats) > 0:
            # Crear dos columnas para el análisis de dosis
            col_dosis1, col_dosis2 = st.columns([2, 1])
            
            with col_dosis1:
                # Gráfico de barras de dosis aplicadas
                fig_dosis = px.bar(
                    x=dosis_stats.index,
                    y=dosis_stats.values,
                    labels={'x': 'Tipo de Dosis', 'y': 'Cantidad'},
                    title='Distribución de Dosis Aplicadas',
                    color=dosis_stats.values,
                    color_continuous_scale='viridis'
                )
                
                # Personalizar el gráfico
                fig_dosis.update_layout(
                    height=400,
                    showlegend=False,
                    xaxis_title="Tipo de Dosis",
                    yaxis_title="Cantidad de Aplicaciones"
                )
                
                # Agregar valores en las barras
                fig_dosis.update_traces(
                    texttemplate='%{y:,}',
                    textposition='outside'
                )
                
                st.plotly_chart(fig_dosis, use_container_width=True)
            
            with col_dosis2:
                # Gráfico de pastel de distribución de dosis
                fig_pie_dosis = px.pie(
                    values=dosis_stats.values,
                    names=dosis_stats.index,
                    title='Proporción de Dosis'
                )
                fig_pie_dosis.update_layout(height=400)
                st.plotly_chart(fig_pie_dosis, use_container_width=True)
            
            # Tabla detallada de dosis
            st.write("#### Detalle por Tipo de Dosis")
            
            # Crear DataFrame con estadísticas detalladas
            dosis_detalle = []
            for dosis in dosis_stats.index:
                df_dosis = df_filtrado[df_filtrado['dosis_aplicada'] == dosis]
                detalle = {
                    'Tipo de Dosis': dosis,
                    'Total Aplicaciones': len(df_dosis),
                    'Personas Únicas': df_dosis['num_iden'].nunique(),
                    'Establecimientos': df_dosis['unicodigo'].nunique(),
                    'Porcentaje': f"{(len(df_dosis) / len(df_filtrado) * 100):.1f}%"
                }
                dosis_detalle.append(detalle)
            
            df_dosis_detalle = pd.DataFrame(dosis_detalle)
            
            # Mostrar tabla con formato
            st.dataframe(
                df_dosis_detalle,
                hide_index=True,
                use_container_width=True,
                column_config={
                    "Total Aplicaciones": st.column_config.NumberColumn(
                        "Total Aplicaciones",
                        format="%d"
                    ),
                    "Personas Únicas": st.column_config.NumberColumn(
                        "Personas Únicas",
                        format="%d"
                    ),
                    "Establecimientos": st.column_config.NumberColumn(
                        "Establecimientos",
                        format="%d"
                    )
                }
            )
            
            # Métricas adicionales de dosis
            col_metr1, col_metr2, col_metr3 = st.columns(3)
            
            with col_metr1:
                dosis_mas_aplicada = dosis_stats.index[0]
                cantidad_mas_aplicada = dosis_stats.values[0]
                st.metric(
                    label="Dosis Más Aplicada",
                    value=f"{dosis_mas_aplicada}",
                    delta=f"{cantidad_mas_aplicada:,} aplicaciones"
                )
            
            with col_metr2:
                promedio_por_tipo = dosis_stats.mean()
                st.metric(
                    label="Promedio por Tipo",
                    value=f"{promedio_por_tipo:,.0f}"
                )
            
            with col_metr3:
                total_tipos_dosis = len(dosis_stats)
                st.metric(
                    label="Tipos de Dosis",
                    value=f"{total_tipos_dosis}"
                )
            
            # Análisis de dosis por sexo
            if 'sexo' in df_filtrado.columns:
                st.write("#### Distribución de Dosis por Sexo")
                
                # Crear datos para el análisis cruzado de dosis y sexo
                dosis_sexo = df_filtrado.groupby(['dosis_aplicada', 'sexo']).size().unstack(fill_value=0)
                
                if not dosis_sexo.empty:
                    # Crear dos columnas para los gráficos de sexo
                    col_sexo_dosis1, col_sexo_dosis2 = st.columns([3, 2])
                    
                    with col_sexo_dosis1:
                        # Gráfico de mariposa (butterfly chart) para dosis por sexo
                        fig_mariposa = go.Figure()
                        
                        # Colores para hombre y mujer
                        colores_sexo = {'M': '#3498db', 'F': '#e74c3c'}
                        
                        # Preparar datos para el gráfico de mariposa
                        tipos_dosis = list(dosis_sexo.index)
                        
                        # Obtener valores para hombres y mujeres con validación
                        if 'M' in dosis_sexo.columns:
                            valores_hombres = dosis_sexo['M'].values.tolist()
                        else:
                            valores_hombres = [0] * len(tipos_dosis)
                            
                        if 'F' in dosis_sexo.columns:
                            valores_mujeres = dosis_sexo['F'].values.tolist()
                        else:
                            valores_mujeres = [0] * len(tipos_dosis)
                        
                        # Los valores de hombres van hacia la izquierda (negativos)
                        valores_hombres_negativos = [-valor for valor in valores_hombres]
                        
                        # Agregar barras para hombres (lado izquierdo)
                        fig_mariposa.add_trace(go.Bar(
                            name='Hombres',
                            y=tipos_dosis,
                            x=valores_hombres_negativos,
                            orientation='h',
                            marker_color='#3498db',
                            text=[f'{abs(val):,}' for val in valores_hombres_negativos],
                            textposition='outside',
                            hovertemplate='<b>%{y}</b><br>Hombres: %{text}<extra></extra>',
                            offsetgroup=1
                        ))
                        
                        # Agregar barras para mujeres (lado derecho)
                        fig_mariposa.add_trace(go.Bar(
                            name='Mujeres',
                            y=tipos_dosis,
                            x=valores_mujeres,
                            orientation='h',
                            marker_color='#e74c3c',
                            text=[f'{val:,}' for val in valores_mujeres],
                            textposition='outside',
                            hovertemplate='<b>%{y}</b><br>Mujeres: %{text}<extra></extra>',
                            offsetgroup=2
                        ))
                        
                        # Calcular el rango máximo para centrar el gráfico
                        max_hombres = max(valores_hombres) if valores_hombres and len(valores_hombres) > 0 else 0
                        max_mujeres = max(valores_mujeres) if valores_mujeres and len(valores_mujeres) > 0 else 0
                        max_valor = max(max_hombres, max_mujeres)
                        rango_x = max_valor * 1.2 if max_valor > 0 else 1000
                        
                        # Configurar el layout del gráfico de mariposas
                        fig_mariposa.update_layout(
                            title='Distribución de Dosis por Sexo (Gráfico Mariposa)',
                            xaxis=dict(
                                title='Cantidad de Dosis',
                                range=[-rango_x, rango_x],
                                tickvals=[-rango_x * 0.75, -rango_x * 0.5, -rango_x * 0.25, 0, 
                                         rango_x * 0.25, rango_x * 0.5, rango_x * 0.75],
                                ticktext=[f'{int(abs(rango_x * 0.75)):,}', f'{int(abs(rango_x * 0.5)):,}', 
                                         f'{int(abs(rango_x * 0.25)):,}', '0', 
                                         f'{int(abs(rango_x * 0.25)):,}', f'{int(abs(rango_x * 0.5)):,}', 
                                         f'{int(abs(rango_x * 0.75)):,}'],
                                zeroline=True,
                                zerolinecolor='gray',
                                zerolinewidth=2,
                                showgrid=True,
                                gridcolor='lightgray'
                            ),
                            yaxis=dict(
                                title='Tipo de Dosis',
                                autorange='reversed'  # Para que las categorías aparezcan en orden natural
                            ),
                            barmode='relative',
                            height=500,
                            legend=dict(
                                orientation="h",
                                yanchor="bottom",
                                y=1.02,
                                xanchor="center",
                                x=0.5
                            ),
                            plot_bgcolor='white',
                            paper_bgcolor='white'
                        )
                        
                        # Agregar anotaciones para indicar los lados
                        fig_mariposa.add_annotation(
                            text="← Hombres",
                            x=-rango_x * 0.6,
                            y=len(tipos_dosis),
                            showarrow=False,
                            font=dict(size=12, color='#3498db'),
                            xanchor="center"
                        )
                        
                        fig_mariposa.add_annotation(
                            text="Mujeres →",
                            x=rango_x * 0.6,
                            y=len(tipos_dosis),
                            showarrow=False,
                            font=dict(size=12, color='#e74c3c'),
                            xanchor="center"
                        )
                        
                        st.plotly_chart(fig_mariposa, use_container_width=True)
                    
                    with col_sexo_dosis2:
                        # Gráfico de pastel para totales por sexo (todas las dosis)
                        total_por_sexo = df_filtrado['sexo'].value_counts()
                        
                        # Mapear etiquetas de sexo
                        total_por_sexo_labels = []
                        for sexo in total_por_sexo.index:
                            if sexo == 'M':
                                total_por_sexo_labels.append('Hombres')
                            elif sexo == 'F':
                                total_por_sexo_labels.append('Mujeres')
                            else:
                                total_por_sexo_labels.append(f'Sexo {sexo}')
                        
                        fig_pie_sexo_total = px.pie(
                            values=total_por_sexo.values,
                            names=total_por_sexo_labels,
                            title='Total General por Sexo',
                            color_discrete_map={'Hombres': '#3498db', 'Mujeres': '#e74c3c'}
                        )
                        fig_pie_sexo_total.update_layout(height=400)
                        st.plotly_chart(fig_pie_sexo_total, use_container_width=True)
                    
                    # Tabla detallada de dosis por sexo
                    st.write("#### Detalle de Dosis por Sexo")
                    
                    # Crear tabla resumen
                    tabla_dosis_sexo = []
                    
                    for dosis in dosis_sexo.index:
                        fila = {'Tipo de Dosis': dosis}
                        
                        for sexo in dosis_sexo.columns:
                            sexo_label = 'Hombres' if sexo == 'M' else 'Mujeres' if sexo == 'F' else f'Sexo {sexo}'
                            fila[sexo_label] = dosis_sexo.loc[dosis, sexo]
                        
                        # Calcular total y porcentajes
                        total_fila = sum([dosis_sexo.loc[dosis, sexo] for sexo in dosis_sexo.columns])
                        fila['Total'] = total_fila
                        
                        # Calcular porcentaje de participación femenina si hay ambos sexos
                        if 'F' in dosis_sexo.columns and 'M' in dosis_sexo.columns and total_fila > 0:
                            porcentaje_f = (dosis_sexo.loc[dosis, 'F'] / total_fila * 100)
                            fila['% Mujeres'] = f"{porcentaje_f:.1f}%"
                        
                        tabla_dosis_sexo.append(fila)
                    
                    df_tabla_dosis_sexo = pd.DataFrame(tabla_dosis_sexo)
                    
                    # Configurar formato de columnas
                    column_config = {}
                    for col in df_tabla_dosis_sexo.columns:
                        if col not in ['Tipo de Dosis', '% Mujeres']:
                            column_config[col] = st.column_config.NumberColumn(
                                col,
                                format="%d"
                            )
                    
                    st.dataframe(
                        df_tabla_dosis_sexo,
                        hide_index=True,
                        use_container_width=True,
                        column_config=column_config
                    )
                    
                    # Métricas comparativas por sexo
                    col_comp1, col_comp2, col_comp3 = st.columns(3)
                    
                    with col_comp1:
                        if 'F' in total_por_sexo.index and 'M' in total_por_sexo.index:
                            diferencia = abs(total_por_sexo['F'] - total_por_sexo['M'])
                            st.metric(
                                label="Diferencia H/M",
                                value=f"{diferencia:,}",
                                delta=f"Brecha de género"
                            )
                        else:
                            st.metric(
                                label="Diferencia H/M",
                                value="N/A"
                            )
                    
                    with col_comp2:
                        if len(total_por_sexo) > 0:
                            sexo_dominante = 'Mujeres' if total_por_sexo.index[0] == 'F' else 'Hombres' if total_por_sexo.index[0] == 'M' else f'Sexo {total_por_sexo.index[0]}'
                            st.metric(
                                label="Sexo Predominante",
                                value=sexo_dominante,
                                delta=f"{total_por_sexo.values[0]:,} dosis"
                            )
                        else:
                            st.metric(
                                label="Sexo Predominante",
                                value="N/A"
                            )
                    
                    with col_comp3:
                        if 'F' in total_por_sexo.index and 'M' in total_por_sexo.index:
                            total_general = total_por_sexo['F'] + total_por_sexo['M']
                            paridad = min(total_por_sexo['F'], total_por_sexo['M']) / max(total_por_sexo['F'], total_por_sexo['M']) * 100
                            st.metric(
                                label="Índice de Paridad",
                                value=f"{paridad:.1f}%",
                                delta="Equilibrio de género"
                            )
                        else:
                            st.metric(
                                label="Índice de Paridad",
                                value="N/A"
                            )
    
    # Análisis de grupo etario por sexo
    if not df_filtrado.empty and sexo_seleccionado == "Todos" and grupo_etario_seleccionado == "Todos" and 'grupo_etario' in df_filtrado.columns and 'sexo' in df_filtrado.columns:
        st.markdown("---")
        st.subheader("Distribución de Grupo Etario por Sexo")
        
        # Crear datos para el análisis cruzado de grupo etario y sexo
        grupo_sexo = df_filtrado.groupby(['grupo_etario', 'sexo']).size().unstack(fill_value=0)
        
        if not grupo_sexo.empty:
            # Crear dos columnas para los gráficos
            col_grupo_sexo1, col_grupo_sexo2 = st.columns([3, 2])
            
            with col_grupo_sexo1:
                # Gráfico de barras apiladas para grupo etario por sexo
                fig_barras_grupo = go.Figure()
                
                # Colores para hombre y mujer
                colores_sexo = {'M': '#3498db', 'F': '#e74c3c'}
                
                # Preparar datos
                grupos_etarios = list(grupo_sexo.index)
                
                # Obtener valores para hombres y mujeres con validación
                if 'M' in grupo_sexo.columns:
                    valores_hombres = grupo_sexo['M'].values.tolist()
                    fig_barras_grupo.add_trace(go.Bar(
                        name='Hombres',
                        x=grupos_etarios,
                        y=valores_hombres,
                        marker_color='#3498db',
                        text=[f'{val:,}' for val in valores_hombres],
                        textposition='auto'
                    ))
                
                if 'F' in grupo_sexo.columns:
                    valores_mujeres = grupo_sexo['F'].values.tolist()
                    fig_barras_grupo.add_trace(go.Bar(
                        name='Mujeres',
                        x=grupos_etarios,
                        y=valores_mujeres,
                        marker_color='#e74c3c',
                        text=[f'{val:,}' for val in valores_mujeres],
                        textposition='auto'
                    ))
                
                fig_barras_grupo.update_layout(
                    title='Distribución por Grupo Etario y Sexo',
                    xaxis_title='Grupo Etario',
                    yaxis_title='Cantidad de Vacunas',
                    barmode='group',
                    height=400,
                    legend=dict(
                        orientation="h",
                        yanchor="bottom",
                        y=1.02,
                        xanchor="center",
                        x=0.5
                    )
                )
                
                st.plotly_chart(fig_barras_grupo, use_container_width=True)
            
            with col_grupo_sexo2:
                # Gráfico de pastel para totales por grupo etario
                total_por_grupo = df_filtrado['grupo_etario'].value_counts()
                
                fig_pie_grupo_total = px.pie(
                    values=total_por_grupo.values,
                    names=total_por_grupo.index,
                    title='Total por Grupo Etario'
                )
                fig_pie_grupo_total.update_layout(height=400)
                st.plotly_chart(fig_pie_grupo_total, use_container_width=True)
            
            # Tabla detallada de grupo etario por sexo
            st.write("#### Detalle por Grupo Etario y Sexo")
            
            # Crear tabla resumen
            tabla_grupo_sexo = []
            
            for grupo in grupo_sexo.index:
                fila = {'Grupo Etario': grupo}
                
                for sexo in grupo_sexo.columns:
                    sexo_label = 'Hombres' if sexo == 'M' else 'Mujeres' if sexo == 'F' else f'Sexo {sexo}'
                    fila[sexo_label] = grupo_sexo.loc[grupo, sexo]
                
                # Calcular total y porcentajes
                total_fila = sum([grupo_sexo.loc[grupo, sexo] for sexo in grupo_sexo.columns])
                fila['Total'] = total_fila
                
                # Calcular porcentaje de participación femenina si hay ambos sexos
                if 'F' in grupo_sexo.columns and 'M' in grupo_sexo.columns and total_fila > 0:
                    porcentaje_f = (grupo_sexo.loc[grupo, 'F'] / total_fila * 100)
                    fila['% Mujeres'] = f"{porcentaje_f:.1f}%"
                
                tabla_grupo_sexo.append(fila)
            
            df_tabla_grupo_sexo = pd.DataFrame(tabla_grupo_sexo)
            
            # Configurar formato de columnas
            column_config_grupo = {}
            for col in df_tabla_grupo_sexo.columns:
                if col not in ['Grupo Etario', '% Mujeres']:
                    column_config_grupo[col] = st.column_config.NumberColumn(
                        col,
                        format="%d"
                    )
            
            st.dataframe(
                df_tabla_grupo_sexo,
                hide_index=True,
                use_container_width=True,
                column_config=column_config_grupo
            )
            
            # Métricas por grupo etario
            col_grupo_metr1, col_grupo_metr2, col_grupo_metr3 = st.columns(3)
            
            with col_grupo_metr1:
                grupo_dominante = total_por_grupo.index[0] if len(total_por_grupo) > 0 else "N/A"
                st.metric(
                    label="Grupo Etario Dominante",
                    value=str(grupo_dominante),
                    delta=f"{total_por_grupo.values[0]:,} dosis" if len(total_por_grupo) > 0 else "N/A"
                )
            
            with col_grupo_metr2:
                total_grupos = len(total_por_grupo)
                st.metric(
                    label="Grupos Etarios Activos",
                    value=f"{total_grupos}"
                )
            
            with col_grupo_metr3:
                if len(total_por_grupo) > 1:
                    diferencia_grupos = total_por_grupo.values[0] - total_por_grupo.values[1]
                    st.metric(
                        label="Diferencia 1° vs 2°",
                        value=f"{diferencia_grupos:,}",
                        delta="Brecha entre grupos"
                    )
                else:
                    st.metric(
                        label="Diferencia 1° vs 2°",
                        value="N/A"
                    )
    
    # Sección de Análisis de Género con Gráfico de Mariposa
    if not df_filtrado.empty and 'sexo' in df_filtrado.columns and 'grupo_etario' in df_filtrado.columns:
        st.markdown("---")
        st.header("📊 Análisis Demográfico por Género")
        st.markdown("*Visualización especializada con gráfico de mariposa (butterfly chart)*")
        
        # Configuración de la sección de género
        col_config1, col_config2, col_config3 = st.columns([2, 1, 1])
        
        with col_config1:
            st.info("💡 **Gráfico de Mariposa**: Permite comparar la distribución por género de forma simétrica, "
                   "facilitando la identificación de patrones demográficos y brechas entre hombres y mujeres.")
        
        with col_config2:
            tipo_mariposa = st.selectbox(
                "Estilo del Gráfico:",
                ["Avanzado", "Básico"],
                help="Selecciona el nivel de detalle del gráfico"
            )
        
        with col_config3:
            mostrar_metricas = st.checkbox(
                "Mostrar métricas detalladas",
                value=True,
                help="Incluir métricas adicionales de análisis de género"
            )
        
        # Preparar datos para el gráfico de mariposa
        try:
            # Crear grupos de edad si no existe la columna grupo_etario
            if 'grupo_etario' not in df_filtrado.columns and 'edad' in df_filtrado.columns:
                # Crear grupos de edad basados en la edad
                df_temp = df_filtrado.copy()
                df_temp['grupo_etario'] = pd.cut(
                    df_temp['edad'], 
                    bins=[0, 5, 11, 17, 29, 39, 49, 59, 69, 100],
                    labels=['0-5', '6-11', '12-17', '18-29', '30-39', '40-49', '50-59', '60-69', '70+'],
                    right=True
                )
            else:
                df_temp = df_filtrado.copy()
            
            # Mapear sexo para consistencia
            if 'sexo' in df_temp.columns:
                sexo_mapping = {'M': 'Masculino', 'F': 'Femenino', 'H': 'Masculino', 'MASCULINO': 'Masculino', 'FEMENINO': 'Femenino'}
                df_temp['genero'] = df_temp['sexo'].map(sexo_mapping).fillna(df_temp['sexo'])
            
            # Agrupar datos por género y grupo etario
            if 'grupo_etario' in df_temp.columns:
                datos_mariposa = df_temp.groupby(['grupo_etario', 'genero']).size().reset_index()
                datos_mariposa.columns = ['edad_grupo', 'genero', 'cantidad']
            else:
                # Crear datos simulados si no hay grupos etarios
                import numpy as np
                np.random.seed(42)
                grupos_edad = ['0-5', '6-11', '12-17', '18-29', '30-39', '40-49', '50-59', '60-69', '70+']
                
                datos_mariposa = []
                total_masculino = len(df_temp[df_temp['genero'] == 'Masculino']) if 'genero' in df_temp.columns else len(df_temp) // 2
                total_femenino = len(df_temp[df_temp['genero'] == 'Femenino']) if 'genero' in df_temp.columns else len(df_temp) // 2
                
                for grupo in grupos_edad:
                    # Distribución proporcional simulada
                    prop = np.random.beta(2, 2)
                    masc_cantidad = int(total_masculino * prop / len(grupos_edad))
                    fem_cantidad = int(total_femenino * prop / len(grupos_edad))
                    
                    datos_mariposa.append({'edad_grupo': grupo, 'genero': 'Masculino', 'cantidad': masc_cantidad})
                    datos_mariposa.append({'edad_grupo': grupo, 'genero': 'Femenino', 'cantidad': fem_cantidad})
                
                datos_mariposa = pd.DataFrame(datos_mariposa)
            
            # Crear el gráfico de mariposa según la configuración
            if tipo_mariposa == "Avanzado":
                fig_mariposa = crear_grafico_mariposa_avanzado(datos_mariposa)
            else:
                fig_mariposa = crear_grafico_mariposa_genero(datos_mariposa)
            
            # Mostrar el gráfico
            st.plotly_chart(fig_mariposa, use_container_width=True)
            
            # Mostrar métricas detalladas si está habilitado
            if mostrar_metricas and not datos_mariposa.empty:
                st.markdown("### 📈 Métricas Demográficas Detalladas")
                
                # Calcular métricas por género
                metricas_genero = datos_mariposa.groupby('genero')['cantidad'].agg(['sum', 'mean', 'std']).round(2)
                
                col_metr1, col_metr2, col_metr3, col_metr4 = st.columns(4)
                
                with col_metr1:
                    if 'Masculino' in metricas_genero.index:
                        total_masc = int(metricas_genero.loc['Masculino', 'sum'])
                        promedio_masc = metricas_genero.loc['Masculino', 'mean']
                        st.metric(
                            label="👨 Total Masculino",
                            value=f"{total_masc:,}",
                            delta=f"Promedio: {promedio_masc:.1f}"
                        )
                    else:
                        st.metric(label="👨 Total Masculino", value="0")
                
                with col_metr2:
                    if 'Femenino' in metricas_genero.index:
                        total_fem = int(metricas_genero.loc['Femenino', 'sum'])
                        promedio_fem = metricas_genero.loc['Femenino', 'mean']
                        st.metric(
                            label="👩 Total Femenino",
                            value=f"{total_fem:,}",
                            delta=f"Promedio: {promedio_fem:.1f}"
                        )
                    else:
                        st.metric(label="👩 Total Femenino", value="0")
                
                with col_metr3:
                    if 'Masculino' in metricas_genero.index and 'Femenino' in metricas_genero.index:
                        total_masc = metricas_genero.loc['Masculino', 'sum']
                        total_fem = metricas_genero.loc['Femenino', 'sum']
                        diferencia = abs(total_fem - total_masc)
                        brecha_porcentual = (diferencia / max(total_masc, total_fem) * 100) if max(total_masc, total_fem) > 0 else 0
                        st.metric(
                            label="📊 Brecha de Género",
                            value=f"{diferencia:,.0f}",
                            delta=f"{brecha_porcentual:.1f}%"
                        )
                    else:
                        st.metric(label="📊 Brecha de Género", value="N/A")
                
                with col_metr4:
                    total_general = datos_mariposa['cantidad'].sum()
                    grupos_activos = datos_mariposa['edad_grupo'].nunique()
                    st.metric(
                        label="👥 Total General",
                        value=f"{total_general:,}",
                        delta=f"{grupos_activos} grupos etarios"
                    )
                
                # Análisis por grupo etario con mayor detalle
                st.markdown("#### 🎯 Análisis por Grupo Etario")
                
                # Tabla detallada por grupo etario
                tabla_grupos = datos_mariposa.pivot_table(
                    index='edad_grupo', 
                    columns='genero', 
                    values='cantidad', 
                    fill_value=0
                ).reset_index()
                
                # Agregar columnas calculadas
                if 'Masculino' in tabla_grupos.columns and 'Femenino' in tabla_grupos.columns:
                    tabla_grupos['Total'] = tabla_grupos['Masculino'] + tabla_grupos['Femenino']
                    tabla_grupos['% Femenino'] = (tabla_grupos['Femenino'] / tabla_grupos['Total'] * 100).round(1)
                    tabla_grupos['Diferencia'] = abs(tabla_grupos['Femenino'] - tabla_grupos['Masculino'])
                
                # Mostrar la tabla con formato
                st.dataframe(
                    tabla_grupos,
                    hide_index=True,
                    use_container_width=True,
                    column_config={
                        "edad_grupo": "Grupo de Edad",
                        "Masculino": st.column_config.NumberColumn("👨 Masculino", format="%d"),
                        "Femenino": st.column_config.NumberColumn("👩 Femenino", format="%d"),
                        "Total": st.column_config.NumberColumn("👥 Total", format="%d"),
                        "% Femenino": st.column_config.NumberColumn("% Femenino", format="%.1f%%"),
                        "Diferencia": st.column_config.NumberColumn("Diferencia", format="%d")
                    }
                )
                
                # Insights automáticos
                with st.expander("🔍 Insights Automáticos del Análisis de Género"):
                    insights = []
                    
                    if not tabla_grupos.empty and 'Total' in tabla_grupos.columns:
                        # Grupo con mayor población
                        grupo_mayor = tabla_grupos.loc[tabla_grupos['Total'].idxmax(), 'edad_grupo']
                        mayor_total = tabla_grupos['Total'].max()
                        insights.append(f"📊 **Grupo más numeroso**: {grupo_mayor} con {mayor_total:,} personas")
                        
                        # Grupo con mayor brecha de género
                        if 'Diferencia' in tabla_grupos.columns:
                            grupo_brecha = tabla_grupos.loc[tabla_grupos['Diferencia'].idxmax(), 'edad_grupo']
                            mayor_brecha = tabla_grupos['Diferencia'].max()
                            insights.append(f"⚖️ **Mayor brecha de género**: {grupo_brecha} con diferencia de {mayor_brecha:,}")
                        
                        # Paridad de género general
                        if 'Masculino' in tabla_grupos.columns and 'Femenino' in tabla_grupos.columns:
                            total_masc_global = tabla_grupos['Masculino'].sum()
                            total_fem_global = tabla_grupos['Femenino'].sum()
                            paridad_global = min(total_masc_global, total_fem_global) / max(total_masc_global, total_fem_global) * 100
                            
                            if paridad_global >= 95:
                                insights.append(f"✅ **Excelente paridad de género**: {paridad_global:.1f}% (muy equilibrado)")
                            elif paridad_global >= 85:
                                insights.append(f"✅ **Buena paridad de género**: {paridad_global:.1f}% (equilibrado)")
                            elif paridad_global >= 70:
                                insights.append(f"⚠️ **Paridad moderada**: {paridad_global:.1f}% (ligero desequilibrio)")
                            else:
                                insights.append(f"🔴 **Baja paridad de género**: {paridad_global:.1f}% (desequilibrio significativo)")
                        
                        # Grupo con mejor paridad
                        if '% Femenino' in tabla_grupos.columns:
                            tabla_grupos['paridad_score'] = 100 - abs(tabla_grupos['% Femenino'] - 50)
                            mejor_paridad_idx = tabla_grupos['paridad_score'].idxmax()
                            mejor_grupo = tabla_grupos.loc[mejor_paridad_idx, 'edad_grupo']
                            mejor_score = tabla_grupos.loc[mejor_paridad_idx, 'paridad_score']
                            insights.append(f"🎯 **Mejor equilibrio de género**: {mejor_grupo} (puntuación: {mejor_score:.1f}/100)")
                    
                    for insight in insights:
                        st.markdown(f"- {insight}")
                    
                    if not insights:
                        st.markdown("- 📋 No hay suficientes datos para generar insights automáticos")
        
        except Exception as e:
            st.error(f"Error al generar el gráfico de mariposa: {str(e)}")
            st.info("💡 **Sugerencia**: Verifique que los datos contengan las columnas 'sexo' y 'grupo_etario' o 'edad' para generar el análisis demográfico.")
    
    # Sección de resumen
    st.markdown("---")
    col1, col2 = st.columns([2, 1])
    
    with col1:
        # Gráfico de progreso de vacunación
        st.write("### Progreso de Vacunación")
        
        if not df_filtrado.empty:
            # Si se seleccionó un mes específico, mostrar progreso diario
            if mes_seleccionado != "Todos":
                # Progreso diario del mes seleccionado
                vacunas_por_dia = df_filtrado.groupby('dia_aplicacion').size().sort_index()
                
                fig = go.Figure()
                fig.add_trace(go.Bar(
                    x=vacunas_por_dia.index,
                    y=vacunas_por_dia.values,
                    name='Vacunas por Día',
                    marker_color='lightblue'
                ))
                
                # Obtener nombre del mes de forma segura
                try:
                    nombre_mes = mes_seleccionado.split(' - ')[1]
                except IndexError:
                    nombre_mes = mes_seleccionado
                
                # Título para el gráfico diario
                titulo_años = años_texto if len(años_seleccionados) <= 2 else f"Años seleccionados"
                
                fig.update_layout(
                    title=f"Vacunas Aplicadas por Día - {nombre_mes} {titulo_años}",
                    xaxis_title="Día del Mes",
                    yaxis_title="Número de Vacunas",
                    height=400
                )
                
            else:
                # Progreso mensual de los años seleccionados
                meses_nombres = {
                    1: "Ene", 2: "Feb", 3: "Mar", 4: "Abr",
                    5: "May", 6: "Jun", 7: "Jul", 8: "Ago",
                    9: "Sep", 10: "Oct", 11: "Nov", 12: "Dic"
                }
                
                fig = go.Figure()
                
                if len(años_seleccionados) == 1:
                    # Un solo año: gráfico simple
                    vacunas_por_mes = df_filtrado.groupby('mes_aplicacion').size().sort_index()
                    meses_labels = [meses_nombres.get(mes, str(mes)) for mes in vacunas_por_mes.index]
                    
                    # Meta estimada (puedes ajustar este valor)
                    meta_mensual = vacunas_por_mes.mean() * 1.2 if len(vacunas_por_mes) > 0 else 60000
                    meta = [meta_mensual] * len(vacunas_por_mes)
                    
                    fig.add_trace(go.Bar(
                        x=meses_labels,
                        y=vacunas_por_mes.values,
                        name='Vacunas Aplicadas',
                        marker_color='lightblue'
                    ))
                    fig.add_trace(go.Scatter(
                        x=meses_labels,
                        y=meta,
                        mode='lines+markers',
                        name='Meta',
                        line=dict(color='red', dash='dash')
                    ))
                else:
                    # Múltiples años: comparación por año
                    colores = ['lightblue', 'lightgreen', 'lightcoral', 'lightyellow', 'lightpink']
                    
                    for i, año in enumerate(años_seleccionados):
                        df_año = df_filtrado[df_filtrado['anio_aplicacion'] == año]
                        vacunas_por_mes = df_año.groupby('mes_aplicacion').size().sort_index()
                        meses_labels = [meses_nombres.get(mes, str(mes)) for mes in vacunas_por_mes.index]
                        
                        fig.add_trace(go.Bar(
                            x=meses_labels,
                            y=vacunas_por_mes.values,
                            name=f'Año {año}',
                            marker_color=colores[i % len(colores)],
                            opacity=0.8
                        ))
                
                # Título para el gráfico mensual
                titulo_años = años_texto if len(años_seleccionados) <= 3 else f"Múltiples años"
                
                fig.update_layout(
                    title=f"Vacunas Aplicadas vs Meta - {titulo_años}",
                    xaxis_title="Mes",
                    yaxis_title="Número de Vacunas",
                    height=400
                )
        else:
            # Gráfico de ejemplo cuando no hay datos
            fig = go.Figure()
            fig.add_annotation(
                text="No hay datos disponibles para mostrar",
                xref="paper", yref="paper",
                x=0.5, y=0.5, xanchor='center', yanchor='middle',
                showarrow=False, font=dict(size=16)
            )
            fig.update_layout(
                title="Progreso de Vacunación",
                height=400,
                xaxis=dict(visible=False),
                yaxis=dict(visible=False)
            )
        
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.write("### Indicadores Clave")
        
        # Indicadores basados en datos filtrados
        if not df_filtrado.empty:
            total_registros = len(df_filtrado)
            establecimientos_activos = df_filtrado['unicodigo'].nunique()
            
            st.info(f"**Registros en período:** {total_registros:,}")
            st.success(f"**Establecimientos activos:** {establecimientos_activos}")
            
            # Calcular estadísticas adicionales
            if 'dosis_aplicada' in df_filtrado.columns:
                dosis_info = df_filtrado['dosis_aplicada'].value_counts()
                st.info(f"**Dosis más aplicada:** {dosis_info.index[0] if len(dosis_info) > 0 else 'N/A'}")
            
            if 'sexo' in df_filtrado.columns:
                distribucion_sexo = df_filtrado['sexo'].value_counts()
                if len(distribucion_sexo) > 0:
                    porcentaje_f = (distribucion_sexo.get('F', 0) / len(df_filtrado) * 100)
                    st.info(f"**Distribución F/M:** {porcentaje_f:.1f}% / {100-porcentaje_f:.1f}%")
        else:
            st.warning("No hay datos para el período seleccionado")
