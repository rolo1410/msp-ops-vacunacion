from datetime import datetime, timedelta

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from data.source import QUERY_VACUNAS_TEMPORAL_FULL, get_duck_db_data


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


def show_general():
    """
    Página principal con información general del sistema de vacunación
    """
    df = get_duck_db_data(QUERY_VACUNAS_TEMPORAL_FULL)
    
    st.header("Vista General del Sistema")
    
    # Sección de filtros
    st.markdown("### Filtros")
    col_filtro1, col_filtro2, col_filtro3, col_filtro4, col_filtro5, col_filtro6,col_filtro7, col_filtro8 = st.columns([2, 1, 1, 1, 1, 1, 1, 1])
    
    with col_filtro1:
        # Filtro por año (multiselect)
        años_disponibles = sorted(df['anio_aplicacion'].unique()) if not df.empty else [2024]
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
        # Filtro por mes
        meses_nombres = {
            1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril",
            5: "Mayo", 6: "Junio", 7: "Julio", 8: "Agosto",
            9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"
        }
        
        # Obtener meses disponibles para los años seleccionados
        if años_seleccionados and not df.empty:
            df_años = df[df['anio_aplicacion'].isin(años_seleccionados)]
            meses_disponibles = sorted(df_años['mes_aplicacion'].unique()) if not df_años.empty else [1]
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
        
        # Mostrar en columnas
        col_zona1, col_zona2 = st.columns([2, 1])
        
        with col_zona1:
            # Tabla de estadísticas por zona
            st.dataframe(
                stats_zona.sort_values('Total Vacunas', ascending=False),
                hide_index=True,
                use_container_width=True)
        
        with col_zona2:
            # Gráfico de pastel de distribución por zona
            fig_pie = px.pie(
                stats_zona,
                values='Total Vacunas',
                names='Zona',
                title='Distribución de Vacunas por Zona'
            )
            fig_pie.update_layout(height=300)
            st.plotly_chart(fig_pie, use_container_width=True)
    
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
        
        st.write("### Estado del Sistema")
        st.success("**Sistema:** Operativo")
        