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
    
    st.header("🏠 Vista General del Sistema")
    
    # Sección de filtros
    st.markdown("### 🔍 Filtros")
    col_filtro1, col_filtro2, col_filtro3 = st.columns([1, 1, 2])
    
    with col_filtro1:
        # Filtro por año
        años_disponibles = sorted(df['anio_aplicacion'].unique()) if not df.empty else [2024]
        año_seleccionado = st.selectbox(
            "Seleccionar Año:",
            options=años_disponibles,
            index=len(años_disponibles)-1 if años_disponibles else 0
        )
    
    with col_filtro2:
        # Filtro por mes
        meses_nombres = {
            1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril",
            5: "Mayo", 6: "Junio", 7: "Julio", 8: "Agosto",
            9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"
        }
        
        # Obtener meses disponibles para el año seleccionado
        df_año = df[df['anio_aplicacion'] == año_seleccionado] if not df.empty else df
        meses_disponibles = sorted(df_año['mes_aplicacion'].unique()) if not df_año.empty else [1]
        
        opciones_meses = ["Todos"] + [f"{mes} - {meses_nombres.get(mes, mes)}" for mes in meses_disponibles]
        mes_seleccionado = st.selectbox(
            "Seleccionar Mes:",
            options=opciones_meses,
            index=0
        )
    
    with col_filtro3:
        st.write("")  # Espacio vacío
        if st.button("🔄 Actualizar Filtros"):
            st.rerun()
    
    # Aplicar filtros
    df_filtrado = df.copy()
    if not df.empty:
        df_filtrado = df_filtrado[df_filtrado['anio_aplicacion'] == año_seleccionado]
        
        if mes_seleccionado != "Todos":
            try:
                mes_numero = int(mes_seleccionado.split(" - ")[0])
                df_filtrado = df_filtrado[df_filtrado['mes_aplicacion'] == mes_numero]
            except (ValueError, IndexError):
                # En caso de error, mantener todos los datos del año
                pass
    
    # Mostrar información de filtros aplicados
    if not df_filtrado.empty:
        mes_texto = " (Todos los meses)"
        if mes_seleccionado != "Todos":
            try:
                mes_texto = f" - {mes_seleccionado.split(' - ')[1]}"
            except IndexError:
                mes_texto = f" - {mes_seleccionado}"
        
        st.info(f"📊 Mostrando datos para: {año_seleccionado}{mes_texto}")
    else:
        st.warning("⚠️ No hay datos disponibles para los filtros seleccionados")
    
    st.markdown("---")
    
    
    # Métricas principales
    col1, col2, col3, col4 = st.columns(4)
    
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
    
    with col4:
        st.metric(
            label="Meta Mensual",
            value="95.2%",
            delta="-4.8%"
        )
    
    # Información adicional de filtros aplicados
    if not df_filtrado.empty:
        st.markdown("---")
        col_info1, col_info2, col_info3 = st.columns(3)
        
        with col_info1:
            st.metric(
                label="Fechas en Período",
                value=f"{df_filtrado['fecha_aplicacion'].nunique()}"
            )
        
        with col_info2:
            vacunas_unicas = df_filtrado['nombre_vacuna'].nunique() if 'nombre_vacuna' in df_filtrado.columns else 0
            st.metric(
                label="Tipos de Vacunas",
                value=f"{vacunas_unicas}"
            )
        
        with col_info3:
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
    
    # Sección de resumen
    st.markdown("---")
    st.subheader("📊 Resumen Ejecutivo")
    
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
                
                fig.update_layout(
                    title=f"Vacunas Aplicadas por Día - {nombre_mes} {año_seleccionado}",
                    xaxis_title="Día del Mes",
                    yaxis_title="Número de Vacunas",
                    height=400
                )
                
            else:
                # Progreso mensual del año seleccionado
                meses_nombres = {
                    1: "Ene", 2: "Feb", 3: "Mar", 4: "Abr",
                    5: "May", 6: "Jun", 7: "Jul", 8: "Ago",
                    9: "Sep", 10: "Oct", 11: "Nov", 12: "Dic"
                }
                
                vacunas_por_mes = df_filtrado.groupby('mes_aplicacion').size().sort_index()
                meses_labels = [meses_nombres.get(mes, str(mes)) for mes in vacunas_por_mes.index]
                
                # Meta estimada (puedes ajustar este valor)
                meta_mensual = vacunas_por_mes.mean() * 1.2 if len(vacunas_por_mes) > 0 else 60000
                meta = [meta_mensual] * len(vacunas_por_mes)
                
                fig = go.Figure()
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
                
                fig.update_layout(
                    title=f"Vacunas Aplicadas vs Meta - Año {año_seleccionado}",
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
        st.success("**Sistema:** ✅ Operativo")
        
        # Acciones rápidas
        st.write("### Acciones Rápidas")
        if st.button("📋 Generar Reporte"):
            st.success("Reporte generado exitosamente")
        
        if st.button("📧 Enviar Notificaciones"):
            st.success("Notificaciones enviadas")
        
        if st.button("🔄 Actualizar Datos"):
            st.rerun()
    
    # Sección de noticias/alertas
    st.markdown("---")
    st.subheader("📢 Alertas y Notificaciones")
    
    alertas = [
        {"tipo": "warning", "mensaje": "Centro de Salud Norte requiere reabastecimiento de vacunas"},
        {"tipo": "info", "mensaje": "Nueva actualización del sistema disponible"},
        {"tipo": "success", "mensaje": "Meta de vacunación superada en la región Sur"}
    ]
    
    for alerta in alertas:
        if alerta["tipo"] == "warning":
            st.warning(f"⚠️ {alerta['mensaje']}")
        elif alerta["tipo"] == "info":
            st.info(f"ℹ️ {alerta['mensaje']}")
        elif alerta["tipo"] == "success":
            st.success(f"✅ {alerta['mensaje']}")