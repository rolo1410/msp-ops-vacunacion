from datetime import datetime, timedelta

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st


def show_general():
    """
    Página principal con información general del sistema de vacunación
    """
    st.header("🏠 Vista General del Sistema")
    
    # Métricas principales
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="Total Vacunas Aplicadas",
            value="1,245,670",
            delta="12,340"
        )
    
    with col2:
        st.metric(
            label="Centros de Vacunación",
            value="156",
            delta="2"
        )
    
    with col3:
        st.metric(
            label="Cobertura Nacional",
            value="78.5%",
            delta="2.1%"
        )
    
    with col4:
        st.metric(
            label="Meta Mensual",
            value="95.2%",
            delta="-4.8%"
        )
    
    # Sección de resumen
    st.markdown("---")
    st.subheader("📊 Resumen Ejecutivo")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        # Gráfico de ejemplo
        st.write("### Progreso de Vacunación por Mes")
        
        # Datos de ejemplo
        meses = ['Ene', 'Feb', 'Mar', 'Abr', 'May', 'Jun']
        vacunas = [45000, 52000, 48000, 65000, 71000, 68000]
        meta = [60000, 60000, 60000, 60000, 60000, 60000]
        
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=meses, 
            y=vacunas, 
            name='Vacunas Aplicadas',
            marker_color='lightblue'
        ))
        fig.add_trace(go.Scatter(
            x=meses, 
            y=meta, 
            mode='lines+markers',
            name='Meta',
            line=dict(color='red', dash='dash')
        ))
        
        fig.update_layout(
            title="Vacunas Aplicadas vs Meta Mensual",
            xaxis_title="Mes",
            yaxis_title="Número de Vacunas",
            height=400
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.write("### Indicadores Clave")
        
        # Indicadores
        st.info("**Estado del Sistema:** ✅ Operativo")
        st.success("**Última Actualización:** Hoy, 14:30")
        st.warning("**Alertas Pendientes:** 3 centros requieren atención")
        
        # Acciones rápidas
        st.write("### Acciones Rápidas")
        if st.button("📋 Generar Reporte"):
            st.success("Reporte generado exitosamente")
        
        if st.button("📧 Enviar Notificaciones"):
            st.success("Notificaciones enviadas")
        
        if st.button("🔄 Actualizar Datos"):
            st.success("Datos actualizados")
    
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