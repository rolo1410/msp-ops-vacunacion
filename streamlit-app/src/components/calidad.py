import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st


def show_calidad():
    """
    Página de control de calidad del sistema de vacunación
    """
    st.header("✅ Control de Calidad")
    
    # Tabs para organizar la información de calidad
    tab1, tab2, tab3, tab4 = st.tabs([
        "📊 Métricas de Calidad", 
        "🔍 Validación de Datos", 
        "⚠️ Alertas de Calidad",
        "📋 Reportes"
    ])
    
    with tab1:
        st.subheader("Indicadores de Calidad")
        
        # Métricas de calidad
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric(
                label="Completitud de Datos",
                value="94.2%",
                delta="1.5%"
            )
        
        with col2:
            st.metric(
                label="Precisión de Registros",
                value="97.8%",
                delta="-0.3%"
            )
        
        with col3:
            st.metric(
                label="Registros Duplicados",
                value="0.12%",
                delta="-0.05%"
            )
        
        # Gráfico de evolución de calidad
        st.write("### Evolución de Indicadores de Calidad")
        
        fechas = pd.date_range(start='2024-01-01', end='2024-06-30', freq='ME')
        completitud = np.random.normal(94, 2, len(fechas))
        precision = np.random.normal(97, 1, len(fechas))
        
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=fechas,
            y=completitud,
            mode='lines+markers',
            name='Completitud (%)',
            line=dict(color='blue')
        ))
        fig.add_trace(go.Scatter(
            x=fechas,
            y=precision,
            mode='lines+markers',
            name='Precisión (%)',
            line=dict(color='green')
        ))
        
        fig.update_layout(
            title="Tendencia de Calidad de Datos",
            xaxis_title="Fecha",
            yaxis_title="Porcentaje (%)",
            height=400
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    with tab2:
        st.subheader("Validación de Datos")
        
        col1, col2 = st.columns([1, 1])
        
        with col1:
            st.write("#### Reglas de Validación")
            
            reglas = [
                {"regla": "Formato de cédula", "estado": "✅", "cumplimiento": "99.8%"},
                {"regla": "Fechas válidas", "estado": "✅", "cumplimiento": "98.5%"},
                {"regla": "Códigos de vacuna", "estado": "⚠️", "cumplimiento": "94.2%"},
                {"regla": "Datos demográficos", "estado": "✅", "cumplimiento": "97.1%"},
                {"regla": "Centro de vacunación", "estado": "❌", "cumplimiento": "89.3%"}
            ]
            
            for regla in reglas:
                col_regla, col_estado, col_cumpl = st.columns([2, 1, 1])
                with col_regla:
                    st.write(regla["regla"])
                with col_estado:
                    st.write(regla["estado"])
                with col_cumpl:
                    st.write(regla["cumplimiento"])
        
        with col2:
            st.write("#### Errores Detectados")
            
            errores = [
                "125 registros con códigos de vacuna inválidos",
                "89 registros con centros de vacunación no registrados",
                "34 registros con fechas futuras",
                "12 registros duplicados detectados"
            ]
            
            for error in errores:
                st.error(f"🔴 {error}")
            
            if st.button("🔧 Ejecutar Corrección Automática"):
                st.success("Correcciones aplicadas exitosamente")
    
    with tab3:
        st.subheader("Sistema de Alertas de Calidad")
        
        # Filtros
        col1, col2, col3 = st.columns(3)
        with col1:
            severidad = st.selectbox("Severidad", ["Todas", "Alta", "Media", "Baja"])
        with col2:
            tipo = st.selectbox("Tipo", ["Todos", "Datos", "Sistema", "Proceso"])
        with col3:
            estado = st.selectbox("Estado", ["Todas", "Activa", "Resuelta", "En Proceso"])
        
        # Lista de alertas
        alertas = [
            {
                "id": "ALT-001",
                "tipo": "Datos",
                "severidad": "Alta",
                "descripcion": "Incremento significativo en registros duplicados",
                "fecha": "2024-10-15 09:30",
                "estado": "Activa"
            },
            {
                "id": "ALT-002", 
                "tipo": "Sistema",
                "severidad": "Media",
                "descripcion": "Tiempo de respuesta elevado en validaciones",
                "fecha": "2024-10-15 08:15",
                "estado": "En Proceso"
            },
            {
                "id": "ALT-003",
                "tipo": "Proceso",
                "severidad": "Baja",
                "descripcion": "Centro Norte con baja tasa de actualización",
                "fecha": "2024-10-14 16:45",
                "estado": "Resuelta"
            }
        ]
        
        for alerta in alertas:
            with st.expander(f"{alerta['id']} - {alerta['descripcion']}"):
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.write(f"**Tipo:** {alerta['tipo']}")
                with col2:
                    st.write(f"**Severidad:** {alerta['severidad']}")
                with col3:
                    st.write(f"**Fecha:** {alerta['fecha']}")
                with col4:
                    st.write(f"**Estado:** {alerta['estado']}")
                
                if alerta['estado'] == 'Activa':
                    if st.button(f"Resolver {alerta['id']}", key=f"resolver_{alerta['id']}"):
                        st.success(f"Alerta {alerta['id']} marcada como resuelta")
    
    with tab4:
        st.subheader("Generador de Reportes de Calidad")
        
        col1, col2 = st.columns([1, 1])
        
        with col1:
            st.write("#### Configuración del Reporte")
            
            tipo_reporte = st.selectbox(
                "Tipo de Reporte",
                ["Reporte Completo", "Métricas de Calidad", "Errores Detectados", "Tendencias"]
            )
            
            fecha_inicio = st.date_input("Fecha de Inicio")
            fecha_fin = st.date_input("Fecha de Fin")
            
            incluir_graficos = st.checkbox("Incluir Gráficos", value=True)
            incluir_detalles = st.checkbox("Incluir Detalles Técnicos", value=False)
            
            formato = st.radio("Formato de Salida", ["PDF", "Excel", "CSV"])
        
        with col2:
            st.write("#### Vista Previa")
            st.info("El reporte incluirá:")
            st.write("- Métricas de calidad del período seleccionado")
            st.write("- Lista de errores detectados y corregidos")
            st.write("- Análisis de tendencias")
            st.write("- Recomendaciones de mejora")
            
            if incluir_graficos:
                st.write("- Gráficos y visualizaciones")
            
            if incluir_detalles:
                st.write("- Detalles técnicos de validaciones")
        
        if st.button("📄 Generar Reporte"):
            with st.spinner("Generando reporte..."):
                import time
                time.sleep(2)
            st.success(f"Reporte generado exitosamente en formato {formato}")
            st.download_button(
                label=f"📥 Descargar Reporte ({formato})",
                data="Contenido del reporte...",
                file_name=f"reporte_calidad_{fecha_inicio}_{fecha_fin}.{formato.lower()}",
                mime="application/octet-stream"
            )