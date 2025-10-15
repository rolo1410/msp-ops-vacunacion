from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st


def show_analisis_temporal():
    """
    Página de análisis temporal del sistema de vacunación
    """
    st.header("📈 Análisis Temporal")
    
    # Controles de filtrado temporal
    col1, col2, col3 = st.columns(3)
    
    with col1:
        periodo = st.selectbox(
            "Período de Análisis",
            ["Último Mes", "Últimos 3 Meses", "Últimos 6 Meses", "Último Año", "Personalizado"]
        )
    
    with col2:
        granularidad = st.selectbox(
            "Granularidad",
            ["Diaria", "Semanal", "Mensual", "Trimestral"]
        )
    
    with col3:
        comparacion = st.selectbox(
            "Comparar con",
            ["Período Anterior", "Mismo Período Año Anterior", "Meta/Objetivo", "Sin Comparación"]
        )
    
    # Fechas personalizadas si se selecciona
    if periodo == "Personalizado":
        col1, col2 = st.columns(2)
        with col1:
            fecha_inicio = st.date_input("Fecha de Inicio")
        with col2:
            fecha_fin = st.date_input("Fecha de Fin")
    
    # Tabs para diferentes tipos de análisis
    tab1, tab2, tab3, tab4 = st.tabs([
        "📊 Tendencias Generales",
        "🗓️ Patrones Temporales",
        "📈 Pronósticos",
        "📋 Análisis Detallado"
    ])
    
    with tab1:
        st.subheader("Tendencias de Vacunación")
        
        # Generar datos de ejemplo
        fechas = pd.date_range(start='2024-01-01', end='2024-10-15', freq='D')
        vacunas_diarias = np.random.poisson(2000, len(fechas)) + np.sin(np.arange(len(fechas)) * 2 * np.pi / 7) * 200
        
        # Gráfico principal de tendencias
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=fechas,
            y=vacunas_diarias,
            mode='lines',
            name='Vacunas Diarias',
            line=dict(color='blue', width=2)
        ))
        
        # Línea de tendencia
        z = np.polyfit(range(len(vacunas_diarias)), vacunas_diarias, 1)
        p = np.poly1d(z)
        fig.add_trace(go.Scatter(
            x=fechas,
            y=p(range(len(vacunas_diarias))),
            mode='lines',
            name='Tendencia',
            line=dict(color='red', dash='dash')
        ))
        
        fig.update_layout(
            title="Evolución Temporal de Vacunaciones",
            xaxis_title="Fecha",
            yaxis_title="Número de Vacunas",
            height=500
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Métricas de tendencia
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Promedio Diario", "2,045", "+156")
        with col2:
            st.metric("Máximo Diario", "3,421", "+89")
        with col3:
            st.metric("Crecimiento Semanal", "+5.2%", "+1.1%")
        with col4:
            st.metric("Variabilidad", "12.3%", "-2.1%")
    
    with tab2:
        st.subheader("Patrones y Estacionalidad")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("#### Patrón Semanal")
            
            # Datos de ejemplo para patrón semanal
            dias_semana = ['Lun', 'Mar', 'Mié', 'Jue', 'Vie', 'Sáb', 'Dom']
            promedio_semanal = [2400, 2300, 2100, 2200, 2500, 1800, 1200]
            
            fig_semanal = px.bar(
                x=dias_semana,
                y=promedio_semanal,
                title="Promedio de Vacunas por Día de la Semana",
                color=promedio_semanal,
                color_continuous_scale='Blues'
            )
            
            st.plotly_chart(fig_semanal, use_container_width=True)
        
        with col2:
            st.write("#### Patrón Mensual")
            
            # Datos de ejemplo para patrón mensual
            meses = ['Ene', 'Feb', 'Mar', 'Abr', 'May', 'Jun', 'Jul', 'Ago', 'Sep', 'Oct']
            vacunas_mes = [65000, 58000, 72000, 68000, 75000, 82000, 79000, 85000, 88000, 42000]
            
            fig_mensual = px.line(
                x=meses,
                y=vacunas_mes,
                title="Vacunas Aplicadas por Mes",
                markers=True
            )
            
            st.plotly_chart(fig_mensual, use_container_width=True)
        
        # Análisis de horas pico
        st.write("#### Distribución por Horas del Día")
        
        horas = list(range(8, 18))  # Horario de atención 8 AM a 6 PM
        vacunas_hora = [120, 180, 220, 280, 320, 350, 300, 280, 250, 180]
        
        fig_horas = px.area(
            x=horas,
            y=vacunas_hora,
            title="Distribución de Vacunas por Hora del Día",
        )
        
        st.plotly_chart(fig_horas, use_container_width=True)
    
    with tab3:
        st.subheader("Pronósticos y Proyecciones")
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            # Configuración del pronóstico
            st.write("#### Configuración del Pronóstico")
            
            horizonte = st.slider("Horizonte de Pronóstico (días)", 7, 90, 30)
            modelo = st.selectbox("Modelo de Pronóstico", 
                                ["Tendencia Linear", "Media Móvil", "Suavizado Exponencial", "ARIMA"])
            
            # Generar pronóstico (simulado)
            fechas_futuras = pd.date_range(start='2024-10-16', periods=horizonte, freq='D')
            
            # Datos históricos simulados
            fechas_hist = pd.date_range(start='2024-09-01', end='2024-10-15', freq='D')
            vacunas_hist = np.random.poisson(2000, len(fechas_hist))
            
            # Pronóstico simulado
            pronostico = np.random.normal(2000, 200, horizonte)
            limite_superior = pronostico + 300
            limite_inferior = pronostico - 300
            
            # Gráfico de pronóstico
            fig_pronostico = go.Figure()
            
            # Datos históricos
            fig_pronostico.add_trace(go.Scatter(
                x=fechas_hist,
                y=vacunas_hist,
                mode='lines',
                name='Datos Históricos',
                line=dict(color='blue')
            ))
            
            # Pronóstico
            fig_pronostico.add_trace(go.Scatter(
                x=fechas_futuras,
                y=pronostico,
                mode='lines',
                name='Pronóstico',
                line=dict(color='red', dash='dash')
            ))
            
            # Bandas de confianza
            fig_pronostico.add_trace(go.Scatter(
                x=list(fechas_futuras) + list(fechas_futuras[::-1]),
                y=list(limite_superior) + list(limite_inferior[::-1]),
                fill='toself',
                fillcolor='rgba(255,0,0,0.2)',
                line=dict(color='rgba(255,255,255,0)'),
                name='Intervalo de Confianza',
                showlegend=True
            ))
            
            fig_pronostico.update_layout(
                title=f"Pronóstico de Vacunaciones - {horizonte} días",
                xaxis_title="Fecha",
                yaxis_title="Número de Vacunas",
                height=400
            )
            
            st.plotly_chart(fig_pronostico, use_container_width=True)
        
        with col2:
            st.write("#### Métricas del Pronóstico")
            
            precision = np.random.uniform(85, 95)
            error_promedio = np.random.uniform(150, 250)
            
            st.metric("Precisión del Modelo", f"{precision:.1f}%")
            st.metric("Error Promedio", f"{error_promedio:.0f}")
            st.metric("Tendencia Proyectada", "+2.3%")
            
            st.write("#### Escenarios")
            escenario = st.radio("Seleccionar Escenario", 
                               ["Optimista", "Base", "Pesimista"])
            
            if escenario == "Optimista":
                st.success("📈 Crecimiento del 15% proyectado")
            elif escenario == "Base":
                st.info("📊 Mantiene tendencia actual")
            else:
                st.warning("📉 Posible reducción del 8%")
    
    with tab4:
        st.subheader("Análisis Detallado por Segmentos")
        
        # Filtros adicionales
        col1, col2, col3 = st.columns(3)
        
        with col1:
            region = st.multiselect("Región", 
                                  ["Norte", "Sur", "Este", "Oeste", "Centro"], 
                                  default=["Norte", "Sur"])
        
        with col2:
            tipo_vacuna = st.multiselect("Tipo de Vacuna",
                                       ["COVID-19", "Influenza", "Hepatitis B", "MMR"],
                                       default=["COVID-19"])
        
        with col3:
            grupo_edad = st.multiselect("Grupo de Edad",
                                      ["0-17", "18-39", "40-59", "60+"],
                                      default=["18-39", "60+"])
        
        # Análisis comparativo
        st.write("#### Análisis Comparativo por Regiones")
        
        # Datos simulados por región
        regiones_data = {
            'Región': ['Norte', 'Sur', 'Este', 'Oeste', 'Centro'],
            'Vacunas_Octubre': [15420, 18750, 12300, 16890, 21340],
            'Vacunas_Septiembre': [14200, 17800, 11950, 15670, 19800],
            'Crecimiento_%': [8.6, 5.3, 2.9, 7.8, 7.8]
        }
        
        df_regiones = pd.DataFrame(regiones_data)
        
        fig_comparativo = px.bar(
            df_regiones,
            x='Región',
            y=['Vacunas_Octubre', 'Vacunas_Septiembre'],
            title="Comparación Mensual por Región",
            barmode='group'
        )
        
        st.plotly_chart(fig_comparativo, use_container_width=True)
        
        # Tabla de estadísticas detalladas
        st.write("#### Estadísticas Detalladas")
        st.dataframe(df_regiones, use_container_width=True)
        
        # Insights automáticos
        st.write("#### Insights Automáticos")
        st.info("🔍 **Hallazgo Principal:** La región Sur mantiene el mayor volumen de vacunación")
        st.success("📈 **Tendencia Positiva:** Todas las regiones muestran crecimiento vs mes anterior")
        st.warning("⚠️ **Atención:** La región Este muestra el menor crecimiento (2.9%)")