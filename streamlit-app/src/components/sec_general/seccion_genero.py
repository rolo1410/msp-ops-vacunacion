import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import numpy as np

def crear_grafico_mariposa_genero(df):
    """
    Crea un gráfico de mariposa (butterfly plot) para comparar géneros por grupo de edad.
    
    Args:
        df: DataFrame con columnas 'genero', 'edad_grupo' y 'cantidad'
    
    Returns:
        Figura de Plotly con gráfico de mariposa
    """
    
    # Datos de ejemplo si no se proporciona DataFrame
    if df is None or df.empty:
        # Crear datos de ejemplo para vacunación por género y grupo de edad
        grupos_edad = ['0-5', '6-11', '12-17', '18-29', '30-39', '40-49', '50-59', '60-69', '70+']
        data_ejemplo = []
        
        for grupo in grupos_edad:
            # Generar datos sintéticos realistas
            base_masculino = np.random.randint(800, 1500)
            base_femenino = np.random.randint(850, 1600)
            
            data_ejemplo.append({'genero': 'Masculino', 'edad_grupo': grupo, 'cantidad': base_masculino})
            data_ejemplo.append({'genero': 'Femenino', 'edad_grupo': grupo, 'cantidad': base_femenino})
        
        df = pd.DataFrame(data_ejemplo)
    
    # Preparar datos para el gráfico de mariposa
    df_pivot = df.pivot_table(index='edad_grupo', columns='genero', values='cantidad', fill_value=0)
    
    # Ordenar grupos de edad de forma lógica
    orden_edad = ['0-5', '6-11', '12-17', '18-29', '30-39', '40-49', '50-59', '60-69', '70+']
    df_pivot = df_pivot.reindex([grupo for grupo in orden_edad if grupo in df_pivot.index])
    
    # Crear el gráfico de mariposa
    fig = go.Figure()
    
    # Datos masculinos (lado izquierdo, valores negativos)
    if 'Masculino' in df_pivot.columns:
        masculino_valores = -df_pivot['Masculino']  # Valores negativos para el lado izquierdo
        fig.add_trace(go.Bar(
            y=df_pivot.index,
            x=masculino_valores,
            name='Masculino',
            orientation='h',
            marker=dict(color='#3498db'),
            text=[f"{abs(val):,}" for val in masculino_valores],
            textposition='outside',
            hovertemplate='<b>%{fullData.name}</b><br>' +
                         'Grupo de edad: %{y}<br>' +
                         'Cantidad: %{text}<br>' +
                         '<extra></extra>'
        ))
    
    # Datos femeninos (lado derecho, valores positivos)
    if 'Femenino' in df_pivot.columns:
        femenino_valores = df_pivot['Femenino']
        fig.add_trace(go.Bar(
            y=df_pivot.index,
            x=femenino_valores,
            name='Femenino',
            orientation='h',
            marker=dict(color='#e74c3c'),
            text=[f"{val:,}" for val in femenino_valores],
            textposition='outside',
            hovertemplate='<b>%{fullData.name}</b><br>' +
                         'Grupo de edad: %{y}<br>' +
                         'Cantidad: %{text}<br>' +
                         '<extra></extra>'
        ))
    
    # Personalizar el layout
    fig.update_layout(
        title={
            'text': 'Distribución por Género y Grupo de Edad<br><sub>Gráfico de Mariposa</sub>',
            'x': 0.5,
            'xanchor': 'center',
            'font': {'size': 18}
        },
        xaxis=dict(
            title='Cantidad de Personas',
            showgrid=True,
            gridcolor='lightgray',
            zeroline=True,
            zerolinecolor='black',
            zerolinewidth=2,
            tickformat=',d'
        ),
        yaxis=dict(
            title='Grupo de Edad',
            categoryorder='array',
            categoryarray=orden_edad
        ),
        barmode='relative',
        bargap=0.1,
        height=600,
        width=800,
        showlegend=True,
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
    
    # Agregar línea central
    fig.add_vline(x=0, line_width=2, line_dash="solid", line_color="black")
    
    # Personalizar los ticks del eje x para mostrar valores absolutos
    if 'Masculino' in df_pivot.columns and 'Femenino' in df_pivot.columns:
        max_val = max(df_pivot['Masculino'].max(), df_pivot['Femenino'].max())
        tick_vals = [-max_val, -max_val//2, 0, max_val//2, max_val]
        tick_text = [f"{abs(val):,}" for val in tick_vals]
        
        fig.update_xaxes(
            tickvals=tick_vals,
            ticktext=tick_text
        )
    
    return fig

def crear_grafico_mariposa_avanzado(df):
    """
    Versión avanzada del gráfico de mariposa con más opciones de personalización.
    
    Args:
        df: DataFrame con datos de género
    
    Returns:
        Figura de Plotly con gráfico de mariposa avanzado
    """
    
    # Datos de ejemplo más detallados
    if df is None or df.empty:
        grupos_edad = ['0-11 meses', '1-4 años', '5-11 años', '12-17 años', 
                      '18-29 años', '30-39 años', '40-49 años', '50-59 años', 
                      '60-69 años', '70-79 años', '80+ años']
        
        data_ejemplo = []
        np.random.seed(42)  # Para reproducibilidad
        
        for i, grupo in enumerate(grupos_edad):
            # Patrones más realistas por grupo de edad
            factor_edad = [0.3, 0.5, 0.7, 0.8, 1.0, 0.9, 0.8, 0.7, 0.6, 0.4, 0.2][i]
            base = 1000 * factor_edad
            
            masculino = int(base * np.random.uniform(0.8, 1.2))
            femenino = int(base * np.random.uniform(0.85, 1.25))
            
            data_ejemplo.append({'genero': 'Masculino', 'edad_grupo': grupo, 'cantidad': masculino})
            data_ejemplo.append({'genero': 'Femenino', 'edad_grupo': grupo, 'cantidad': femenino})
        
        df = pd.DataFrame(data_ejemplo)
    
    # Crear subplots para mejor control
    fig = make_subplots(
        rows=1, cols=1,
        subplot_titles=['Distribución por Género y Edad']
    )
    
    # Preparar datos
    df_pivot = df.pivot_table(index='edad_grupo', columns='genero', values='cantidad', fill_value=0)
    
    # Colores personalizados
    colores = {
        'Masculino': '#2E86AB',
        'Femenino': '#A23B72'
    }
    
    # Agregar barras para masculino (izquierda)
    if 'Masculino' in df_pivot.columns:
        fig.add_trace(go.Bar(
            y=df_pivot.index,
            x=-df_pivot['Masculino'],
            name='Masculino',
            orientation='h',
            marker=dict(
                color=colores['Masculino'],
                line=dict(color='white', width=1)
            ),
            text=[f"{val:,}" for val in df_pivot['Masculino']],
            textposition='outside',
            textfont=dict(size=10),
            customdata=df_pivot['Masculino'],
            hovertemplate='<b>Masculino</b><br>' +
                         'Grupo: %{y}<br>' +
                         'Cantidad: %{customdata:,}<br>' +
                         '<extra></extra>'
        ))
    
    # Agregar barras para femenino (derecha)
    if 'Femenino' in df_pivot.columns:
        fig.add_trace(go.Bar(
            y=df_pivot.index,
            x=df_pivot['Femenino'],
            name='Femenino',
            orientation='h',
            marker=dict(
                color=colores['Femenino'],
                line=dict(color='white', width=1)
            ),
            text=[f"{val:,}" for val in df_pivot['Femenino']],
            textposition='outside',
            textfont=dict(size=10),
            hovertemplate='<b>Femenino</b><br>' +
                         'Grupo: %{y}<br>' +
                         'Cantidad: %{text}<br>' +
                         '<extra></extra>'
        ))
    
    # Layout avanzado
    fig.update_layout(
        title={
            'text': 'Análisis Demográfico por Género<br><sub>Gráfico de Mariposa - Vacunación por Grupos de Edad</sub>',
            'x': 0.5,
            'xanchor': 'center',
            'font': {'size': 20, 'color': '#2c3e50'}
        },
        xaxis=dict(
            title='Número de Personas Vacunadas',
            titlefont=dict(size=14, color='#2c3e50'),
            showgrid=True,
            gridcolor='#ecf0f1',
            gridwidth=1,
            zeroline=True,
            zerolinecolor='#34495e',
            zerolinewidth=3,
            tickfont=dict(size=12),
            tickcolor='#7f8c8d'
        ),
        yaxis=dict(
            title='Grupos de Edad',
            titlefont=dict(size=14, color='#2c3e50'),
            tickfont=dict(size=11),
            categoryorder='array',
            categoryarray=df_pivot.index[::-1]  # Invertir orden para mostrar de mayor a menor edad
        ),
        barmode='relative',
        bargap=0.15,
        height=700,
        width=1000,
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="center",
            x=0.5,
            bgcolor="rgba(255,255,255,0.8)",
            bordercolor="black",
            borderwidth=1,
            font=dict(size=12)
        ),
        plot_bgcolor='#fafafa',
        paper_bgcolor='white',
        margin=dict(l=100, r=100, t=100, b=50)
    )
    
    # Agregar anotaciones
    total_masculino = df_pivot.get('Masculino', pd.Series()).sum()
    total_femenino = df_pivot.get('Femenino', pd.Series()).sum()
    
    fig.add_annotation(
        x=-total_masculino/2,
        y=len(df_pivot.index),
        text=f"Total Masculino:<br>{total_masculino:,}",
        showarrow=False,
        font=dict(size=12, color=colores['Masculino']),
        bgcolor="rgba(255,255,255,0.8)",
        bordercolor=colores['Masculino'],
        borderwidth=1
    )
    
    fig.add_annotation(
        x=total_femenino/2,
        y=len(df_pivot.index),
        text=f"Total Femenino:<br>{total_femenino:,}",
        showarrow=False,
        font=dict(size=12, color=colores['Femenino']),
        bgcolor="rgba(255,255,255,0.8)",
        bordercolor=colores['Femenino'],
        borderwidth=1
    )
    
    return fig

def mostrar_seccion_genero():
    """
    Función principal para mostrar la sección de análisis por género en Streamlit.
    """
    
    st.header("📊 Análisis por Género")
    st.markdown("---")
    
    # Opciones de configuración
    col1, col2 = st.columns([1, 1])
    
    with col1:
        tipo_grafico = st.selectbox(
            "Tipo de Gráfico de Mariposa:",
            ["Básico", "Avanzado"],
            help="Selecciona el estilo del gráfico de mariposa"
        )
    
    with col2:
        usar_datos_reales = st.checkbox(
            "Usar datos reales", 
            value=False,
            help="Activa para usar datos reales del sistema (requiere conexión a BD)"
        )
    
    # Simulación de carga de datos reales
    df_datos = None
    if usar_datos_reales:
        st.info("🔄 Conectando a la base de datos para obtener datos reales...")
        # Aquí iría la conexión real a la base de datos
        # df_datos = cargar_datos_reales()
        st.warning("⚠️ Función de datos reales no implementada. Usando datos de ejemplo.")
    
    # Crear y mostrar el gráfico
    if tipo_grafico == "Básico":
        fig = crear_grafico_mariposa_genero(df_datos)
    else:
        fig = crear_grafico_mariposa_avanzado(df_datos)
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Mostrar métricas adicionales
    st.markdown("### 📈 Métricas Resumen")
    
    col1, col2, col3, col4 = st.columns(4)
    
    # Datos de ejemplo para métricas
    if df_datos is None:
        total_masculino = 8945
        total_femenino = 9823
        diferencia = total_femenino - total_masculino
        porcentaje_masc = (total_masculino / (total_masculino + total_femenino)) * 100
    else:
        # Calcular métricas reales
        df_resumen = df_datos.groupby('genero')['cantidad'].sum()
        total_masculino = df_resumen.get('Masculino', 0)
        total_femenino = df_resumen.get('Femenino', 0)
        diferencia = total_femenino - total_masculino
        porcentaje_masc = (total_masculino / (total_masculino + total_femenino)) * 100 if (total_masculino + total_femenino) > 0 else 0
    
    with col1:
        st.metric(
            label="👨 Total Masculino",
            value=f"{total_masculino:,}",
            delta=f"{porcentaje_masc:.1f}%"
        )
    
    with col2:
        st.metric(
            label="👩 Total Femenino",
            value=f"{total_femenino:,}",
            delta=f"{100-porcentaje_masc:.1f}%"
        )
    
    with col3:
        st.metric(
            label="📊 Diferencia",
            value=f"{abs(diferencia):,}",
            delta=f"{(diferencia/max(total_masculino, total_femenino)*100):.1f}%"
        )
    
    with col4:
        st.metric(
            label="👥 Total General",
            value=f"{total_masculino + total_femenino:,}"
        )
    
    # Información adicional
    with st.expander("ℹ️ Información sobre el Gráfico de Mariposa"):
        st.markdown("""
        **¿Qué es un Gráfico de Mariposa?**
        
        Un gráfico de mariposa (butterfly chart) es una visualización especializada que permite comparar 
        dos grupos o categorías de manera simétrica. En este caso, comparamos la distribución por género 
        y grupos de edad.
        
        **Características principales:**
        - **Lado izquierdo**: Representa los datos masculinos
        - **Lado derecho**: Representa los datos femeninos  
        - **Eje central**: Permite comparar fácilmente las diferencias
        - **Grupos de edad**: Organizados verticalmente para facilitar la comparación
        
        **Ventajas:**
        - Comparación visual inmediata entre géneros
        - Identificación rápida de patrones demográficos
        - Visualización clara de diferencias por grupo de edad
        - Fácil interpretación de la distribución total
        """)

# Ejemplo de uso
if __name__ == "__main__":
    mostrar_seccion_genero()