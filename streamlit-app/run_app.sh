#!/bin/bash

# Script para ejecutar la aplicación Streamlit de Vacunación MSP

echo "🚀 Iniciando Sistema de Vacunación MSP..."
echo "==============================================="

# Verificar si Python está instalado
if ! command -v python3 &> /dev/null; then
    echo "❌ Error: Python 3 no está instalado"
    exit 1
fi

# Verificar si pip está instalado
if ! command -v pip3 &> /dev/null; then
    echo "❌ Error: pip3 no está instalado"
    exit 1
fi

# Instalar dependencias si no están instaladas
echo "📦 Verificando dependencias..."
pip3 install -r requirements.txt --quiet

# Verificar si streamlit está instalado
if ! command -v streamlit &> /dev/null; then
    echo "❌ Error: Streamlit no se instaló correctamente"
    exit 1
fi

echo "✅ Dependencias verificadas"
echo "🌐 Iniciando servidor Streamlit..."
echo "📱 La aplicación estará disponible en:"
echo "   - http://localhost:8000"
echo "   - http://$(hostname -I | awk '{print $1}'):8000"
echo "==============================================="

# Ejecutar la aplicación
cd src && streamlit run app.py --server.port=8080 --server.address=0.0.0.0
