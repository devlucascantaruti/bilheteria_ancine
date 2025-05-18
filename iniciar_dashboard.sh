#!/bin/bash

# Acessa a pasta do projeto
cd /media/hd/bilheteria_ancine || exit 1

# Ativa o ambiente virtual
source .venv/bin/activate

# Redireciona Streamlit e variáveis de ambiente para o HD
HOME=/media/hd \
STREAMLIT_HOME=/media/hd/.streamlit \
XDG_CONFIG_HOME=/media/hd/.streamlit \
XDG_CACHE_HOME=/media/hd/.streamlit/cache \
python -m streamlit run app.py
