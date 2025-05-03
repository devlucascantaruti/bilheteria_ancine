# Dockerfile para Dashboard ANCINE
# Usa Python slim como base para imagem leve
FROM python:3.10-slim

# Define diretório de trabalho
WORKDIR /app

# Copia apenas dependências primeiro para cache de camadas
COPY requirements.txt ./

# Instala dependências do sistema necessárias para geodados, DuckDB e outros
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    gdal-bin \
    libgdal-dev \
    && rm -rf /var/lib/apt/lists/*

# Instala pacotes Python
RUN pip install --no-cache-dir -r requirements.txt

# Copia código da aplicação
COPY app.py tmdb_utils.py brazil_states.geojson ./

# Cria pasta de dados (volume externo pode ser montado aqui)
RUN mkdir -p ancine_data

# Expõe a porta padrão do Streamlit
EXPOSE 8501

# Comando de inicialização
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.headless=true"]