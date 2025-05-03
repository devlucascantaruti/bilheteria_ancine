import os
import duckdb

# Diretórios
base_dir       = 'ancine_data'
master_parquet = os.path.join(base_dir, 'ancine_all.parquet')

# Se o Parquet mestre já existe, pule tudo
if os.path.exists(master_parquet):
    print(f"🎉 '{master_parquet}' já existe. Nada a processar.")
    exit(0)

# Conexão DuckDB persistente
db_path = os.path.join(base_dir, 'ancine.duckdb')
con     = duckdb.connect(database=db_path, read_only=False)

# Unifica todos os Parquets gerados
print("Criando tabela 'ancine' a partir dos Parquets...")
con.execute(
    "CREATE OR REPLACE TABLE ancine AS "
    "SELECT * FROM read_parquet('ancine_data/*.parquet', union_by_name => TRUE);"
)

# Exporta master Parquet
print(f"Exportando master Parquet para {master_parquet}...")
con.execute(f"COPY ancine TO '{master_parquet}' (FORMAT PARQUET);")
print("✅ Exportado master Parquet com sucesso.")

con.close()