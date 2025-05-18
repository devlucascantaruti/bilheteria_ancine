import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

base_dir = 'ancine_data'
csv_files = [f for f in os.listdir(base_dir) if f.lower().endswith('.csv')]

for csv_name in csv_files:
    csv_path = os.path.join(base_dir, csv_name)
    name, _ = os.path.splitext(csv_name)
    parquet_path = os.path.join(base_dir, f"{name}.parquet")

    if os.path.exists(parquet_path):
        print(f"🟡 Já existe, pulando: {parquet_path}")
        continue

    try:
        print(f"🔄 Convertendo {csv_name} → {name}.parquet ...")
        df = pd.read_csv(csv_path, sep=';', encoding='latin1')
        table = pa.Table.from_pandas(df)
        pq.write_table(table, parquet_path)
        print(f"✅ Salvo: {parquet_path} ({len(df)} registros)")
    except Exception as e:
        print(f"❌ Erro em {csv_name}: {e}")
