import os
import zipfile
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import ijson
import itertools

# Diretórios
tmp_extract = os.path.join('ancine_data', 'tmp_extract')
base_dir = 'ancine_data'

# Cria pasta temporária
os.makedirs(tmp_extract, exist_ok=True)

# Função para converter JSON grande em Parquet via streaming
def json_to_parquet_stream(input_path, output_path, prefix='data.item', chunksize=100_000):
    """
    Converte um JSON padrão {'data':[...]} em Parquet em chunks, sem carregar tudo na memória.
    """
    writer = None
    with open(input_path, 'rb') as f:
        items = ijson.items(f, prefix)
        while True:
            batch = list(itertools.islice(items, chunksize))
            if not batch:
                break
            df = pd.json_normalize(batch)
            table = pa.Table.from_pandas(df)
            if writer is None:
                writer = pq.ParquetWriter(output_path, table.schema)
            writer.write_table(table)
            print(f"Escrito chunk de {len(df)} registros em {output_path}")
    if writer:
        writer.close()
        print(f"JSON -> Parquet completo: {output_path}")
    else:
        print(f"Nenhum registro em {input_path}, pulando JSON.")

# Processa arquivos ZIP
zip_files = [f for f in os.listdir(base_dir) if f.lower().endswith('.zip')]
for zip_name in zip_files:
    zip_path = os.path.join(base_dir, zip_name)
    extract_dir = os.path.join(tmp_extract, os.path.splitext(zip_name)[0])
    os.makedirs(extract_dir, exist_ok=True)
    print(f"Extraindo {zip_name} para {extract_dir}...")
    with zipfile.ZipFile(zip_path, 'r') as z:
        z.extractall(extract_dir)

    for root, _, files in os.walk(extract_dir):
        for fname in files:
            path = os.path.join(root, fname)
            name, ext = os.path.splitext(fname)
            ext = ext.lower()
            out_path = os.path.join(base_dir, f"{name}.parquet")
            if os.path.exists(out_path):
                print(f"Parquet já existe, pulando: {out_path}")
                continue
            try:
                if ext == '.csv':
                    df = pd.read_csv(path, sep=';', encoding='latin1')
                    table = pa.Table.from_pandas(df)
                    pq.write_table(table, out_path)
                    print(f"CSV -> Parquet: {out_path} ({len(df)} registros)")
                elif ext in ('.xlsx', '.xls'):
                    df = pd.read_excel(path)
                    table = pa.Table.from_pandas(df)
                    pq.write_table(table, out_path)
                    print(f"Excel -> Parquet: {out_path} ({len(df)} registros)")
                elif ext == '.json':
                    json_to_parquet_stream(path, out_path)
                elif ext == '.parquet':
                    os.replace(path, out_path)
                    print(f"Movido Parquet: {out_path}")
            except Exception as e:
                print(f"❌ Erro ao processar {csv_path}: {e}")
                import traceback
                traceback.print_exc()


# 🆕 NOVO BLOCO: Processa arquivos CSV soltos na pasta principal
csv_files = [f for f in os.listdir(base_dir) if f.lower().endswith('.csv')]
for csv_name in csv_files:
    csv_path = os.path.join(base_dir, csv_name)
    name, _ = os.path.splitext(csv_name)
    out_path = os.path.join(base_dir, f"{name}.parquet")
    if os.path.exists(out_path):
        print(f"Parquet já existe, pulando: {out_path}")
        continue
    try:
        df = pd.read_csv(csv_path, sep=';', encoding='latin1')
        table = pa.Table.from_pandas(df)
        pq.write_table(table, out_path)
        print(f"CSV direto -> Parquet: {out_path} ({len(df)} registros)")
    except Exception as e:
        print(f"Erro ao processar {csv_path}: {e}")

print("✅ Processo de extração e conversão para Parquet concluído.")
