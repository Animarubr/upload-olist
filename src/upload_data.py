import os
import pandas as pd
import sqlalchemy

from olistlib.db import utils

# Endereços do projeto e sub pastas
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")

# Carregando os nomes dos arquivos
files_names = [i for i in os.listdir(DATA_DIR) if i.endswith(".csv")]

# Abrindo conexão com o banco...
connection = utils.connect_db("postgres", os.path.join(BASE_DIR, '.env'))

# Para cada arquivo é realizado uma inserção no banco de dados
for i in files_names:
    df_tmp = pd.read_csv(os.path.join(DATA_DIR, i))
    table_name = "tb_" + i.replace("olist_", "").replace("_dataset","").split(".csv")[0]
    df_tmp.to_sql(
        table_name,
        connection,
        schema='olist',
        if_exists='replace',
        index=False)

