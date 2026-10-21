# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Coleta e camada bronze
# MAGIC
# MAGIC **MVP: nível de serviço logístico da Olist** · Tiago Andre Gondim · 231013476
# MAGIC
# MAGIC Este notebook baixa os nove arquivos CSV do conjunto público da Olist (versão 2 no Kaggle), grava os arquivos originais em um *volume* do Unity Catalog e cria uma tabela Delta por arquivo na camada **bronze**. A bronze guarda tudo como texto, exatamente como veio da origem, acrescido de duas colunas de rastreio: arquivo de origem e momento da ingestão.
# MAGIC
# MAGIC | Item | Valor |
# MAGIC |---|---|
# MAGIC | Fonte | Brazilian E-Commerce Public Dataset by Olist, `olistbr/brazilian-ecommerce`, versão 2 |
# MAGIC | Licença | CC BY-NC-SA 4.0 (uso não comercial, com atribuição e compartilhamento pela mesma licença) |
# MAGIC | Destino dos arquivos | `/Volumes/<catálogo>/olist_bronze/arquivos_origem` |
# MAGIC | Destino das tabelas | `<catálogo>.olist_bronze.<nome_do_arquivo>` |
# MAGIC
# MAGIC Se o ambiente não tiver acesso à internet, envie os nove CSVs manualmente para o volume (Catalog > olist_bronze > arquivos_origem > Upload) e rode o notebook de novo: a célula de coleta detecta os arquivos já presentes.

# COMMAND ----------

# MAGIC %pip install kagglehub==1.0.2 --quiet

# COMMAND ----------

dbutils.widgets.text("catalogo", "workspace", "Catálogo do Unity Catalog")
CATALOGO = dbutils.widgets.get("catalogo")
VOLUME = f"/Volumes/{CATALOGO}/olist_bronze/arquivos_origem"
DATASET = "olistbr/brazilian-ecommerce/versions/2"
ARQUIVOS = [
    "olist_customers_dataset.csv",
    "olist_geolocation_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
    "olist_orders_dataset.csv",
    "olist_products_dataset.csv",
    "olist_sellers_dataset.csv",
    "product_category_name_translation.csv",
]

for camada in ["olist_bronze", "olist_silver", "olist_gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{camada}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOGO}.olist_bronze.arquivos_origem")
print("Esquemas e volume prontos em", CATALOGO)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Coleta
# MAGIC Download pelo pacote oficial `kagglehub`. O conjunto é público; não há credencial no código.

# COMMAND ----------

import os, shutil

def arquivos_no_volume():
    try:
        return {f.name for f in dbutils.fs.ls(VOLUME)}
    except Exception:
        return set()

faltantes = [a for a in ARQUIVOS if a not in arquivos_no_volume()]
if faltantes:
    os.environ["KAGGLEHUB_CACHE"] = "/tmp/kagglehub"
    import kagglehub
    pasta = kagglehub.dataset_download(DATASET)
    for nome in faltantes:
        shutil.copy(os.path.join(pasta, nome), f"{VOLUME}/{nome}")
    print(f"{len(faltantes)} arquivos baixados do Kaggle e gravados no volume")
else:
    print("Os nove arquivos já estão no volume; download dispensado")

ausentes = [a for a in ARQUIVOS if a not in arquivos_no_volume()]
assert not ausentes, f"Arquivos ausentes no volume: {ausentes}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carga na bronze
# MAGIC Leitura com `multiLine` e aspas escapadas, porque os comentários das avaliações contêm quebras de linha. Todas as colunas entram como texto; tipagem e limpeza ficam na silver.

# COMMAND ----------

from pyspark.sql import functions as F

registro = []
for nome in ARQUIVOS:
    tabela = nome.removesuffix(".csv").removeprefix("olist_").removesuffix("_dataset")
    df = (spark.read
          .option("header", True)
          .option("multiLine", True)
          .option("quote", '"')
          .option("escape", '"')
          .option("inferSchema", False)
          .csv(f"{VOLUME}/{nome}")
          .withColumn("_arquivo_origem", F.lit(nome))
          .withColumn("_ingestao_em", F.current_timestamp()))
    (df.write.mode("overwrite").option("overwriteSchema", True)
       .saveAsTable(f"{CATALOGO}.olist_bronze.{tabela}"))
    tamanho = dbutils.fs.ls(f"{VOLUME}/{nome}")[0].size
    registro.append((nome, f"{CATALOGO}.olist_bronze.{tabela}", tamanho, df.count(), len(df.columns) - 2))

registro_df = (spark.createDataFrame(registro, "arquivo string, tabela string, bytes long, linhas long, colunas int")
               .withColumn("fonte", F.lit("Kaggle: " + DATASET))
               .withColumn("licenca", F.lit("CC BY-NC-SA 4.0"))
               .withColumn("formato_original", F.lit("CSV, UTF-8, separador vírgula"))
               .withColumn("coletado_em", F.current_timestamp()))
registro_df.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{CATALOGO}.olist_bronze.registro_coleta")
display(registro_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificação
# MAGIC As contagens precisam bater com as linhas dos arquivos publicados pela Olist. Uma divergência indica erro de leitura (por exemplo, quebra de linha dentro de comentário mal interpretada).

# COMMAND ----------

ESPERADO = {
    "olist_customers_dataset.csv": 99441,
    "olist_geolocation_dataset.csv": 1000163,
    "olist_order_items_dataset.csv": 112650,
    "olist_order_payments_dataset.csv": 103886,
    "olist_order_reviews_dataset.csv": 99224,
    "olist_orders_dataset.csv": 99441,
    "olist_products_dataset.csv": 32951,
    "olist_sellers_dataset.csv": 3095,
    "product_category_name_translation.csv": 71,
}
obtido = {r.arquivo: r.linhas for r in registro_df.collect()}
divergentes = {k: (v, obtido.get(k)) for k, v in ESPERADO.items() if obtido.get(k) != v}
assert not divergentes, f"Contagens divergentes (esperado, obtido): {divergentes}"
print(f"Bronze verificada: {sum(obtido.values()):,} linhas em {len(obtido)} tabelas".replace(",", "."))
