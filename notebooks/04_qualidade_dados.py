# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Qualidade dos dados
# MAGIC
# MAGIC Três níveis de verificação, todos gravados em tabelas da gold para consulta e auditoria:
# MAGIC
# MAGIC 1. **Atributos da origem** (`dq_bronze_atributos`): ausências e cardinalidade de cada uma das 52 colunas dos nove arquivos, antes de qualquer tratamento.
# MAGIC 2. **Atributos do modelo** (`dq_gold_atributos`): para cada coluna do Catálogo de Dados, completude, obrigatoriedade, cardinalidade, mínimo, máximo e quantidade de valores fora do domínio declarado.
# MAGIC 3. **Regras** (`dq_regras`): unicidade de chaves, integridade referencial, consistência temporal, acurácia geográfica e atualidade.
# MAGIC
# MAGIC Valor fora do domínio não é necessariamente erro: o domínio do catálogo é a faixa *esperada*, e o que escapa dela precisa ser examinado e explicado.

# COMMAND ----------

import os
import pandas as pd
from pyspark.sql import functions as F

dbutils.widgets.text("catalogo", "workspace", "Catálogo do Unity Catalog")
CATALOGO = dbutils.widgets.get("catalogo")
BRONZE, SILVER, GOLD = (f"{CATALOGO}.olist_{c}" for c in ("bronze", "silver", "gold"))
catalogo = pd.read_csv(os.path.join(os.getcwd(), "..", "catalogo", "catalogo_dados.csv"), sep=";", dtype=str).fillna("")

def gravar(pdf, nome):
    spark.createDataFrame(pdf).write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{GOLD}.{nome}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Atributos da origem
# MAGIC Na bronze tudo é texto; ausência é nulo ou texto vazio.

# COMMAND ----------

linhas = []
for t in [r.tableName for r in spark.sql(f"SHOW TABLES IN {BRONZE}").collect() if r.tableName != "registro_coleta"]:
    df = spark.table(f"{BRONZE}.{t}")
    cols = [c for c in df.columns if not c.startswith("_")]
    vazio = lambda c: F.col(c).isNull() | (F.trim(F.col(c)) == "")
    r = df.agg(F.count("*").alias("_n"),
               *[F.sum(vazio(c).cast("int")).alias(f"nulo_{c}") for c in cols],
               *[F.approx_count_distinct(c).alias(f"dist_{c}") for c in cols]).first()
    for c in cols:
        linhas.append({"tabela_origem": t, "atributo": c, "linhas": r["_n"], "ausentes": r[f"nulo_{c}"],
                       "pct_ausentes": round(100 * r[f"nulo_{c}"] / r["_n"], 2),
                       "distintos_aprox": r[f"dist_{c}"]})
dq_bronze = pd.DataFrame(linhas)
gravar(dq_bronze, "dq_bronze_atributos")
display(dq_bronze.sort_values("pct_ausentes", ascending=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Atributos do modelo contra o Catálogo de Dados

# COMMAND ----------

def fora_do_dominio(col, tipo_spark, r):
    if r.categorias:
        return ~F.lower(col.cast("string")).isin([x.lower() for x in r.categorias.split("|")])
    cond = F.lit(False)
    if r.dominio_min:
        cond = cond | (col < F.lit(r.dominio_min).cast(tipo_spark))
    if r.dominio_max:
        cond = cond | (col > F.lit(r.dominio_max).cast(tipo_spark))
    return cond

linhas = []
for tabela, grupo in catalogo.groupby("tabela", sort=False):
    df = spark.table(f"{GOLD}.{tabela}")
    tipos = dict(df.dtypes)
    exprs = [F.count("*").alias("_n")]
    for r in grupo.itertuples():
        c = F.col(r.atributo)
        exprs += [F.sum(c.isNull().cast("int")).alias(f"nulo_{r.atributo}"),
                  F.approx_count_distinct(c).alias(f"dist_{r.atributo}"),
                  F.min(c).cast("string").alias(f"min_{r.atributo}"),
                  F.max(c).cast("string").alias(f"max_{r.atributo}"),
                  F.sum((c.isNotNull() & fora_do_dominio(c, tipos[r.atributo], r)).cast("int")).alias(f"fora_{r.atributo}")]
    agg = df.agg(*exprs).first()
    for r in grupo.itertuples():
        nulos = agg[f"nulo_{r.atributo}"]
        linhas.append({"tabela": tabela, "atributo": r.atributo, "tipo": r.tipo, "linhas": agg["_n"],
                       "nulos": nulos, "pct_nulos": round(100 * nulos / agg["_n"], 2),
                       "obrigatorio": r.obrigatorio, "viola_obrigatoriedade": r.obrigatorio == "sim" and nulos > 0,
                       "distintos_aprox": agg[f"dist_{r.atributo}"],
                       "minimo": agg[f"min_{r.atributo}"], "maximo": agg[f"max_{r.atributo}"],
                       "dominio": r.categorias or (f"[{r.dominio_min}, {r.dominio_max}]" if r.dominio_min else "livre"),
                       "fora_do_dominio": agg[f"fora_{r.atributo}"]})
dq_gold = pd.DataFrame(linhas)
gravar(dq_gold, "dq_gold_atributos")
display(dq_gold)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Atributos que exigem atenção
# MAGIC Colunas obrigatórias com nulos ou com valores fora do domínio declarado.

# COMMAND ----------

display(dq_gold[(dq_gold.fora_do_dominio > 0) | dq_gold.viola_obrigatoriedade]
        [["tabela", "atributo", "nulos", "minimo", "maximo", "dominio", "fora_do_dominio"]])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Regras de qualidade
# MAGIC Cada regra é uma consulta SQL que conta violações. O resultado esperado indica se a violação é tolerada (tratada por uma regra de negócio) ou bloqueante.

# COMMAND ----------

REGRAS = [
    ("unicidade", "Pedido duplicado na fato_pedido",
     f"SELECT count(*) - count(DISTINCT order_id) FROM {GOLD}.fato_pedido", "zero"),
    ("unicidade", "Item duplicado na fato_item_pedido",
     f"SELECT count(*) - count(DISTINCT order_id, order_item_id) FROM {GOLD}.fato_item_pedido", "zero"),
    ("unicidade", "Avaliações a mais na origem (tratado por RN10)",
     f"SELECT count(*) - count(DISTINCT order_id) FROM {BRONZE}.order_reviews", "tolerado"),
    ("unicidade", "review_id repetido em pedidos diferentes na origem",
     f"SELECT count(*) - count(DISTINCT review_id) FROM {BRONZE}.order_reviews", "tolerado"),
    ("integridade", "Item sem pedido correspondente",
     f"SELECT count(*) FROM {GOLD}.fato_item_pedido i LEFT ANTI JOIN {GOLD}.fato_pedido p USING (order_id)", "zero"),
    ("integridade", "Item com produto inexistente",
     f"SELECT count(*) FROM {GOLD}.fato_item_pedido i LEFT ANTI JOIN {GOLD}.dim_produto p USING (product_id)", "zero"),
    ("integridade", "Item com vendedor inexistente",
     f"SELECT count(*) FROM {GOLD}.fato_item_pedido i LEFT ANTI JOIN {GOLD}.dim_vendedor v USING (seller_id)", "zero"),
    ("integridade", "Pedido com cliente inexistente",
     f"SELECT count(*) FROM {GOLD}.fato_pedido p LEFT ANTI JOIN {GOLD}.dim_cliente c USING (customer_id)", "zero"),
    ("integridade", "Pagamento sem pedido correspondente",
     f"SELECT count(*) FROM {GOLD}.fato_pagamento g LEFT ANTI JOIN {GOLD}.fato_pedido p USING (order_id)", "zero"),
    ("integridade", "Avaliação de pedido inexistente",
     f"SELECT count(*) FROM {SILVER}.avaliacoes a LEFT ANTI JOIN {GOLD}.fato_pedido p USING (order_id)", "zero"),
    ("completude", "Pedido sem nenhum item (pedidos cancelados ou indisponíveis)",
     f"SELECT count(*) FROM {GOLD}.fato_pedido WHERE qtd_itens IS NULL", "tolerado"),
    ("completude", "Pedido entregue sem avaliação",
     f"SELECT count(*) FROM {GOLD}.fato_pedido WHERE flag_entregue AND nota_avaliacao IS NULL", "tolerado"),
    ("completude", "Pedido entregue de vendedor único sem distância (prefixo sem coordenada)",
     f"SELECT count(*) FROM {GOLD}.fato_pedido WHERE flag_entregue AND seller_id IS NOT NULL AND distancia_km IS NULL", "tolerado"),
    ("consistência", "Pedido entregue com etapa de duração negativa (RN07)",
     f"SELECT count(*) FROM {GOLD}.fato_pedido WHERE flag_entregue AND flag_inconsistencia_temporal", "tolerado"),
    ("consistência", "Status delivered sem data de entrega",
     f"SELECT count(*) FROM {GOLD}.fato_pedido WHERE status = 'delivered' AND data_entrega_key IS NULL", "tolerado"),
    ("consistência", "Data de entrega preenchida com status diferente de delivered",
     f"SELECT count(*) FROM {GOLD}.fato_pedido WHERE status <> 'delivered' AND data_entrega_key IS NOT NULL", "tolerado"),
    ("consistência", "Soma dos pagamentos difere do valor de itens mais frete em mais de R$ 1",
     f"""SELECT count(*) FROM (SELECT order_id, sum(valor) v FROM {GOLD}.fato_pagamento GROUP BY order_id) g
         JOIN {GOLD}.fato_pedido p USING (order_id) WHERE abs(g.v - (p.valor_itens + p.valor_frete)) > 1""", "tolerado"),
    ("acurácia", "Ponto de geolocalização fora do Brasil (tratado por RN12)",
     f"""SELECT count(*) FROM {BRONZE}.geolocation WHERE NOT (CAST(geolocation_lat AS DOUBLE) BETWEEN -33.75 AND 5.27
         AND CAST(geolocation_lng AS DOUBLE) BETWEEN -73.99 AND -34.79)""", "tolerado"),
    ("acurácia", "Prazo-limite de envio posterior a 2018",
     f"SELECT count(*) FROM {GOLD}.fato_item_pedido WHERE data_limite_envio_key > 20181231", "tolerado"),
    ("acurácia", "Produto com peso zero",
     f"SELECT count(*) FROM {GOLD}.dim_produto WHERE peso_g = 0", "tolerado"),
    ("atualidade", "Dias entre a última compra registrada e a data da análise",
     f"SELECT datediff(current_date(), max(to_date(CAST(data_compra_key AS STRING), 'yyyyMMdd'))) FROM {GOLD}.fato_pedido", "informativo"),
]
resultado = pd.DataFrame([{"dimensao": d, "regra": n, "violacoes": spark.sql(q).first()[0], "esperado": e, "consulta": q}
                          for d, n, q, e in REGRAS])
resultado["status"] = resultado.apply(
    lambda r: "FALHA" if r.esperado == "zero" and r.violacoes > 0 else ("ok" if r.esperado == "zero" else r.esperado), axis=1)
gravar(resultado, "dq_regras")
display(resultado[["dimensao", "regra", "violacoes", "status"]])
assert (resultado.status != "FALHA").all(), "Regra bloqueante violada"
