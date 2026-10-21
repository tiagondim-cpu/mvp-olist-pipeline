# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Modelo dimensional e camada gold
# MAGIC
# MAGIC A gold é um esquema **constelação**: três tabelas fato que compartilham quatro dimensões conformadas.
# MAGIC
# MAGIC | Tabela | Grão (uma linha por) | Pergunta que atende |
# MAGIC |---|---|---|
# MAGIC | `fato_pedido` | pedido | nível de serviço, origem do atraso, rotas, calibração da promessa, avaliação (P1 a P6) |
# MAGIC | `fato_item_pedido` | item de pedido | receita e frete por produto e vendedor |
# MAGIC | `fato_pagamento` | linha de pagamento | meio de pagamento e parcelamento |
# MAGIC | `dim_cliente`, `dim_vendedor`, `dim_produto`, `dim_data` | entidade | filtros e agrupamentos |
# MAGIC
# MAGIC As dimensões usam as chaves naturais da Olist, que são estáveis e únicas; o modelo não guarda histórico de mudanças (dimensões do tipo 1). A `dim_data` é usada em vários papéis: data da compra, da entrega, prometida e do prazo-limite de envio.
# MAGIC
# MAGIC Ao final, o notebook grava no Unity Catalog a descrição de cada coluna lida de `catalogo/catalogo_dados.csv` e declara chaves primárias e estrangeiras. Assim o Catálogo de Dados fica visível na própria plataforma, no Catalog Explorer.

# COMMAND ----------

import os
import pandas as pd
from pyspark.sql import functions as F

dbutils.widgets.text("catalogo", "workspace", "Catálogo do Unity Catalog")
CATALOGO = dbutils.widgets.get("catalogo")
SILVER, GOLD = f"{CATALOGO}.olist_silver", f"{CATALOGO}.olist_gold"
s = lambda nome: spark.table(f"{SILVER}.{nome}")

# Recria a gold do zero: fatos antes das dimensões, por causa das chaves estrangeiras
for t in ["fato_pagamento", "fato_item_pedido", "fato_pedido", "dim_cliente", "dim_vendedor", "dim_produto", "dim_data"]:
    spark.sql(f"DROP TABLE IF EXISTS {GOLD}.{t}")

def salvar(df, nome):
    df.write.mode("overwrite").saveAsTable(f"{GOLD}.{nome}")
    print(f"{GOLD}.{nome}: {spark.table(f'{GOLD}.{nome}').count():,} linhas".replace(",", "."))

def data_key(col):
    return F.date_format(col, "yyyyMMdd").cast("int")

REGIAO = {**dict.fromkeys(["AC", "AM", "AP", "PA", "RO", "RR", "TO"], "Norte"),
          **dict.fromkeys(["AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE"], "Nordeste"),
          **dict.fromkeys(["DF", "GO", "MS", "MT"], "Centro-Oeste"),
          **dict.fromkeys(["ES", "MG", "RJ", "SP"], "Sudeste"),
          **dict.fromkeys(["PR", "RS", "SC"], "Sul")}
mapa_regiao = F.create_map(*[F.lit(x) for kv in REGIAO.items() for x in kv])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimensões

# COMMAND ----------

dim_data = (spark.sql("SELECT explode(sequence(DATE'2016-01-01', DATE'2020-12-31')) AS data")
            .select(data_key("data").alias("data_key"), "data",
                    F.year("data").alias("ano"), F.quarter("data").alias("trimestre"),
                    F.month("data").alias("mes"), F.date_format("data", "yyyy-MM").alias("ano_mes"),
                    F.dayofweek("data").alias("dia_semana"),
                    F.dayofweek("data").isin(1, 7).alias("flag_fim_de_semana")))
salvar(dim_data, "dim_data")

geo = s("geo_prefixo")
for origem, chave, destino in [("clientes", ["customer_id", "customer_unique_id"], "dim_cliente"),
                               ("vendedores", ["seller_id"], "dim_vendedor")]:
    salvar(s(origem).join(geo, "cep_prefixo", "left")
           .select(*chave, "cep_prefixo", "cidade", "uf", mapa_regiao[F.col("uf")].alias("regiao"),
                   "latitude", "longitude"), destino)

salvar(s("produtos").withColumn("volume_cm3", F.col("comprimento_cm") * F.col("altura_cm") * F.col("largura_cm")),
       "dim_produto")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fato de pedido
# MAGIC Une pedido, avaliação deduplicada e coordenadas de vendedor e cliente. Aplica RN05 (folga), RN09 (origem do atraso), RN11 (avaliação ruim), RN14 (distância) e as faixas de atraso.

# COMMAND ----------

def haversine_km(lat1, lng1, lat2, lng2):
    lat1, lng1, lat2, lng2 = [F.radians(c) for c in (lat1, lng1, lat2, lng2)]
    a = F.sin((lat2 - lat1) / 2) ** 2 + F.cos(lat1) * F.cos(lat2) * F.sin((lng2 - lng1) / 2) ** 2
    return 2 * 6371 * F.asin(F.sqrt(a))

cli = spark.table(f"{GOLD}.dim_cliente").select("customer_id", F.col("latitude").alias("lat_c"), F.col("longitude").alias("lng_c"))
ven = spark.table(f"{GOLD}.dim_vendedor").select(F.col("seller_id").alias("seller_id_unico"), F.col("latitude").alias("lat_v"), F.col("longitude").alias("lng_v"))
av = s("avaliacoes").select("order_id", F.col("nota").alias("nota_avaliacao"))
atraso = F.col("dias_atraso")

fato_pedido = (s("pedidos").join(av, "order_id", "left").join(cli, "customer_id", "left").join(ven, "seller_id_unico", "left")
    .select("order_id", "customer_id", F.col("seller_id_unico").alias("seller_id"), "status",
            data_key("ts_compra").alias("data_compra_key"),
            data_key("ts_entrega").alias("data_entrega_key"),
            data_key("data_estimada").alias("data_estimada_key"),
            "qtd_itens", "qtd_vendedores", "valor_itens", "valor_frete",
            *[F.round(c, 4).alias(c) for c in ["dias_aprovacao", "dias_postagem", "dias_transporte", "dias_lead_time"]],
            "dias_prometidos", "dias_atraso",
            (-atraso).alias("folga_dias"),
            F.when(atraso <= 0, "no prazo ou antecipado").when(atraso <= 3, "1 a 3 dias")
             .when(atraso <= 7, "4 a 7 dias").when(atraso <= 14, "8 a 14 dias")
             .when(atraso.isNotNull(), "15 dias ou mais").alias("faixa_atraso"),
            "flag_entregue", "flag_atrasado", "flag_postagem_tardia",
            F.when(F.col("flag_atrasado") & F.col("flag_postagem_tardia"), "vendedor")
             .when(F.col("flag_atrasado") & ~F.col("flag_postagem_tardia"), "transporte").alias("origem_atraso"),
            "flag_inconsistencia_temporal",
            haversine_km("lat_v", "lng_v", "lat_c", "lng_c").alias("distancia_km"),
            "nota_avaliacao",
            (F.col("nota_avaliacao") <= 2).alias("flag_avaliacao_ruim")))
salvar(fato_pedido, "fato_pedido")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fatos de item e de pagamento
# MAGIC Cada uma mantém seu próprio grão (RN19). Nenhum valor de pagamento é somado à fato de pedido, o que evitaria a duplicação de valores em pedidos com várias linhas de pagamento.

# COMMAND ----------

ped = s("pedidos").select("order_id", "customer_id", data_key("ts_compra").alias("data_compra_key"))

salvar(s("itens_pedido").join(ped, "order_id")
       .select("order_id", "order_item_id", "product_id", "seller_id", "customer_id", "data_compra_key",
               data_key("ts_limite_envio").alias("data_limite_envio_key"), "preco", "frete"),
       "fato_item_pedido")

salvar(s("pagamentos").join(ped, "order_id")
       .select("order_id", "sequencial", "customer_id", "data_compra_key", "tipo_pagamento", "parcelas", "valor"),
       "fato_pagamento")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Chaves e Catálogo de Dados no Unity Catalog
# MAGIC As restrições de chave no Databricks são informativas: documentam o modelo e orientam o otimizador, mas não bloqueiam carga. A integridade é verificada de fato no notebook 04.

# COMMAND ----------

PK = {"dim_data": ["data_key"], "dim_cliente": ["customer_id"], "dim_vendedor": ["seller_id"],
      "dim_produto": ["product_id"], "fato_pedido": ["order_id"],
      "fato_item_pedido": ["order_id", "order_item_id"], "fato_pagamento": ["order_id", "sequencial"]}
FK = [("fato_pedido", "customer_id", "dim_cliente", "customer_id"),
      ("fato_pedido", "seller_id", "dim_vendedor", "seller_id"),
      ("fato_pedido", "data_compra_key", "dim_data", "data_key"),
      ("fato_pedido", "data_entrega_key", "dim_data", "data_key"),
      ("fato_pedido", "data_estimada_key", "dim_data", "data_key"),
      ("fato_item_pedido", "order_id", "fato_pedido", "order_id"),
      ("fato_item_pedido", "product_id", "dim_produto", "product_id"),
      ("fato_item_pedido", "seller_id", "dim_vendedor", "seller_id"),
      ("fato_item_pedido", "customer_id", "dim_cliente", "customer_id"),
      ("fato_item_pedido", "data_compra_key", "dim_data", "data_key"),
      ("fato_item_pedido", "data_limite_envio_key", "dim_data", "data_key"),
      ("fato_pagamento", "order_id", "fato_pedido", "order_id"),
      ("fato_pagamento", "customer_id", "dim_cliente", "customer_id"),
      ("fato_pagamento", "data_compra_key", "dim_data", "data_key")]

for tabela, cols in PK.items():
    for c in cols:
        spark.sql(f"ALTER TABLE {GOLD}.{tabela} ALTER COLUMN {c} SET NOT NULL")
    spark.sql(f"ALTER TABLE {GOLD}.{tabela} ADD CONSTRAINT pk_{tabela} PRIMARY KEY ({', '.join(cols)})")
for tabela, col, ref, ref_col in FK:
    spark.sql(f"ALTER TABLE {GOLD}.{tabela} ADD CONSTRAINT fk_{tabela}_{col} "
              f"FOREIGN KEY ({col}) REFERENCES {GOLD}.{ref} ({ref_col})")

DESCRICAO_TABELA = {
    "dim_data": "Calendário de 2016 a 2020, usado nos papéis de compra, entrega, promessa e limite de envio",
    "dim_cliente": "Cliente do pedido, com localização pelo prefixo de CEP",
    "dim_vendedor": "Vendedor do marketplace, com localização pelo prefixo de CEP",
    "dim_produto": "Produto, categoria e dimensões da embalagem",
    "fato_pedido": "Um registro por pedido: prazos, etapas do lead time, atraso, origem do atraso, distância e avaliação",
    "fato_item_pedido": "Um registro por item de pedido: preço, frete e prazo-limite de envio",
    "fato_pagamento": "Um registro por linha de pagamento do pedido",
}
caminho = os.path.join(os.getcwd(), "..", "catalogo", "catalogo_dados.csv")
catalogo = pd.read_csv(caminho, sep=";", dtype=str).fillna("")
esc = lambda t: t.replace("'", "\\'")
for tabela, texto in DESCRICAO_TABELA.items():
    spark.sql(f"COMMENT ON TABLE {GOLD}.{tabela} IS '{esc(texto)}'")
for r in catalogo.itertuples():
    spark.sql(f"ALTER TABLE {GOLD}.{r.tabela} ALTER COLUMN {r.atributo} COMMENT '{esc(r.descricao)}'")

# Catálogo e modelo precisam descrever exatamente as mesmas colunas
no_modelo = {(t, c) for t in PK for c in spark.table(f"{GOLD}.{t}").columns}
no_catalogo = set(zip(catalogo.tabela, catalogo.atributo))
assert no_modelo == no_catalogo, f"Divergência catálogo x modelo: {no_modelo ^ no_catalogo}"
print(f"Catálogo aplicado: {len(no_catalogo)} colunas em {len(PK)} tabelas, {len(PK)} chaves primárias e {len(FK)} estrangeiras")
