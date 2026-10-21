# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Transformação e camada silver
# MAGIC
# MAGIC A silver tipa, limpa, deduplica e concilia os arquivos da bronze. Cada transformação cita a regra de negócio que aplica (RN01 a RN19, descritas em `catalogo/regras_de_negocio.md`). Nenhuma linha é descartada em silêncio: o que sai do universo de análise ganha uma *flag* e é contado no notebook 04.

# COMMAND ----------

from pyspark.sql import functions as F, Window

dbutils.widgets.text("catalogo", "workspace", "Catálogo do Unity Catalog")
CATALOGO = dbutils.widgets.get("catalogo")
BRONZE, SILVER = f"{CATALOGO}.olist_bronze", f"{CATALOGO}.olist_silver"
TS = "yyyy-MM-dd HH:mm:ss"

def salvar(df, nome):
    df.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{SILVER}.{nome}")
    print(f"{SILVER}.{nome}: {spark.table(f'{SILVER}.{nome}').count():,} linhas".replace(",", "."))

def dias(fim, inicio):
    """Duração em dias com fração, a partir da diferença em segundos (RN06)."""
    return (F.unix_timestamp(fim) - F.unix_timestamp(inicio)) / 86400

# COMMAND ----------

# MAGIC %md
# MAGIC ## Itens e pedidos
# MAGIC O pedido herda dos itens a quantidade de vendedores, o vendedor único (quando houver) e o maior prazo-limite de envio, usado para identificar postagem tardia (RN08).

# COMMAND ----------

itens = (spark.table(f"{BRONZE}.order_items")
         .select("order_id",
                 F.col("order_item_id").cast("int"),
                 "product_id", "seller_id",
                 F.to_timestamp("shipping_limit_date", TS).alias("ts_limite_envio"),
                 F.col("price").cast("decimal(10,2)").alias("preco"),
                 F.col("freight_value").cast("decimal(10,2)").alias("frete")))
salvar(itens, "itens_pedido")

resumo_itens = (itens.groupBy("order_id")
                .agg(F.count("*").alias("qtd_itens"),
                     F.countDistinct("seller_id").alias("qtd_vendedores"),
                     F.min("seller_id").alias("_seller_min"),
                     F.max("ts_limite_envio").alias("ts_limite_envio_max"),
                     F.sum("preco").alias("valor_itens"),
                     F.sum("frete").alias("valor_frete"))
                .withColumn("seller_id_unico", F.when(F.col("qtd_vendedores") == 1, F.col("_seller_min")))
                .drop("_seller_min"))

p = (spark.table(f"{BRONZE}.orders")
     .select("order_id", "customer_id", F.col("order_status").alias("status"),
             F.to_timestamp("order_purchase_timestamp", TS).alias("ts_compra"),
             F.to_timestamp("order_approved_at", TS).alias("ts_aprovacao"),
             F.to_timestamp("order_delivered_carrier_date", TS).alias("ts_postagem"),
             F.to_timestamp("order_delivered_customer_date", TS).alias("ts_entrega"),
             F.to_date(F.to_timestamp("order_estimated_delivery_date", TS)).alias("data_estimada"))
     .join(resumo_itens, "order_id", "left"))

pedidos = (p
    # RN01: universo de análise de entrega
    .withColumn("flag_entregue", (F.col("status") == "delivered") & F.col("ts_entrega").isNotNull())
    # RN06: decomposição do lead time
    .withColumn("dias_aprovacao", dias("ts_aprovacao", "ts_compra"))
    .withColumn("dias_postagem", dias("ts_postagem", "ts_aprovacao"))
    .withColumn("dias_transporte", dias("ts_entrega", "ts_postagem"))
    .withColumn("dias_lead_time", dias("ts_entrega", "ts_compra"))
    .withColumn("dias_prometidos", F.datediff("data_estimada", F.to_date("ts_compra")))
    # RN04: atraso por data-calendário
    .withColumn("dias_atraso", F.datediff(F.to_date("ts_entrega"), "data_estimada"))
    .withColumn("flag_atrasado", F.col("dias_atraso") > 0)
    # RN07: qualquer etapa com duração negativa
    .withColumn("flag_inconsistencia_temporal",
                F.coalesce(F.col("dias_aprovacao") < 0, F.lit(False))
                | F.coalesce(F.col("dias_postagem") < 0, F.lit(False))
                | F.coalesce(F.col("dias_transporte") < 0, F.lit(False)))
    # RN08: postagem depois do prazo-limite do vendedor
    .withColumn("flag_postagem_tardia", F.col("ts_postagem") > F.col("ts_limite_envio_max")))
salvar(pedidos, "pedidos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Avaliações
# MAGIC RN10: 547 pedidos têm mais de uma avaliação (551 linhas excedentes). Conserva-se a respondida por último; o empate vai para o maior `review_id`. A junção com o pedido passa a ser um-para-um.

# COMMAND ----------

av = (spark.table(f"{BRONZE}.order_reviews")
      .select("review_id", "order_id",
              F.col("review_score").cast("int").alias("nota"),
              F.to_timestamp("review_creation_date", TS).alias("ts_criacao"),
              F.to_timestamp("review_answer_timestamp", TS).alias("ts_resposta")))
ordem = Window.partitionBy("order_id").orderBy(F.col("ts_resposta").desc(), F.col("review_id").desc())
avaliacoes = av.withColumn("_n", F.row_number().over(ordem)).filter("_n = 1").drop("_n")
salvar(avaliacoes, "avaliacoes")
print(f"Avaliações removidas na deduplicação: {av.count() - avaliacoes.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Geolocalização por prefixo de CEP
# MAGIC RN12 descarta pontos fora do retângulo do Brasil; RN13 resume cada prefixo pela mediana das coordenadas.

# COMMAND ----------

geo = (spark.table(f"{BRONZE}.geolocation")
       .select(F.col("geolocation_zip_code_prefix").cast("int").alias("cep_prefixo"),
               F.col("geolocation_lat").cast("double").alias("lat"),
               F.col("geolocation_lng").cast("double").alias("lng")))
geo_valida = geo.filter(F.col("lat").between(-33.75, 5.27) & F.col("lng").between(-73.99, -34.79))
print(f"Pontos fora do Brasil descartados: {geo.count() - geo_valida.count()}")
geo_prefixo = geo_valida.groupBy("cep_prefixo").agg(F.median("lat").alias("latitude"),
                                                    F.median("lng").alias("longitude"))
salvar(geo_prefixo, "geo_prefixo")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clientes, vendedores, produtos e pagamentos

# COMMAND ----------

salvar(spark.table(f"{BRONZE}.customers").select(
    "customer_id", "customer_unique_id",
    F.col("customer_zip_code_prefix").cast("int").alias("cep_prefixo"),
    F.col("customer_city").alias("cidade"),
    F.upper("customer_state").alias("uf")), "clientes")

salvar(spark.table(f"{BRONZE}.sellers").select(
    "seller_id",
    F.col("seller_zip_code_prefix").cast("int").alias("cep_prefixo"),
    F.col("seller_city").alias("cidade"),
    F.upper("seller_state").alias("uf")), "vendedores")

# RN16: tradução da categoria, com duas traduções manuais e categoria explícita para nulos
traducao = (spark.table(f"{BRONZE}.product_category_name_translation")
            .select("product_category_name", "product_category_name_english")
            .unionByName(spark.createDataFrame(
                [("pc_gamer", "pc_gamer"),
                 ("portateis_cozinha_e_preparadores_de_alimentos", "kitchen_portables_and_food_preparers")],
                "product_category_name string, product_category_name_english string")))
produtos = (spark.table(f"{BRONZE}.products")
            .join(traducao, "product_category_name", "left")
            .select("product_id",
                    F.coalesce("product_category_name", F.lit("sem_categoria")).alias("categoria"),
                    F.coalesce("product_category_name_english", F.lit("sem_categoria")).alias("categoria_en"),
                    F.col("product_photos_qty").cast("int").alias("qtd_fotos"),
                    F.col("product_weight_g").cast("int").alias("peso_g"),
                    F.col("product_length_cm").cast("int").alias("comprimento_cm"),
                    F.col("product_height_cm").cast("int").alias("altura_cm"),
                    F.col("product_width_cm").cast("int").alias("largura_cm")))
salvar(produtos, "produtos")

salvar(spark.table(f"{BRONZE}.order_payments").select(
    "order_id",
    F.col("payment_sequential").cast("int").alias("sequencial"),
    F.col("payment_type").alias("tipo_pagamento"),
    F.col("payment_installments").cast("int").alias("parcelas"),
    F.col("payment_value").cast("decimal(10,2)").alias("valor")), "pagamentos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificações da silver

# COMMAND ----------

def unico(tabela, chaves):
    t = spark.table(f"{SILVER}.{tabela}")
    return t.count() == t.select(*chaves).distinct().count()

checagens = {
    "pedidos: order_id único": unico("pedidos", ["order_id"]),
    "avaliacoes: um registro por pedido": unico("avaliacoes", ["order_id"]),
    "itens_pedido: (order_id, order_item_id) único": unico("itens_pedido", ["order_id", "order_item_id"]),
    "pagamentos: (order_id, sequencial) único": unico("pagamentos", ["order_id", "sequencial"]),
    "geo_prefixo: prefixo único": unico("geo_prefixo", ["cep_prefixo"]),
    "produtos: nenhum perdido na tradução": spark.table(f"{SILVER}.produtos").count() == spark.table(f"{BRONZE}.products").count(),
    "pedidos: nenhum perdido na junção com itens": spark.table(f"{SILVER}.pedidos").count() == spark.table(f"{BRONZE}.orders").count(),
}
for nome, ok in checagens.items():
    print("OK  " if ok else "FALHA", nome)
assert all(checagens.values()), "Há verificação da silver com falha"
