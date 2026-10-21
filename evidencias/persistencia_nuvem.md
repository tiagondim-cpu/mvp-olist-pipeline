# Persistência na nuvem

Tabelas Delta no Unity Catalog do workspace Databricks Free Edition (catálogo `workspace`), consultadas pela API de SQL depois da execução do job. Gerado por `scripts/executar_e_exportar.py`.

| Esquema | Tabela | Formato | Linhas | Criada em (UTC) | Descrição |
|---|---|---|---:|---|---|
| olist_bronze | customers | DELTA | 99.441 | 2026-10-21 15:24:49 | Cópia fiel de olist_customers_dataset.csv: colunas da origem como texto, mais arquivo de origem e momento da ingestão |
| olist_bronze | geolocation | DELTA | 1.000.163 | 2026-10-21 15:24:59 | Cópia fiel de olist_geolocation_dataset.csv: colunas da origem como texto, mais arquivo de origem e momento da ingestão |
| olist_bronze | order_items | DELTA | 112.650 | 2026-10-21 15:25:08 | Cópia fiel de olist_order_items_dataset.csv: colunas da origem como texto, mais arquivo de origem e momento da ingestão |
| olist_bronze | order_payments | DELTA | 103.886 | 2026-10-21 15:25:13 | Cópia fiel de olist_order_payments_dataset.csv: colunas da origem como texto, mais arquivo de origem e momento da ingestão |
| olist_bronze | order_reviews | DELTA | 99.224 | 2026-10-21 15:25:20 | Cópia fiel de olist_order_reviews_dataset.csv: colunas da origem como texto, mais arquivo de origem e momento da ingestão |
| olist_bronze | orders | DELTA | 99.441 | 2026-10-21 15:25:26 | Cópia fiel de olist_orders_dataset.csv: colunas da origem como texto, mais arquivo de origem e momento da ingestão |
| olist_bronze | product_category_name_translation | DELTA | 71 | 2026-10-21 15:25:43 | Cópia fiel de product_category_name_translation.csv: colunas da origem como texto, mais arquivo de origem e momento da ingestão |
| olist_bronze | products | DELTA | 32.951 | 2026-10-21 15:25:32 | Cópia fiel de olist_products_dataset.csv: colunas da origem como texto, mais arquivo de origem e momento da ingestão |
| olist_bronze | registro_coleta | DELTA | 9 | 2026-10-21 15:25:47 | Registro da coleta: arquivo, volume, linhas, fonte, licença e data |
| olist_bronze | sellers | DELTA | 3.095 | 2026-10-21 15:25:38 | Cópia fiel de olist_sellers_dataset.csv: colunas da origem como texto, mais arquivo de origem e momento da ingestão |
| olist_gold | dim_cliente | DELTA | 99.441 | 2026-10-21 16:18:22 | Cliente do pedido, com localização pelo prefixo de CEP |
| olist_gold | dim_data | DELTA | 1.827 | 2026-10-21 16:18:17 | Calendário de 2016 a 2020, usado nos papéis de compra, entrega, promessa e limite de envio |
| olist_gold | dim_produto | DELTA | 32.951 | 2026-10-21 16:18:30 | Produto, categoria e dimensões da embalagem |
| olist_gold | dim_vendedor | DELTA | 3.095 | 2026-10-21 16:18:26 | Vendedor do marketplace, com localização pelo prefixo de CEP |
| olist_gold | dq_bronze_atributos | DELTA | 52 | 2026-10-21 15:30:12 | Qualidade: ausências e cardinalidade de cada coluna da origem |
| olist_gold | dq_gold_atributos | DELTA | 75 | 2026-10-21 15:30:24 | Qualidade: cada coluna do modelo conferida contra o Catálogo de Dados |
| olist_gold | dq_regras | DELTA | 21 | 2026-10-21 15:30:49 | Qualidade: regras de unicidade, integridade, consistência, acurácia e atualidade |
| olist_gold | fato_item_pedido | DELTA | 112.650 | 2026-10-21 16:18:42 | Um registro por item de pedido: preço, frete e prazo-limite de envio |
| olist_gold | fato_pagamento | DELTA | 103.886 | 2026-10-21 16:18:46 | Um registro por linha de pagamento do pedido |
| olist_gold | fato_pedido | DELTA | 99.441 | 2026-10-21 16:18:36 | Um registro por pedido: prazos, etapas do lead time, atraso, origem do atraso, distância e avaliação |
| olist_silver | avaliacoes | DELTA | 98.673 | 2026-10-21 15:26:18 | Uma avaliação por pedido, deduplicada pela regra RN10 |
| olist_silver | clientes | DELTA | 99.441 | 2026-10-21 15:26:35 | Clientes tipados, UF em maiúsculas |
| olist_silver | geo_prefixo | DELTA | 19.010 | 2026-10-21 15:26:28 | Coordenada mediana por prefixo de CEP, só com pontos dentro do Brasil (RN12 e RN13) |
| olist_silver | itens_pedido | DELTA | 112.650 | 2026-10-21 15:26:03 | Itens de pedido tipados |
| olist_silver | pagamentos | DELTA | 103.886 | 2026-10-21 15:26:49 | Linhas de pagamento tipadas |
| olist_silver | pedidos | DELTA | 99.441 | 2026-10-21 15:26:10 | Pedidos tipados, com etapas do lead time, atraso e flags das regras RN01, RN04, RN06, RN07 e RN08 |
| olist_silver | produtos | DELTA | 32.951 | 2026-10-21 15:26:45 | Produtos com categoria traduzida e sem nulos de categoria (RN16) |
| olist_silver | vendedores | DELTA | 3.095 | 2026-10-21 15:26:40 | Vendedores tipados, UF em maiúsculas |
