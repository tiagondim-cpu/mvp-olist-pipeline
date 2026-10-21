# Catálogo de Dados

Fonte única: [`catalogo_dados.csv`](catalogo_dados.csv), lido pelo pipeline para gravar as descrições no Unity Catalog (notebook 03) e para conferir cada coluna contra o domínio declarado (notebook 04). As colunas **observado**, **nulos** e **fora do domínio** vêm da execução de 21/10/2026 no Databricks (tabela `olist_gold.dq_gold_atributos`).

Como ler: *domínio* é a faixa esperada pelo negócio. Valor fora dela não é descartado; é contado e explicado no README (seção 6.1). *Nulo significa* explica o que a ausência representa quando a coluna admite nulos.


## dim_data · calendário

1.827 linhas.

| Atributo | Tipo | Descrição | Domínio | Observado | Nulos | Fora do domínio | Obrigatório | Nulo significa | Linhagem |
|---|---|---|---|---|---:|---:|---|---|---|
| `data_key` | numérico | Chave da data no formato AAAAMMDD | 20160101 a 20201231 | 20160101 a 20201231 | 0 | 0 | sim | - | Gerada por sequência de datas no notebook 03 |
| `data` | data | Data-calendário | 2016-01-01 a 2020-12-31 | 2016-01-01 a 2020-12-31 | 0 | 0 | sim | - | Gerada por sequência de datas no notebook 03 |
| `ano` | numérico | Ano civil | 2016 a 2020 | 2016 a 2020 | 0 | 0 | sim | - | Derivado de data |
| `trimestre` | numérico | Trimestre do ano | 1 a 4 | 1 a 4 | 0 | 0 | sim | - | Derivado de data |
| `mes` | numérico | Mês do ano | 1 a 12 | 1 a 12 | 0 | 0 | sim | - | Derivado de data |
| `ano_mes` | categórico | Ano e mês no formato AAAA-MM | livre | 60 valores distintos | 0 | 0 | sim | - | Derivado de data |
| `dia_semana` | numérico | Dia da semana (1 = domingo, 7 = sábado) | 1 a 7 | 1 a 7 | 0 | 0 | sim | - | Derivado de data pela função dayofweek do Spark |
| `flag_fim_de_semana` | booleano | Verdadeiro para sábado e domingo | true, false | 2 categorias | 0 | 0 | sim | - | Derivado de dia_semana |

## dim_cliente · cliente do pedido

99.441 linhas.

| Atributo | Tipo | Descrição | Domínio | Observado | Nulos | Fora do domínio | Obrigatório | Nulo significa | Linhagem |
|---|---|---|---|---|---:|---:|---|---|---|
| `customer_id` | texto | Identificador do cliente no pedido, cada pedido gera um valor novo (RN03) | livre | 99.441 valores distintos | 0 | 0 | sim | - | olist_customers_dataset.customer_id sem transformação |
| `customer_unique_id` | texto | Identificador da pessoa compradora, repete-se entre pedidos do mesmo comprador | livre | 96.096 valores distintos | 0 | 0 | sim | - | olist_customers_dataset.customer_unique_id sem transformação |
| `cep_prefixo` | numérico | Cinco primeiros dígitos do CEP do cliente | 1000 a 99999 | 1003 a 99990 | 0 | 0 | sim | - | olist_customers_dataset.customer_zip_code_prefix convertido para inteiro |
| `cidade` | texto | Município do cliente em minúsculas sem acento, como no arquivo de origem | livre | 4.119 valores distintos | 0 | 0 | sim | - | olist_customers_dataset.customer_city sem transformação |
| `uf` | categórico | Unidade da federação do cliente | AC, AL, AM, AP, BA, CE, DF, ES, GO, MA, MG, MS, MT, PA, PB, PE, PI, PR, RJ, RN, RO, RR, RS, SC, SE, SP, TO | 27 categorias | 0 | 0 | sim | - | olist_customers_dataset.customer_state em maiúsculas |
| `regiao` | categórico | Região geográfica do IBGE correspondente à UF | Norte, Nordeste, Centro-Oeste, Sudeste, Sul | 5 categorias | 0 | 0 | sim | - | Mapeamento fixo UF para região no notebook 03 |
| `latitude` | numérico | Latitude mediana dos pontos válidos do prefixo de CEP (RN12 e RN13) | -33,75 a 5,27 | -33,69 a 3,84 | 279 | 0 | não | Prefixo de CEP sem ponto válido no arquivo de geolocalização | olist_geolocation_dataset filtrado pelo retângulo do Brasil e agregado pela mediana por prefixo |
| `longitude` | numérico | Longitude mediana dos pontos válidos do prefixo de CEP (RN12 e RN13) | -73,99 a -34,79 | -72,67 a -34,80 | 279 | 0 | não | Prefixo de CEP sem ponto válido no arquivo de geolocalização | olist_geolocation_dataset filtrado pelo retângulo do Brasil e agregado pela mediana por prefixo |

## dim_vendedor · vendedor

3.095 linhas.

| Atributo | Tipo | Descrição | Domínio | Observado | Nulos | Fora do domínio | Obrigatório | Nulo significa | Linhagem |
|---|---|---|---|---|---:|---:|---|---|---|
| `seller_id` | texto | Identificador do vendedor | livre | 3.095 valores distintos | 0 | 0 | sim | - | olist_sellers_dataset.seller_id sem transformação |
| `cep_prefixo` | numérico | Cinco primeiros dígitos do CEP do vendedor | 1000 a 99999 | 1001 a 99730 | 0 | 0 | sim | - | olist_sellers_dataset.seller_zip_code_prefix convertido para inteiro |
| `cidade` | texto | Município do vendedor como no arquivo de origem | livre | 611 valores distintos | 0 | 0 | sim | - | olist_sellers_dataset.seller_city sem transformação |
| `uf` | categórico | Unidade da federação do vendedor | AC, AL, AM, AP, BA, CE, DF, ES, GO, MA, MG, MS, MT, PA, PB, PE, PI, PR, RJ, RN, RO, RR, RS, SC, SE, SP, TO | 23 categorias | 0 | 0 | sim | - | olist_sellers_dataset.seller_state em maiúsculas |
| `regiao` | categórico | Região geográfica do IBGE correspondente à UF | Norte, Nordeste, Centro-Oeste, Sudeste, Sul | 5 categorias | 0 | 0 | sim | - | Mapeamento fixo UF para região no notebook 03 |
| `latitude` | numérico | Latitude mediana dos pontos válidos do prefixo de CEP (RN12 e RN13) | -33,75 a 5,27 | -32,07 a -2,50 | 7 | 0 | não | Prefixo de CEP sem ponto válido no arquivo de geolocalização | olist_geolocation_dataset filtrado e agregado pela mediana por prefixo |
| `longitude` | numérico | Longitude mediana dos pontos válidos do prefixo de CEP (RN12 e RN13) | -73,99 a -34,79 | -67,81 a -34,86 | 7 | 0 | não | Prefixo de CEP sem ponto válido no arquivo de geolocalização | olist_geolocation_dataset filtrado e agregado pela mediana por prefixo |

## dim_produto · produto

32.951 linhas.

| Atributo | Tipo | Descrição | Domínio | Observado | Nulos | Fora do domínio | Obrigatório | Nulo significa | Linhagem |
|---|---|---|---|---|---:|---:|---|---|---|
| `product_id` | texto | Identificador do produto | livre | 32.951 valores distintos | 0 | 0 | sim | - | olist_products_dataset.product_id sem transformação |
| `categoria` | categórico | Categoria do produto em português (73 valores mais sem_categoria) | livre | 74 valores distintos | 0 | 0 | sim | - | olist_products_dataset.product_category_name, nulo substituído por sem_categoria (RN16) |
| `categoria_en` | categórico | Categoria do produto em inglês | livre | 74 valores distintos | 0 | 0 | sim | - | product_category_name_translation, duas traduções manuais e sem_categoria para nulos (RN16) |
| `qtd_fotos` | numérico | Quantidade de fotos no anúncio | 1 a 20 | 1 a 20 | 610 | 0 | não | Anúncio sem metadados de cadastro | olist_products_dataset.product_photos_qty convertido para inteiro |
| `peso_g` | numérico | Peso do produto em gramas | 1 a 50000 | 0 a 40425 | 2 | 4 | não | Produto sem medidas cadastradas | olist_products_dataset.product_weight_g convertido para inteiro |
| `comprimento_cm` | numérico | Comprimento da embalagem em centímetros | 1 a 200 | 7 a 105 | 2 | 0 | não | Produto sem medidas cadastradas | olist_products_dataset.product_length_cm convertido para inteiro |
| `altura_cm` | numérico | Altura da embalagem em centímetros | 1 a 200 | 2 a 105 | 2 | 0 | não | Produto sem medidas cadastradas | olist_products_dataset.product_height_cm convertido para inteiro |
| `largura_cm` | numérico | Largura da embalagem em centímetros | 1 a 200 | 6 a 118 | 2 | 0 | não | Produto sem medidas cadastradas | olist_products_dataset.product_width_cm convertido para inteiro |
| `volume_cm3` | numérico | Volume da embalagem (comprimento x altura x largura) | 1 a 8000000 | 168 a 296208 | 2 | 0 | não | Produto sem medidas cadastradas | Calculado no notebook 03 |

## fato_pedido · um registro por pedido

99.441 linhas.

| Atributo | Tipo | Descrição | Domínio | Observado | Nulos | Fora do domínio | Obrigatório | Nulo significa | Linhagem |
|---|---|---|---|---|---:|---:|---|---|---|
| `order_id` | texto | Identificador do pedido, chave primária da fato | livre | 99.441 valores distintos | 0 | 0 | sim | - | olist_orders_dataset.order_id sem transformação |
| `customer_id` | texto | Chave estrangeira para dim_cliente | livre | 99.441 valores distintos | 0 | 0 | sim | - | olist_orders_dataset.customer_id |
| `seller_id` | texto | Chave estrangeira para dim_vendedor quando o pedido tem um único vendedor | livre | 3.078 valores distintos | 2.053 | 0 | não | Pedido com mais de um vendedor (RN14) | Único seller_id de olist_order_items_dataset por pedido |
| `status` | categórico | Situação do pedido na origem | delivered, shipped, canceled, unavailable, invoiced, processing, created, approved | 8 categorias | 0 | 0 | sim | - | olist_orders_dataset.order_status sem transformação |
| `data_compra_key` | numérico | Chave estrangeira para dim_data (data da compra) | 20160101 a 20201231 | 20160904 a 20181017 | 0 | 0 | sim | - | Data de olist_orders_dataset.order_purchase_timestamp |
| `data_entrega_key` | numérico | Chave estrangeira para dim_data (data da entrega ao cliente) | 20160101 a 20201231 | 20161011 a 20181017 | 2.965 | 0 | não | Pedido não entregue | Data de olist_orders_dataset.order_delivered_customer_date |
| `data_estimada_key` | numérico | Chave estrangeira para dim_data (data de entrega prometida) | 20160101 a 20201231 | 20160930 a 20181112 | 0 | 0 | sim | - | olist_orders_dataset.order_estimated_delivery_date |
| `qtd_itens` | numérico | Quantidade de itens do pedido | 1 a 21 | 1 a 21 | 775 | 0 | não | Pedido sem itens no arquivo de itens | Contagem em olist_order_items_dataset |
| `qtd_vendedores` | numérico | Quantidade de vendedores distintos no pedido | 1 a 5 | 1 a 5 | 775 | 0 | não | Pedido sem itens no arquivo de itens | Contagem distinta de seller_id em olist_order_items_dataset |
| `valor_itens` | numérico | Soma dos preços dos itens em reais (RN19) | 0,85 a 15000 | 0,85 a 13.440,00 | 775 | 0 | não | Pedido sem itens no arquivo de itens | Soma de olist_order_items_dataset.price |
| `valor_frete` | numérico | Soma dos fretes dos itens em reais (RN19) | 0 a 2000 | 0,00 a 1.794,96 | 775 | 0 | não | Pedido sem itens no arquivo de itens | Soma de olist_order_items_dataset.freight_value |
| `dias_aprovacao` | numérico | Dias entre compra e aprovação do pagamento (RN06) | 0 a 10 | 0,00 a 187,88 | 160 | 48 | não | Pagamento não aprovado | order_approved_at menos order_purchase_timestamp em horas dividido por 24 |
| `dias_postagem` | numérico | Dias entre aprovação e entrega à transportadora (RN06) | 0 a 30 | -171,22 a 125,76 | 1.797 | 1.544 | não | Pedido não postado | order_delivered_carrier_date menos order_approved_at em horas dividido por 24 |
| `dias_transporte` | numérico | Dias entre postagem e entrega ao cliente (RN06) | 0 a 60 | -16,10 a 205,19 | 2.966 | 248 | não | Pedido não entregue | order_delivered_customer_date menos order_delivered_carrier_date em horas dividido por 24 |
| `dias_lead_time` | numérico | Dias entre compra e entrega ao cliente | 0 a 90 | 0,53 a 209,63 | 2.965 | 77 | não | Pedido não entregue | order_delivered_customer_date menos order_purchase_timestamp em horas dividido por 24 |
| `dias_prometidos` | numérico | Dias entre a data da compra e a data prometida | 1 a 180 | 2 a 156 | 0 | 0 | sim | - | Data estimada menos data da compra |
| `dias_atraso` | numérico | Dias de calendário entre entrega e data prometida, positivo indica atraso (RN04) | -60 a 60 | -147 a 188 | 2.965 | 113 | não | Pedido não entregue | Data de entrega menos data estimada |
| `folga_dias` | numérico | Dias de antecedência em relação à promessa, negativo indica atraso (RN05) | -60 a 60 | -188 a 147 | 2.965 | 113 | não | Pedido não entregue | Igual a menos dias_atraso |
| `faixa_atraso` | categórico | Faixa de dias de atraso | no prazo ou antecipado, 1 a 3 dias, 4 a 7 dias, 8 a 14 dias, 15 dias ou mais | 5 categorias | 2.965 | 0 | não | Pedido não entregue | Derivada de dias_atraso |
| `flag_entregue` | booleano | Pedido no universo de análise de entrega (RN01) | true, false | 2 categorias | 0 | 0 | sim | - | status = delivered e data de entrega preenchida |
| `flag_atrasado` | booleano | Entregue depois da data prometida (RN04) | true, false | 2 categorias | 2.965 | 0 | não | Pedido não entregue | dias_atraso maior que zero |
| `flag_postagem_tardia` | booleano | Postado depois do prazo-limite de envio do vendedor (RN08) | true, false | 2 categorias | 1.784 | 0 | não | Pedido não postado ou sem itens | order_delivered_carrier_date maior que o máximo de shipping_limit_date dos itens |
| `origem_atraso` | categórico | Etapa a que se atribui o atraso (RN09) | vendedor, transporte | 2 categorias | 92.907 | 0 | não | Pedido não atrasado | vendedor se flag_postagem_tardia, transporte caso contrário |
| `flag_inconsistencia_temporal` | booleano | Sequência de datas impossível (RN07) | true, false | 2 categorias | 0 | 0 | sim | - | Alguma etapa do lead time com duração negativa |
| `distancia_km` | numérico | Distância em linha reta entre vendedor e cliente (RN14) | 0 a 4500 | 0,00 a 3.579,34 | 2.537 | 0 | não | Pedido com vários vendedores ou prefixo sem coordenada | Haversine entre as coordenadas de dim_vendedor e dim_cliente |
| `nota_avaliacao` | numérico | Nota dada pelo cliente (RN10) | 1 a 5 | 1 a 5 | 768 | 0 | não | Pedido sem avaliação | olist_order_reviews_dataset.review_score após deduplicação |
| `flag_avaliacao_ruim` | booleano | Nota 1 ou 2 (RN11) | true, false | 2 categorias | 768 | 0 | não | Pedido sem avaliação | Derivado de nota_avaliacao |

## fato_item_pedido · um registro por item

112.650 linhas.

| Atributo | Tipo | Descrição | Domínio | Observado | Nulos | Fora do domínio | Obrigatório | Nulo significa | Linhagem |
|---|---|---|---|---|---:|---:|---|---|---|
| `order_id` | texto | Identificador do pedido, parte da chave primária | livre | 98.666 valores distintos | 0 | 0 | sim | - | olist_order_items_dataset.order_id |
| `order_item_id` | numérico | Sequencial do item dentro do pedido, parte da chave primária | 1 a 21 | 1 a 21 | 0 | 0 | sim | - | olist_order_items_dataset.order_item_id |
| `product_id` | texto | Chave estrangeira para dim_produto | livre | 32.951 valores distintos | 0 | 0 | sim | - | olist_order_items_dataset.product_id |
| `seller_id` | texto | Chave estrangeira para dim_vendedor | livre | 3.095 valores distintos | 0 | 0 | sim | - | olist_order_items_dataset.seller_id |
| `customer_id` | texto | Chave estrangeira para dim_cliente | livre | 98.666 valores distintos | 0 | 0 | sim | - | olist_orders_dataset.customer_id pela junção com o pedido |
| `data_compra_key` | numérico | Chave estrangeira para dim_data (data da compra) | 20160101 a 20201231 | 20160904 a 20180903 | 0 | 0 | sim | - | olist_orders_dataset.order_purchase_timestamp pela junção com o pedido |
| `data_limite_envio_key` | numérico | Chave estrangeira para dim_data (prazo-limite de envio do vendedor) | 20160101 a 20201231 | 20160919 a 20200409 | 0 | 0 | sim | - | olist_order_items_dataset.shipping_limit_date |
| `preco` | numérico | Preço do item em reais | 0,01 a 7000 | 0,85 a 6.735,00 | 0 | 0 | sim | - | olist_order_items_dataset.price |
| `frete` | numérico | Frete do item em reais | 0 a 500 | 0,00 a 409,68 | 0 | 0 | sim | - | olist_order_items_dataset.freight_value |

## fato_pagamento · um registro por linha de pagamento

103.886 linhas.

| Atributo | Tipo | Descrição | Domínio | Observado | Nulos | Fora do domínio | Obrigatório | Nulo significa | Linhagem |
|---|---|---|---|---|---:|---:|---|---|---|
| `order_id` | texto | Identificador do pedido, parte da chave primária | livre | 99.440 valores distintos | 0 | 0 | sim | - | olist_order_payments_dataset.order_id |
| `sequencial` | numérico | Ordem do pagamento dentro do pedido, parte da chave primária | 1 a 29 | 1 a 29 | 0 | 0 | sim | - | olist_order_payments_dataset.payment_sequential |
| `customer_id` | texto | Chave estrangeira para dim_cliente | livre | 99.440 valores distintos | 0 | 0 | sim | - | olist_orders_dataset.customer_id pela junção com o pedido |
| `data_compra_key` | numérico | Chave estrangeira para dim_data (data da compra) | 20160101 a 20201231 | 20160904 a 20181017 | 0 | 0 | sim | - | olist_orders_dataset.order_purchase_timestamp pela junção com o pedido |
| `tipo_pagamento` | categórico | Meio de pagamento | credit_card, boleto, voucher, debit_card, not_defined | 5 categorias | 0 | 0 | sim | - | olist_order_payments_dataset.payment_type |
| `parcelas` | numérico | Número de parcelas | 1 a 24 | 0 a 24 | 0 | 2 | sim | - | olist_order_payments_dataset.payment_installments |
| `valor` | numérico | Valor pago nesta linha em reais | 0,01 a 15000 | 0,00 a 13.664,08 | 0 | 9 | sim | - | olist_order_payments_dataset.payment_value |
