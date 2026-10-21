# Regras de negócio aplicadas na transformação

Cada regra abaixo altera a interpretação dos resultados. O código que a implementa está indicado entre parênteses. Os volumes afetados por cada regra são medidos no notebook `04_qualidade_dados` e reportados no README.

## Universo e chaves

| Código | Regra | Motivo |
|---|---|---|
| RN01 | O universo de análise de entrega são pedidos com `order_status = 'delivered'` e `order_delivered_customer_date` preenchida. | Só esses pedidos têm atraso observável. Pedidos ainda em trânsito ficam de fora e isso censura à direita os meses finais. (02_silver) |
| RN02 | A série mensal usa compras de janeiro/2017 a agosto/2018. | Os meses de 2016 e de setembro/outubro de 2018 têm menos de 300 pedidos entregues, volume insuficiente para comparar taxas. (05_analise) |
| RN03 | `customer_id` identifica o cliente do pedido; `customer_unique_id` identifica a pessoa. A dimensão de cliente usa `customer_id` como chave natural e preserva `customer_unique_id` como atributo. | No conjunto Olist, cada pedido gera um `customer_id` novo. (03_gold) |

## Datas e prazos

| Código | Regra | Motivo |
|---|---|---|
| RN04 | Atraso é medido por data-calendário: `dias_atraso = data(entrega ao cliente) − data(entrega estimada)`. Pedido atrasado é aquele com `dias_atraso > 0`. | A data estimada não tem hora (vem como 00:00:00); comparar carimbos de data e hora classificaria como atrasada uma entrega feita no próprio dia prometido. (02_silver) |
| RN05 | Folga da promessa: `folga_dias = −dias_atraso`. Valores positivos indicam entrega antes do prometido. | Mede calibração do prazo comunicado ao cliente. (03_gold) |
| RN06 | O lead time é decomposto em três etapas, em dias com fração (horas ÷ 24): aprovação (`approved − purchase`), postagem pelo vendedor (`delivered_carrier − approved`) e transporte (`delivered_customer − delivered_carrier`). | Separa o que é responsabilidade do vendedor do que é responsabilidade da transportadora. (02_silver) |
| RN07 | Pedidos com sequência temporal impossível, isto é, com qualquer uma das três etapas da RN06 com duração negativa (aprovação antes da compra, postagem antes da aprovação ou entrega antes da postagem), recebem `flag_inconsistencia_temporal = true`. Eles permanecem no modelo, mas ficam fora das análises de decomposição do lead time. | Excluir em silêncio esconderia o problema; usar distorceria as médias por etapa. (02_silver) |
| RN08 | Postagem tardia do vendedor: `order_delivered_carrier_date > max(shipping_limit_date)` entre os itens do pedido. | O prazo-limite de envio é o compromisso do vendedor com a plataforma. (02_silver) |
| RN09 | Origem do atraso de um pedido atrasado: `vendedor` quando houve postagem tardia; `transporte` caso contrário. | Regra simples e auditável. Ela atribui ao vendedor todo atraso de um pedido postado fora do prazo, mesmo quando o transporte também demorou, e portanto superestima a parcela do vendedor. (03_gold) |

## Avaliações

| Código | Regra | Motivo |
|---|---|---|
| RN10 | Uma avaliação por pedido: quando há mais de uma, conserva-se a de `review_answer_timestamp` mais recente; empate é resolvido pelo maior `review_id`. | 547 pedidos têm mais de uma avaliação, somando 551 linhas excedentes. A junção com a fato de pedido precisa ser um-para-um. (02_silver) |
| RN11 | Avaliação ruim: nota 1 ou 2. | Corte usual de insatisfação em escala de cinco pontos. (03_gold) |

## Geografia e distância

| Código | Regra | Motivo |
|---|---|---|
| RN12 | Coordenadas fora do retângulo que contém o Brasil (latitude −33,75 a 5,27; longitude −73,99 a −34,79) são descartadas. | Há pontos com latitude 45, isto é, fora do país. (02_silver) |
| RN13 | Cada prefixo de CEP recebe uma coordenada: a mediana da latitude e a mediana da longitude dos pontos válidos daquele prefixo. | O arquivo traz vários pontos por prefixo; a mediana é robusta a pontos errados residuais. (02_silver) |
| RN14 | Distância vendedor→cliente pela fórmula de haversine (raio 6.371 km), calculada só para pedidos de um único vendedor. Prefixos sem coordenada geram distância nula. | Pedidos com mais de um vendedor (1,3%) não têm uma origem única. (03_gold) |
| RN15 | Rota = UF do vendedor → UF do cliente, apenas para pedidos de um único vendedor. Rotas com menos de 300 pedidos entregues não entram em rankings. | Evita que rotas raras liderem o ranking por acaso. (05_analise) |

## Produto e vendedor

| Código | Regra | Motivo |
|---|---|---|
| RN16 | A categoria em inglês vem da tabela de tradução. As duas categorias sem tradução (`pc_gamer` e `portateis_cozinha_e_preparadores_de_alimentos`) recebem tradução manual; produto sem categoria recebe `sem_categoria`. | Evita perder itens na junção. (02_silver) |
| RN17 | Rankings de vendedor consideram apenas vendedores com pelo menos 30 pedidos entregues de vendedor único. | Taxas calculadas sobre poucos pedidos são instáveis. (05_analise) |

## Valores

| Código | Regra | Motivo |
|---|---|---|
| RN18 | Valores monetários em reais, como no arquivo de origem, sem correção inflacionária. | O período tem dois anos; a correção não altera as comparações relativas feitas aqui. (03_gold) |
| RN19 | Valor do pedido = soma de `price` dos itens; frete do pedido = soma de `freight_value`. Pagamentos ficam em fato própria e não são somados à fato de pedido. | Pagamento parcelado e voucher geram várias linhas por pedido; misturar granularidades duplicaria valores. (03_gold) |
