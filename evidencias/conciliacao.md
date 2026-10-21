# Conciliação Databricks e pandas

Compara evidencias/resultado_databricks.json, produzido pelo notebook 05 sobre a gold, com evidencias/referencia_pandas.json, calculado dos CSVs brutos. Contagens exigem igualdade exata; taxas e participações entre 0 e 1 admitem diferença absoluta de até 0,0002; medianas em dias e médias, até 0,06. A tolerância contempla arredondamentos Spark HALF_UP e numpy half-even. A coluna diferença mostra valores absolutos. Ausências são DIVERGENTE.

255 de 256 comparações dentro da tolerância

Diferenças explicadas: 1. DIVERGENTE: 0. Somente linhas com resultado ok entram no numerador do resumo.

## P1: Nível de serviço

| métrica | Databricks | pandas | diferença | resultado |
| --- | --- | --- | --- | --- |
| Global: pedidos entregues | 96.470 | 96.470 | 0 | ok |
| Global: atrasados | 6.534 | 6.534 | 0 | ok |
| Global: taxa de atraso | 0,0677 | 0,0677 | 0,0000 | ok |
| 2017-01: pedidos | 750 | 750 | 0 | ok |
| 2017-01: taxa de atraso | 0,0293 | 0,0293 | 0,0000 | ok |
| 2017-02: pedidos | 1.653 | 1.653 | 0 | ok |
| 2017-02: taxa de atraso | 0,0296 | 0,0296 | 0,0000 | ok |
| 2017-03: pedidos | 2.546 | 2.546 | 0 | ok |
| 2017-03: taxa de atraso | 0,0456 | 0,0456 | 0,0000 | ok |
| 2017-04: pedidos | 2.303 | 2.303 | 0 | ok |
| 2017-04: taxa de atraso | 0,0656 | 0,0656 | 0,0000 | ok |
| 2017-05: pedidos | 3.545 | 3.545 | 0 | ok |
| 2017-05: taxa de atraso | 0,0299 | 0,0299 | 0,0000 | ok |
| 2017-06: pedidos | 3.135 | 3.135 | 0 | ok |
| 2017-06: taxa de atraso | 0,0303 | 0,0303 | 0,0000 | ok |
| 2017-07: pedidos | 3.872 | 3.872 | 0 | ok |
| 2017-07: taxa de atraso | 0,0279 | 0,0279 | 0,0000 | ok |
| 2017-08: pedidos | 4.193 | 4.193 | 0 | ok |
| 2017-08: taxa de atraso | 0,0291 | 0,0291 | 0,0000 | ok |
| 2017-09: pedidos | 4.150 | 4.150 | 0 | ok |
| 2017-09: taxa de atraso | 0,0439 | 0,0439 | 0,0000 | ok |
| 2017-10: pedidos | 4.478 | 4.478 | 0 | ok |
| 2017-10: taxa de atraso | 0,0418 | 0,0418 | 0,0000 | ok |
| 2017-11: pedidos | 7.288 | 7.288 | 0 | ok |
| 2017-11: taxa de atraso | 0,1240 | 0,1240 | 0,0000 | ok |
| 2017-12: pedidos | 5.513 | 5.513 | 0 | ok |
| 2017-12: taxa de atraso | 0,0746 | 0,0746 | 0,0000 | ok |
| 2018-01: pedidos | 7.069 | 7.069 | 0 | ok |
| 2018-01: taxa de atraso | 0,0570 | 0,0570 | 0,0000 | ok |
| 2018-02: pedidos | 6.555 | 6.555 | 0 | ok |
| 2018-02: taxa de atraso | 0,1413 | 0,1413 | 0,0000 | ok |
| 2018-03: pedidos | 7.003 | 7.003 | 0 | ok |
| 2018-03: taxa de atraso | 0,1896 | 0,1896 | 0,0000 | ok |
| 2018-04: pedidos | 6.798 | 6.798 | 0 | ok |
| 2018-04: taxa de atraso | 0,0450 | 0,0450 | 0,0000 | ok |
| 2018-05: pedidos | 6.749 | 6.749 | 0 | ok |
| 2018-05: taxa de atraso | 0,0656 | 0,0656 | 0,0000 | ok |
| 2018-06: pedidos | 6.096 | 6.096 | 0 | ok |
| 2018-06: taxa de atraso | 0,0116 | 0,0116 | 0,0000 | ok |
| 2018-07: pedidos | 6.156 | 6.156 | 0 | ok |
| 2018-07: taxa de atraso | 0,0338 | 0,0338 | 0,0000 | ok |
| 2018-08: pedidos | 6.351 | 6.351 | 0 | ok |
| 2018-08: taxa de atraso | 0,0619 | 0,0619 | 0,0000 | ok |

## P2: Origem do atraso

| métrica | Databricks | pandas | diferença | resultado |
| --- | --- | --- | --- | --- |
| Atrasados válidos nas etapas | 6.510 | 6.510 | 0 | ok |
| Atrasados na divisão por origem | 6.509 | 6.510 | 1 | diferença explicada |
| Origem transporte: participação | 0,7213 | 0,7214 | 0,0001 | ok |
| Origem vendedor: participação | 0,2787 | 0,2786 | 0,0001 | ok |
| atrasados: mediana de aprovacao (dias) | 0,0200 | 0,0166 | 0,0034 | ok |
| atrasados: mediana de postagem (dias) | 3,0700 | 3,0665 | 0,0035 | ok |
| atrasados: mediana de transporte (dias) | 26,2000 | 26,1981 | 0,0019 | ok |
| no prazo: mediana de aprovacao (dias) | 0,0100 | 0,0139 | 0,0039 | ok |
| no prazo: mediana de postagem (dias) | 1,7900 | 1,7920 | 0,0020 | ok |
| no prazo: mediana de transporte (dias) | 6,9600 | 6,9568 | 0,0032 | ok |

O Databricks exclui da divisão por origem os atrasados com origem nula (sem data de entrega à transportadora); o pandas pode classificá-los como transporte.

Na base comum de 6.510 atrasados válidos, as participações calculadas com as contagens Databricks e os casos sem origem atribuídos a transporte seriam 0,2786 (vendedor) e 0,7214 (transporte). Ambas concordam com o pandas dentro da tolerância. A tabela preserva os valores publicados.

- diferença explicada, Atrasados na divisão por origem: 1 pedido(s) a menos na divisão por origem. O Databricks exclui da divisão por origem os atrasados com origem nula (sem data de entrega à transportadora); o pandas pode classificá-los como transporte.

## P3: Rotas e distância

| métrica | Databricks | pandas | diferença | resultado |
| --- | --- | --- | --- | --- |
| Top 10: quantidade de registros | 10 | 10 | 0 | ok |
| Top 1: rota | SP->MA | SP->MA | igual | ok |
| Top 1: pedidos | 486 | 486 | 0 | ok |
| Top 1: atrasados | 93 | 93 | 0 | ok |
| Top 1: taxa_atraso | 0,1914 | 0,1914 | 0,0000 | ok |
| Top 1: participacao_atrasos | 0,0143 | 0,0143 | 0,0000 | ok |
| Top 2: rota | SP->PI | SP->PI | igual | ok |
| Top 2: pedidos | 327 | 327 | 0 | ok |
| Top 2: atrasados | 54 | 54 | 0 | ok |
| Top 2: taxa_atraso | 0,1651 | 0,1651 | 0,0000 | ok |
| Top 2: participacao_atrasos | 0,0083 | 0,0083 | 0,0000 | ok |
| Top 3: rota | SP->RJ | SP->RJ | igual | ok |
| Top 3: pedidos | 8.048 | 8.048 | 0 | ok |
| Top 3: atrasados | 1.147 | 1.147 | 0 | ok |
| Top 3: taxa_atraso | 0,1425 | 0,1425 | 0,0000 | ok |
| Top 3: participacao_atrasos | 0,1759 | 0,1759 | 0,0000 | ok |
| Top 4: rota | SP->CE | SP->CE | igual | ok |
| Top 4: pedidos | 961 | 961 | 0 | ok |
| Top 4: atrasados | 133 | 133 | 0 | ok |
| Top 4: taxa_atraso | 0,1384 | 0,1384 | 0,0000 | ok |
| Top 4: participacao_atrasos | 0,0204 | 0,0204 | 0,0000 | ok |
| Top 5: rota | SP->BA | SP->BA | igual | ok |
| Top 5: pedidos | 2.278 | 2.278 | 0 | ok |
| Top 5: atrasados | 297 | 297 | 0 | ok |
| Top 5: taxa_atraso | 0,1304 | 0,1304 | 0,0000 | ok |
| Top 5: participacao_atrasos | 0,0455 | 0,0455 | 0,0000 | ok |
| Top 6: rota | SP->MS | SP->MS | igual | ok |
| Top 6: pedidos | 471 | 471 | 0 | ok |
| Top 6: atrasados | 59 | 59 | 0 | ok |
| Top 6: taxa_atraso | 0,1253 | 0,1253 | 0,0000 | ok |
| Top 6: participacao_atrasos | 0,0090 | 0,0090 | 0,0000 | ok |
| Top 7: rota | SP->ES | SP->ES | igual | ok |
| Top 7: pedidos | 1.444 | 1.444 | 0 | ok |
| Top 7: atrasados | 180 | 180 | 0 | ok |
| Top 7: taxa_atraso | 0,1247 | 0,1247 | 0,0000 | ok |
| Top 7: participacao_atrasos | 0,0276 | 0,0276 | 0,0000 | ok |
| Top 8: rota | PR->RJ | PR->RJ | igual | ok |
| Top 8: pedidos | 951 | 951 | 0 | ok |
| Top 8: atrasados | 117 | 117 | 0 | ok |
| Top 8: taxa_atraso | 0,1230 | 0,1230 | 0,0000 | ok |
| Top 8: participacao_atrasos | 0,0179 | 0,0179 | 0,0000 | ok |
| Top 9: rota | SP->PA | SP->PA | igual | ok |
| Top 9: pedidos | 675 | 675 | 0 | ok |
| Top 9: atrasados | 81 | 81 | 0 | ok |
| Top 9: taxa_atraso | 0,1200 | 0,1200 | 0,0000 | ok |
| Top 9: participacao_atrasos | 0,0124 | 0,0124 | 0,0000 | ok |
| Top 10: rota | SP->PB | SP->PB | igual | ok |
| Top 10: pedidos | 333 | 333 | 0 | ok |
| Top 10: atrasados | 35 | 35 | 0 | ok |
| Top 10: taxa_atraso | 0,1051 | 0,1051 | 0,0000 | ok |
| Top 10: participacao_atrasos | 0,0054 | 0,0054 | 0,0000 | ok |
| Top 10: participação total nos atrasos | 0,3367 | 0,3367 | 0,0000 | ok |
| Distância [0,100) km: pedidos | 17.694 | 17.694 | 0 | ok |
| Distância [0,100) km: taxa de atraso | 0,0450 | 0,0450 | 0,0000 | ok |
| Distância [100,500) km: pedidos | 36.462 | 36.462 | 0 | ok |
| Distância [100,500) km: taxa de atraso | 0,0618 | 0,0618 | 0,0000 | ok |
| Distância [500,1000) km: pedidos | 25.389 | 25.389 | 0 | ok |
| Distância [500,1000) km: taxa de atraso | 0,0721 | 0,0721 | 0,0000 | ok |
| Distância [1000,2000) km: pedidos | 9.608 | 9.608 | 0 | ok |
| Distância [1000,2000) km: taxa de atraso | 0,0961 | 0,0961 | 0,0000 | ok |
| Distância [2000,inf) km: pedidos | 5.571 | 5.571 | 0 | ok |
| Distância [2000,inf) km: taxa de atraso | 0,1208 | 0,1208 | 0,0000 | ok |

As faixas de distância são alinhadas pelos intervalos em km, com limite superior aberto. A participação total do top 10 no pandas é a soma das participações publicadas.

## P4: Calibração por UF

| métrica | Databricks | pandas | diferença | resultado |
| --- | --- | --- | --- | --- |
| AC: pedidos | 80 | 80 | 0 | ok |
| AC: folga mediana (dias) | 22,0000 | 22,0000 | 0,0000 | ok |
| AC: taxa de atraso | 0,0375 | 0,0375 | 0,0000 | ok |
| AL: pedidos | 397 | 397 | 0 | ok |
| AL: folga mediana (dias) | 10,0000 | 10,0000 | 0,0000 | ok |
| AL: taxa de atraso | 0,2141 | 0,2141 | 0,0000 | ok |
| AM: pedidos | 145 | 145 | 0 | ok |
| AM: folga mediana (dias) | 19,0000 | 19,0000 | 0,0000 | ok |
| AM: taxa de atraso | 0,0276 | 0,0276 | 0,0000 | ok |
| AP: pedidos | 67 | 67 | 0 | ok |
| AP: folga mediana (dias) | 22,0000 | 22,0000 | 0,0000 | ok |
| AP: taxa de atraso | 0,0299 | 0,0299 | 0,0000 | ok |
| BA: pedidos | 3.256 | 3.256 | 0 | ok |
| BA: folga mediana (dias) | 12,0000 | 12,0000 | 0,0000 | ok |
| BA: taxa de atraso | 0,1216 | 0,1216 | 0,0000 | ok |
| CE: pedidos | 1.279 | 1.279 | 0 | ok |
| CE: folga mediana (dias) | 13,0000 | 13,0000 | 0,0000 | ok |
| CE: taxa de atraso | 0,1376 | 0,1376 | 0,0000 | ok |
| DF: pedidos | 2.080 | 2.080 | 0 | ok |
| DF: folga mediana (dias) | 13,0000 | 13,0000 | 0,0000 | ok |
| DF: taxa de atraso | 0,0567 | 0,0567 | 0,0000 | ok |
| ES: pedidos | 1.995 | 1.995 | 0 | ok |
| ES: folga mediana (dias) | 12,0000 | 12,0000 | 0,0000 | ok |
| ES: taxa de atraso | 0,1073 | 0,1073 | 0,0000 | ok |
| GO: pedidos | 1.957 | 1.957 | 0 | ok |
| GO: folga mediana (dias) | 13,0000 | 13,0000 | 0,0000 | ok |
| GO: taxa de atraso | 0,0654 | 0,0654 | 0,0000 | ok |
| MA: pedidos | 717 | 717 | 0 | ok |
| MA: folga mediana (dias) | 11,0000 | 11,0000 | 0,0000 | ok |
| MA: taxa de atraso | 0,1743 | 0,1743 | 0,0000 | ok |
| MG: pedidos | 11.354 | 11.354 | 0 | ok |
| MG: folga mediana (dias) | 14,0000 | 14,0000 | 0,0000 | ok |
| MG: taxa de atraso | 0,0457 | 0,0457 | 0,0000 | ok |
| MS: pedidos | 701 | 701 | 0 | ok |
| MS: folga mediana (dias) | 12,0000 | 12,0000 | 0,0000 | ok |
| MS: taxa de atraso | 0,0970 | 0,0970 | 0,0000 | ok |
| MT: pedidos | 886 | 886 | 0 | ok |
| MT: folga mediana (dias) | 15,0000 | 15,0000 | 0,0000 | ok |
| MT: taxa de atraso | 0,0598 | 0,0598 | 0,0000 | ok |
| PA: pedidos | 946 | 946 | 0 | ok |
| PA: folga mediana (dias) | 16,0000 | 16,0000 | 0,0000 | ok |
| PA: taxa de atraso | 0,1121 | 0,1121 | 0,0000 | ok |
| PB: pedidos | 517 | 517 | 0 | ok |
| PB: folga mediana (dias) | 14,0000 | 14,0000 | 0,0000 | ok |
| PB: taxa de atraso | 0,1044 | 0,1044 | 0,0000 | ok |
| PE: pedidos | 1.593 | 1.593 | 0 | ok |
| PE: folga mediana (dias) | 14,0000 | 14,0000 | 0,0000 | ok |
| PE: taxa de atraso | 0,0960 | 0,0960 | 0,0000 | ok |
| PI: pedidos | 476 | 476 | 0 | ok |
| PI: folga mediana (dias) | 14,0000 | 14,0000 | 0,0000 | ok |
| PI: taxa de atraso | 0,1387 | 0,1387 | 0,0000 | ok |
| PR: pedidos | 4.923 | 4.923 | 0 | ok |
| PR: folga mediana (dias) | 14,0000 | 14,0000 | 0,0000 | ok |
| PR: taxa de atraso | 0,0404 | 0,0404 | 0,0000 | ok |
| RJ: pedidos | 12.350 | 12.350 | 0 | ok |
| RJ: folga mediana (dias) | 13,0000 | 13,0000 | 0,0000 | ok |
| RJ: taxa de atraso | 0,1211 | 0,1211 | 0,0000 | ok |
| RN: pedidos | 474 | 474 | 0 | ok |
| RN: folga mediana (dias) | 15,0000 | 15,0000 | 0,0000 | ok |
| RN: taxa de atraso | 0,0928 | 0,0928 | 0,0000 | ok |
| RO: pedidos | 243 | 243 | 0 | ok |
| RO: folga mediana (dias) | 21,0000 | 21,0000 | 0,0000 | ok |
| RO: taxa de atraso | 0,0288 | 0,0288 | 0,0000 | ok |
| RR: pedidos | 41 | 41 | 0 | ok |
| RR: folga mediana (dias) | 20,0000 | 20,0000 | 0,0000 | ok |
| RR: taxa de atraso | 0,1220 | 0,1220 | 0,0000 | ok |
| RS: pedidos | 5.344 | 5.344 | 0 | ok |
| RS: folga mediana (dias) | 15,0000 | 15,0000 | 0,0000 | ok |
| RS: taxa de atraso | 0,0608 | 0,0608 | 0,0000 | ok |
| SC: pedidos | 3.546 | 3.546 | 0 | ok |
| SC: folga mediana (dias) | 13,0000 | 13,0000 | 0,0000 | ok |
| SC: taxa de atraso | 0,0821 | 0,0821 | 0,0000 | ok |
| SE: pedidos | 335 | 335 | 0 | ok |
| SE: folga mediana (dias) | 13,0000 | 13,0000 | 0,0000 | ok |
| SE: taxa de atraso | 0,1522 | 0,1522 | 0,0000 | ok |
| SP: pedidos | 40.494 | 40.494 | 0 | ok |
| SP: folga mediana (dias) | 11,0000 | 11,0000 | 0,0000 | ok |
| SP: taxa de atraso | 0,0449 | 0,0449 | 0,0000 | ok |
| TO: pedidos | 274 | 274 | 0 | ok |
| TO: folga mediana (dias) | 13,0000 | 13,0000 | 0,0000 | ok |
| TO: taxa de atraso | 0,0985 | 0,0985 | 0,0000 | ok |

## P5: Avaliação por faixa de atraso

| métrica | Databricks | pandas | diferença | resultado |
| --- | --- | --- | --- | --- |
| no prazo ou antecipado (<=0): pedidos_com_avaliacao | 89.443 | 89.443 | 0 | ok |
| no prazo ou antecipado (<=0): nota_media | 4,2900 | 4,2904 | 0,0004 | ok |
| no prazo ou antecipado (<=0): taxa_avaliacao_ruim | 0,0927 | 0,0927 | 0,0000 | ok |
| 1-3: pedidos_com_avaliacao | 1.852 | 1.852 | 0 | ok |
| 1-3: nota_media | 3,2900 | 3,2910 | 0,0010 | ok |
| 1-3: taxa_avaliacao_ruim | 0,3213 | 0,3213 | 0,0000 | ok |
| 4-7: pedidos_com_avaliacao | 1.748 | 1.748 | 0 | ok |
| 4-7: nota_media | 2,1000 | 2,1030 | 0,0030 | ok |
| 4-7: taxa_avaliacao_ruim | 0,6768 | 0,6768 | 0,0000 | ok |
| 8-14: pedidos_com_avaliacao | 1.446 | 1.446 | 0 | ok |
| 8-14: nota_media | 1,6700 | 1,6708 | 0,0008 | ok |
| 8-14: taxa_avaliacao_ruim | 0,8015 | 0,8015 | 0,0000 | ok |
| 15+ dias: pedidos_com_avaliacao | 1.335 | 1.335 | 0 | ok |
| 15+ dias: nota_media | 1,7200 | 1,7236 | 0,0036 | ok |
| 15+ dias: taxa_avaliacao_ruim | 0,7835 | 0,7835 | 0,0000 | ok |

## P6: Concentração nos vendedores

| métrica | Databricks | pandas | diferença | resultado |
| --- | --- | --- | --- | --- |
| vendedores_elegiveis | 615 | 615 | 0 | ok |
| postagens_tardias | 6.963 | 6.963 | 0 | ok |
| vendedores_para_80pct | 162 | 162 | 0 | ok |
| fracao_vendedores_80pct | 0,2634 | 0,2634 | 0,0000 | ok |
| Top 10: quantidade de registros | 10 | 10 | 0 | ok |
| Top 1: seller_id | 7c67e1448b00f6e969d365cea6b010ab | 7c67e1448b00f6e969d365cea6b010ab | igual | ok |
| Top 1: pedidos | 963 | 963 | 0 | ok |
| Top 1: postagens_tardias | 288 | 288 | 0 | ok |
| Top 1: taxa_postagem_tardia | 0,2991 | 0,2991 | 0,0000 | ok |
| Top 2: seller_id | 1025f0e2d44d7041d6cf58b6550e0bfa | 1025f0e2d44d7041d6cf58b6550e0bfa | igual | ok |
| Top 2: pedidos | 879 | 879 | 0 | ok |
| Top 2: postagens_tardias | 161 | 161 | 0 | ok |
| Top 2: taxa_postagem_tardia | 0,1832 | 0,1832 | 0,0000 | ok |
| Top 3: seller_id | 4a3ca9315b744ce9f8e9374361493884 | 4a3ca9315b744ce9f8e9374361493884 | igual | ok |
| Top 3: pedidos | 1.673 | 1.673 | 0 | ok |
| Top 3: postagens_tardias | 151 | 151 | 0 | ok |
| Top 3: taxa_postagem_tardia | 0,0903 | 0,0903 | 0,0000 | ok |
| Top 4: seller_id | 1f50f920176fa81dab994f9023523100 | 1f50f920176fa81dab994f9023523100 | igual | ok |
| Top 4: pedidos | 1.364 | 1.364 | 0 | ok |
| Top 4: postagens_tardias | 149 | 149 | 0 | ok |
| Top 4: taxa_postagem_tardia | 0,1092 | 0,1092 | 0,0000 | ok |
| Top 5: seller_id | 3d871de0142ce09b7081e2b9d1733cb1 | 3d871de0142ce09b7081e2b9d1733cb1 | igual | ok |
| Top 5: pedidos | 1.044 | 1.044 | 0 | ok |
| Top 5: postagens_tardias | 137 | 137 | 0 | ok |
| Top 5: taxa_postagem_tardia | 0,1312 | 0,1312 | 0,0000 | ok |
| Top 6: seller_id | 06a2c3af7b3aee5d69171b0e14f0ee87 | 06a2c3af7b3aee5d69171b0e14f0ee87 | igual | ok |
| Top 6: pedidos | 388 | 388 | 0 | ok |
| Top 6: postagens_tardias | 121 | 121 | 0 | ok |
| Top 6: taxa_postagem_tardia | 0,3119 | 0,3119 | 0,0000 | ok |
| Top 7: seller_id | 88460e8ebdecbfecb5f9601833981930 | 88460e8ebdecbfecb5f9601833981930 | igual | ok |
| Top 7: pedidos | 227 | 227 | 0 | ok |
| Top 7: postagens_tardias | 116 | 116 | 0 | ok |
| Top 7: taxa_postagem_tardia | 0,5110 | 0,5110 | 0,0000 | ok |
| Top 8: seller_id | 53e4c6e0f4312d4d2107a8c9cddf45cd | 53e4c6e0f4312d4d2107a8c9cddf45cd | igual | ok |
| Top 8: pedidos | 444 | 444 | 0 | ok |
| Top 8: postagens_tardias | 94 | 94 | 0 | ok |
| Top 8: taxa_postagem_tardia | 0,2117 | 0,2117 | 0,0000 | ok |
| Top 9: seller_id | 8160255418d5aaa7dbdc9f4c64ebda44 | 8160255418d5aaa7dbdc9f4c64ebda44 | igual | ok |
| Top 9: pedidos | 355 | 355 | 0 | ok |
| Top 9: postagens_tardias | 90 | 90 | 0 | ok |
| Top 9: taxa_postagem_tardia | 0,2535 | 0,2535 | 0,0000 | ok |
| Top 10: seller_id | ea8482cd71df3c1969d7b9473ff13abc | ea8482cd71df3c1969d7b9473ff13abc | igual | ok |
| Top 10: pedidos | 1.116 | 1.116 | 0 | ok |
| Top 10: postagens_tardias | 87 | 87 | 0 | ok |
| Top 10: taxa_postagem_tardia | 0,0780 | 0,0780 | 0,0000 | ok |
