# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Respostas às perguntas de negócio
# MAGIC
# MAGIC Todas as respostas são consultas SQL sobre a gold. Cada seção traz a consulta, a tabela de resultado e, quando ajuda a leitura, um gráfico. A interpretação dos números está no README, na seção de resultados.
# MAGIC
# MAGIC Universo padrão: pedidos entregues (RN01). Nas análises de etapas do lead time saem os pedidos com inconsistência temporal (RN07); nas de rota e vendedor, só entram pedidos de vendedor único (RN14, RN15, RN17).

# COMMAND ----------

import json
import matplotlib.pyplot as plt

dbutils.widgets.text("catalogo", "workspace", "Catálogo do Unity Catalog")
CATALOGO = dbutils.widgets.get("catalogo")
spark.sql(f"USE {CATALOGO}.olist_gold")
q = lambda sql: spark.sql(sql).toPandas()
resumo = {}

plt.rcParams.update({"figure.dpi": 120, "axes.spines.top": False, "axes.spines.right": False,
                     "axes.titlesize": 11, "axes.titleweight": "bold", "font.size": 9})
AZUL, CINZA, LARANJA = "#1f5f99", "#9aa5b1", "#d9822b"

# COMMAND ----------

# MAGIC %md
# MAGIC ## P1 · Nível de serviço
# MAGIC Qual a proporção de pedidos entregues depois da data prometida, como ela evoluiu mês a mês entre janeiro de 2017 e agosto de 2018 e em quais meses houve picos?

# COMMAND ----------

global_ = q("""SELECT count(*) AS pedidos_entregues, sum(int(flag_atrasado)) AS atrasados,
                      round(avg(int(flag_atrasado)), 4) AS taxa_atraso
               FROM fato_pedido WHERE flag_entregue""")
mensal = q("""SELECT d.ano_mes, count(*) AS pedidos, sum(int(f.flag_atrasado)) AS atrasados,
                     round(avg(int(f.flag_atrasado)), 4) AS taxa_atraso,
                     round(avg(f.dias_prometidos), 1) AS prazo_prometido_medio
              FROM fato_pedido f JOIN dim_data d ON d.data_key = f.data_compra_key
              WHERE f.flag_entregue AND d.ano_mes BETWEEN '2017-01' AND '2018-08'
              GROUP BY d.ano_mes ORDER BY d.ano_mes""")
display(global_); display(mensal)
resumo["p1"] = {"global": global_.iloc[0].to_dict(), "serie": mensal.to_dict("records")}

fig, ax = plt.subplots(figsize=(9, 3.4))
ax.bar(mensal.ano_mes, mensal.taxa_atraso * 100, color=[LARANJA if t > 0.10 else AZUL for t in mensal.taxa_atraso])
ax.axhline(global_.taxa_atraso[0] * 100, color=CINZA, ls="--", lw=1)
ax.text(len(mensal) - 0.5, global_.taxa_atraso[0] * 100 + 0.4, f"média {global_.taxa_atraso[0]:.1%}", ha="right", color="#555")
ax.set_ylabel("% entregue depois do prometido"); ax.set_title("P1 · Taxa de atraso por mês da compra (laranja: acima de 10%)")
ax.tick_params(axis="x", rotation=60); plt.tight_layout(); plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## P2 · Origem do atraso
# MAGIC Nos pedidos atrasados, o atraso nasce na postagem do vendedor ou no transporte? Qual etapa do lead time mais se alonga nos pedidos atrasados?

# COMMAND ----------

origem = q("""SELECT origem_atraso, count(*) AS pedidos,
                     round(count(*) / sum(count(*)) OVER (), 4) AS participacao
              FROM fato_pedido
              WHERE flag_entregue AND flag_atrasado AND NOT flag_inconsistencia_temporal AND origem_atraso IS NOT NULL
              GROUP BY origem_atraso ORDER BY pedidos DESC""")
etapas = q("""SELECT CASE WHEN flag_atrasado THEN 'atrasado' ELSE 'no prazo' END AS grupo, count(*) AS pedidos,
                     round(median(dias_aprovacao), 2) AS aprovacao, round(median(dias_postagem), 2) AS postagem,
                     round(median(dias_transporte), 2) AS transporte, round(median(dias_lead_time), 2) AS lead_time,
                     round(median(dias_prometidos), 1) AS prometido,
                     round(avg(int(flag_postagem_tardia)), 4) AS taxa_postagem_tardia
              FROM fato_pedido WHERE flag_entregue AND NOT flag_inconsistencia_temporal
              GROUP BY 1 ORDER BY 1""")
display(origem); display(etapas)
resumo["p2"] = {"origem": origem.to_dict("records"), "etapas_medianas": etapas.to_dict("records")}

fig, ax = plt.subplots(figsize=(8, 2.6))
for i, g in enumerate(etapas.itertuples()):
    esquerda = 0
    for etapa, cor in [("aprovacao", CINZA), ("postagem", LARANJA), ("transporte", AZUL)]:
        v = getattr(g, etapa)
        ax.barh(g.grupo, v, left=esquerda, color=cor, label=etapa if i == 0 else None)
        if v > 1: ax.text(esquerda + v / 2, i, f"{v:.1f}", va="center", ha="center", color="white", fontweight="bold")
        esquerda += v
ax.set_xlabel("dias (mediana de cada etapa)"); ax.legend(frameon=False, ncol=3, loc="lower right")
ax.set_title("P2 · Etapas do lead time: o transporte é a etapa que mais se alonga"); plt.tight_layout(); plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## P3 · Concentração geográfica
# MAGIC Quais rotas (UF do vendedor para UF do cliente, com pelo menos 300 pedidos) têm maior taxa de atraso e quanto do total de atrasos elas concentram? Como a distância se relaciona com o atraso?

# COMMAND ----------

rotas = q("""WITH base AS (
               SELECT concat(v.uf, '->', c.uf) AS rota, f.flag_atrasado
               FROM fato_pedido f JOIN dim_vendedor v USING (seller_id) JOIN dim_cliente c USING (customer_id)
               WHERE f.flag_entregue)
             SELECT rota, count(*) AS pedidos, sum(int(flag_atrasado)) AS atrasados,
                    round(avg(int(flag_atrasado)), 4) AS taxa_atraso,
                    round(sum(int(flag_atrasado)) / (SELECT sum(int(flag_atrasado)) FROM base), 4) AS participacao_atrasos
             FROM base GROUP BY rota HAVING count(*) >= 300 ORDER BY taxa_atraso DESC""")
distancia = q("""SELECT CASE WHEN distancia_km < 100 THEN '1. até 100 km' WHEN distancia_km < 500 THEN '2. 100 a 500 km'
                             WHEN distancia_km < 1000 THEN '3. 500 a 1.000 km' WHEN distancia_km < 2000 THEN '4. 1.000 a 2.000 km'
                             ELSE '5. 2.000 km ou mais' END AS faixa_distancia,
                        count(*) AS pedidos, round(avg(int(flag_atrasado)), 4) AS taxa_atraso,
                        round(median(dias_transporte), 1) AS transporte_mediano, round(median(dias_prometidos), 1) AS prometido_mediano
                 FROM fato_pedido WHERE flag_entregue AND distancia_km IS NOT NULL
                 GROUP BY 1 ORDER BY 1""")
display(rotas.head(10)); display(distancia)
top10 = rotas.head(10)
resumo["p3"] = {"rotas_elegiveis": len(rotas), "top10": top10.to_dict("records"),
                "participacao_top10": round(float(top10.participacao_atrasos.sum()), 4),
                "distancia": distancia.to_dict("records")}

fig, ax = plt.subplots(figsize=(8, 3.6))
ax.barh(top10.rota[::-1], top10.taxa_atraso[::-1] * 100, color=AZUL)
for y, (t, n) in enumerate(zip(top10.taxa_atraso[::-1], top10.pedidos[::-1])):
    ax.text(t * 100 + 0.3, y, f"{t:.1%} de {n:,} pedidos".replace(",", "."), va="center", fontsize=8)
ax.axvline(global_.taxa_atraso[0] * 100, color=CINZA, ls="--", lw=1)
ax.set_xlabel("% entregue depois do prometido"); ax.set_title("P3 · Dez rotas com maior taxa de atraso (mínimo de 300 pedidos)")
plt.tight_layout(); plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## P4 · Calibração da promessa
# MAGIC O prazo prometido está calibrado por UF de destino? Em quais UFs a promessa é folgada demais e em quais ela é violada com frequência?

# COMMAND ----------

calibracao = q("""SELECT c.uf, c.regiao, count(*) AS pedidos,
                         round(median(f.dias_prometidos), 1) AS prazo_prometido_mediano,
                         round(median(f.dias_lead_time), 1) AS lead_time_mediano,
                         round(median(f.folga_dias), 1) AS folga_mediana,
                         round(avg(int(f.flag_atrasado)), 4) AS taxa_atraso
                  FROM fato_pedido f JOIN dim_cliente c USING (customer_id)
                  WHERE f.flag_entregue GROUP BY c.uf, c.regiao ORDER BY folga_mediana DESC""")
display(calibracao)
resumo["p4"] = calibracao.to_dict("records")

fig, ax = plt.subplots(figsize=(7, 4.4))
ax.scatter(calibracao.folga_mediana, calibracao.taxa_atraso * 100, s=calibracao.pedidos / 60 + 15, color=AZUL, alpha=.7)
for r in calibracao.itertuples():
    ax.annotate(r.uf, (r.folga_mediana, r.taxa_atraso * 100), fontsize=8, xytext=(3, 3), textcoords="offset points")
ax.set_xlabel("folga mediana (dias entregues antes do prometido)"); ax.set_ylabel("% entregue depois do prometido")
ax.set_title("P4 · Folga alta e atraso alto convivem: a promessa não está calibrada"); plt.tight_layout(); plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## P5 · Impacto no cliente
# MAGIC Quanto a nota média e a proporção de avaliações ruins (1 ou 2) pioram por faixa de dias de atraso?

# COMMAND ----------

avaliacao = q("""SELECT faixa_atraso, count(*) AS pedidos_com_avaliacao, round(avg(nota_avaliacao), 2) AS nota_media,
                        round(avg(int(flag_avaliacao_ruim)), 4) AS taxa_avaliacao_ruim
                 FROM fato_pedido WHERE flag_entregue AND nota_avaliacao IS NOT NULL
                 GROUP BY faixa_atraso
                 ORDER BY CASE faixa_atraso WHEN 'no prazo ou antecipado' THEN 0 WHEN '1 a 3 dias' THEN 1
                          WHEN '4 a 7 dias' THEN 2 WHEN '8 a 14 dias' THEN 3 ELSE 4 END""")
display(avaliacao)
resumo["p5"] = avaliacao.to_dict("records")

fig, ax = plt.subplots(figsize=(7, 3))
barras = ax.bar(avaliacao.faixa_atraso, avaliacao.taxa_avaliacao_ruim * 100,
                color=[AZUL] + [LARANJA] * (len(avaliacao) - 1))
ax.bar_label(barras, labels=[f"{t:.0%}\nnota {n:.1f}" for t, n in zip(avaliacao.taxa_avaliacao_ruim, avaliacao.nota_media)], fontsize=8)
ax.set_ylabel("% de avaliações 1 ou 2"); ax.set_ylim(0, 100)
ax.set_title("P5 · Avaliação ruim por faixa de atraso"); plt.tight_layout(); plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## P6 · Concentração nos vendedores
# MAGIC Entre vendedores com pelo menos 30 pedidos entregues, qual fração deles responde por 80% das postagens tardias?

# COMMAND ----------

vendedores = q("""SELECT seller_id, count(*) AS pedidos, sum(int(flag_postagem_tardia)) AS postagens_tardias,
                         round(avg(int(flag_postagem_tardia)), 4) AS taxa_postagem_tardia,
                         round(avg(int(flag_atrasado)), 4) AS taxa_atraso
                  FROM fato_pedido WHERE flag_entregue AND seller_id IS NOT NULL
                  GROUP BY seller_id HAVING count(*) >= 30
                  ORDER BY postagens_tardias DESC, pedidos DESC""")
vendedores["acumulado"] = vendedores.postagens_tardias.cumsum() / vendedores.postagens_tardias.sum()
n80 = int((vendedores.acumulado < 0.8).sum()) + 1
display(vendedores.head(10))
resumo["p6"] = {"vendedores_elegiveis": len(vendedores), "postagens_tardias": int(vendedores.postagens_tardias.sum()),
                "vendedores_para_80pct": n80, "fracao_vendedores_80pct": round(n80 / len(vendedores), 4),
                "top10": vendedores.head(10).drop(columns="acumulado").to_dict("records")}
print(f"{n80} de {len(vendedores)} vendedores ({n80 / len(vendedores):.1%}) respondem por 80% das postagens tardias")

fig, ax = plt.subplots(figsize=(7, 3))
x = (vendedores.index + 1) / len(vendedores) * 100
ax.plot(x, vendedores.acumulado * 100, color=AZUL)
ax.axhline(80, color=CINZA, ls="--", lw=1); ax.axvline(n80 / len(vendedores) * 100, color=LARANJA, ls="--", lw=1)
ax.set_xlabel("% dos vendedores (ordenados por postagens tardias)"); ax.set_ylabel("% acumulado das postagens tardias")
ax.set_title("P6 · Curva de Pareto das postagens tardias"); plt.tight_layout(); plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo para conciliação
# MAGIC O JSON abaixo é comparado com o resultado do script independente `scripts/validacao_referencia.py`, escrito em pandas sem reaproveitar este código.

# COMMAND ----------

print(json.dumps(resumo, ensure_ascii=False, default=str, indent=1))
