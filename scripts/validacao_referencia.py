"""Referência independente: catalogo/regras_de_negocio.md, sem consultar notebooks.

Taxas e participações são proporções (0 a 1); dias podem ter fração.
RN02 limita somente a série mensal. Ausência de observações vira null no JSON.
"""

import argparse
import json
import os
from pathlib import Path

import numpy as np
import pandas as pd

RAW_PADRAO = r"C:\Users\tiago\.cache\kagglehub\datasets\olistbr\brazilian-ecommerce\versions\2"
FAIXAS_DISTANCIA = ["[0,100)", "[100,500)", "[500,1000)", "[1000,2000)", "[2000,inf)"]
FAIXAS_ATRASO = ["no prazo ou antecipado (<=0)", "1-3", "4-7", "8-14", "15+ dias"]


def carregar(pasta):
    prefixos = {f"{nome}_zip_code_prefix": "str" for nome in ("customer", "seller", "geolocation")}
    arquivos = {p.name: pd.read_csv(p, dtype=prefixos) for p in sorted(pasta.glob("*.csv"))}
    dados = {nome.removesuffix(".csv").removeprefix("olist_").removesuffix("_dataset"): tabela
             for nome, tabela in arquivos.items()}
    return dados, {nome: len(tabela) for nome, tabela in arquivos.items()}


def preparar(dados):
    pedidos, itens, avaliacoes = (dados[n].copy() for n in ("orders", "order_items", "order_reviews"))
    datas = ["order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date",
             "order_delivered_customer_date", "order_estimated_delivery_date"]
    pedidos[datas] = pedidos[datas].apply(pd.to_datetime)
    itens["shipping_limit_date"] = pd.to_datetime(itens["shipping_limit_date"])
    avaliacoes["review_answer_timestamp"] = pd.to_datetime(avaliacoes["review_answer_timestamp"])
    # RN10: datas ausentes são anteriores às preenchidas; maior identificador desempata.
    unicas = avaliacoes.sort_values(["review_answer_timestamp", "review_id"], na_position="first")
    unicas = unicas.drop_duplicates("order_id", keep="last")
    # RN08/RN18/RN19: agrega itens antes de juntar; pagamentos permanecem separados.
    por_pedido = itens.groupby("order_id").agg(
        vendedores=("seller_id", "nunique"), seller_id=("seller_id", "first"),
        limite_envio=("shipping_limit_date", "max"),
        valor_pedido=("price", "sum"), frete_pedido=("freight_value", "sum"))
    pedidos = pedidos.loc[pedidos["order_status"].eq("delivered")
                          & pedidos["order_delivered_customer_date"].notna()].copy()  # RN01
    pedidos = pedidos.merge(por_pedido, on="order_id", how="left", validate="one_to_one")
    pedidos = pedidos.merge(dados["customers"], on="customer_id", how="left", validate="many_to_one")  # RN03
    pedidos = pedidos.merge(unicas[["order_id", "review_score"]], on="order_id", how="left", validate="one_to_one")
    compra, aprovacao, postagem, entrega, estimada = (pedidos[c] for c in datas)
    pedidos["dias_atraso"] = (entrega.dt.normalize() - estimada.dt.normalize()).dt.days  # RN04
    pedidos["atrasado"] = pedidos["dias_atraso"].gt(0)
    pedidos["folga_dias"] = -pedidos["dias_atraso"]  # RN05
    pedidos["inconsistente"] = (postagem < compra) | (entrega < postagem) | (aprovacao < compra)  # RN07
    for nome, inicio, fim in (("aprovacao", compra, aprovacao), ("postagem", aprovacao, postagem),
                              ("transporte", postagem, entrega)):
        pedidos[nome] = (fim - inicio).dt.total_seconds() / 86400  # RN06: não arredonda antes de agregar.
    pedidos["postagem_tardia"] = postagem.gt(pedidos["limite_envio"])  # RN08: ausências resultam em falso.
    return pedidos, len(avaliacoes) - len(unicas)


def geografia(dados, pedidos):
    geo = dados["geolocation"]
    lat, lng = geo["geolocation_lat"], geo["geolocation_lng"]
    fora = lat.lt(-33.75) | lat.gt(5.27) | lng.lt(-73.99) | lng.gt(-34.79)
    validos = lat.between(-33.75, 5.27) & lng.between(-73.99, -34.79)  # RN12: bordas inclusivas.
    coordenadas = geo.loc[validos].groupby("geolocation_zip_code_prefix")[
        ["geolocation_lat", "geolocation_lng"]].median()  # RN13
    unicos = pedidos.loc[pedidos["vendedores"].eq(1)].copy()  # RN14/RN15
    unicos = unicos.merge(dados["sellers"], on="seller_id", how="left", validate="many_to_one")
    for origem in ("seller", "customer"):
        cep = unicos[f"{origem}_zip_code_prefix"]
        unicos[f"lat_{origem}"] = cep.map(coordenadas["geolocation_lat"])
        unicos[f"lng_{origem}"] = cep.map(coordenadas["geolocation_lng"])
    lat_v, lng_v, lat_c, lng_c = (np.radians(unicos[c]) for c in
                                 ("lat_seller", "lng_seller", "lat_customer", "lng_customer"))
    hav = (np.sin((lat_c - lat_v) / 2) ** 2
           + np.cos(lat_v) * np.cos(lat_c) * np.sin((lng_c - lng_v) / 2) ** 2)
    unicos["distancia_km"] = 2 * 6371 * np.arcsin(np.sqrt(hav.clip(0, 1)))
    unicos["rota"] = unicos["seller_state"] + "->" + unicos["customer_state"]
    unicos["faixa_distancia_km"] = pd.cut(
        unicos["distancia_km"], [0, 100, 500, 1000, 2000, np.inf], labels=FAIXAS_DISTANCIA, right=False)
    return unicos, int(fora.sum()), len(coordenadas)


def taxas(tabela, coluna):
    resumo = tabela.groupby(coluna, observed=False, dropna=False).agg(
        pedidos=("order_id", "size"), atrasados=("atrasado", "sum"), taxa_atraso=("atrasado", "mean"))
    return resumo.reset_index()


def calcular(dados, linhas):
    pedidos, duplicadas = preparar(dados)
    unicos, fora, prefixos = geografia(dados, pedidos)
    contagens = {"linhas_por_arquivo": linhas, "pedidos_entregues": len(pedidos),
                 "pedidos_com_inconsistencia_temporal": int(pedidos["inconsistente"].sum()),
                 "pedidos_multivendedor": int(pedidos["vendedores"].gt(1).sum()),
                 "avaliacoes_duplicadas_removidas": duplicadas, "pontos_geo_fora_brasil": fora,
                 "prefixos_com_coordenada": prefixos,
                 "produtos_sem_categoria": int(dados["products"]["product_category_name"].isna().sum())}
    # RN16: ausência de categoria equivale a sem_categoria; tradução não altera esta contagem.
    pedidos["mes"] = pedidos["order_purchase_timestamp"].dt.strftime("%Y-%m")
    mensal = taxas(pedidos.loc[pedidos["mes"].between("2017-01", "2018-08")], "mes")
    mensal = mensal.set_index("mes").reindex(pd.period_range("2017-01", "2018-08", freq="M").astype(str))
    mensal["pedidos"] = mensal["pedidos"].fillna(0).astype(int)
    mensal = mensal.rename_axis("mes").reset_index()[["mes", "pedidos", "taxa_atraso"]]

    coerentes = pedidos.loc[~pedidos["inconsistente"]].copy()
    tardios = coerentes.loc[coerentes["atrasado"]]
    origem = []
    for nome, flag in (("vendedor", True), ("transporte", False)):
        n = int(tardios["postagem_tardia"].eq(flag).sum())  # RN09, incluindo pedidos multivendedor.
        origem.append({"origem": nome, "pedidos": n,
                       "participacao": n / len(tardios) if len(tardios) else None})
    etapas = []
    for nome, flag in (("atrasados", True), ("no_prazo", False)):
        grupo = coerentes.loc[coerentes["atrasado"].eq(flag)]
        etapas.append({"situacao": nome, "pedidos": len(grupo),
                       "medianas_dias": grupo[["aprovacao", "postagem", "transporte"]].median().to_dict(),
                       "observacoes_por_etapa": grupo[["aprovacao", "postagem", "transporte"]].count().to_dict()})

    rotas = taxas(unicos, "rota").query("pedidos >= 300")
    # Denominador anterior ao filtro de 300 pedidos e ao corte das dez rotas.
    total_atrasados = int(unicos["atrasado"].sum())
    rotas["participacao_atrasados"] = rotas["atrasados"] / total_atrasados if total_atrasados else np.nan
    rotas = rotas.sort_values(["taxa_atraso", "rota"], ascending=[False, True]).head(10)
    distancias = taxas(unicos.loc[unicos["distancia_km"].notna()], "faixa_distancia_km")
    calibracao = taxas(pedidos, "customer_state").rename(columns={"customer_state": "uf"})
    folga = pedidos.groupby("customer_state")["folga_dias"].median()
    calibracao["mediana_folga_dias"] = calibracao["uf"].map(folga)
    calibracao = calibracao.sort_values(["mediana_folga_dias", "uf"], ascending=[False, True])

    avaliados = pedidos.loc[pedidos["review_score"].notna()].copy()  # Ausências excluídas apenas de P5.
    avaliados["faixa_atraso"] = pd.cut(
        avaliados["dias_atraso"], [-np.inf, 0, 3, 7, 14, np.inf], labels=FAIXAS_ATRASO)
    avaliados["avaliacao_ruim"] = avaliados["review_score"].isin([1, 2])  # RN11
    notas = avaliados.groupby("faixa_atraso", observed=False).agg(
        pedidos_com_avaliacao=("order_id", "size"), nota_media=("review_score", "mean"),
        taxa_avaliacao_ruim=("avaliacao_ruim", "mean")).reset_index()

    vendedores = unicos.groupby("seller_id").agg(
        pedidos=("order_id", "size"), postagens_tardias=("postagem_tardia", "sum"),
        taxa_postagem_tardia=("postagem_tardia", "mean")).query("pedidos >= 30").reset_index()  # RN17
    vendedores = vendedores.sort_values(["postagens_tardias", "seller_id"], ascending=[False, True])
    total_postagens = int(vendedores["postagens_tardias"].sum())
    # Inclui o vendedor que cruza 80%; empates são ordenados por seller_id.
    minimo = (int(np.searchsorted(vendedores["postagens_tardias"].cumsum(), total_postagens * 0.8) + 1)
              if total_postagens else 0)
    return {"contagens": contagens,
            "p1_nivel_servico": {"taxa_atraso_global": pedidos["atrasado"].mean(),
                                  "serie_mensal": mensal.to_dict("records")},
            "p2_origem_atraso": {"origem_atraso": origem, "lead_time_por_situacao": etapas},
            "p3_rotas": {"top_10_rotas": rotas.to_dict("records"),
                         "por_faixa_distancia_km": distancias.to_dict("records"),
                         "pedidos_sem_distancia": int(unicos["distancia_km"].isna().sum())},
            "p4_calibracao": calibracao[["uf", "pedidos", "mediana_folga_dias", "taxa_atraso"]].to_dict("records"),
            "p5_avaliacao": notas.to_dict("records"),
            "p6_pareto_vendedores": {"vendedores_elegiveis": len(vendedores),
                                      "total_postagens_tardias": total_postagens,
                                      "vendedores_para_80_pct": minimo,
                                      "participacao_vendedores_para_80_pct": minimo / len(vendedores) if len(vendedores) else None,
                                      "top_10_vendedores": vendedores.head(10).to_dict("records")}}


def arredondar(valor):
    if isinstance(valor, dict):
        return {chave: arredondar(item) for chave, item in valor.items()}
    if isinstance(valor, list):
        return [arredondar(item) for item in valor]
    if isinstance(valor, (float, np.floating)):
        return round(float(valor), 4) if np.isfinite(valor) else None
    if isinstance(valor, np.integer):
        return int(valor)
    return valor


def main():
    parser = argparse.ArgumentParser(description="Calcula a referência pandas a partir dos CSVs brutos Olist.")
    parser.add_argument("pasta", nargs="?", type=Path, default=Path(os.environ.get("OLIST_RAW_DIR") or RAW_PADRAO),
                        help="Pasta dos CSVs (prioridade: argumento, OLIST_RAW_DIR, caminho padrão).")
    args = parser.parse_args()
    if not args.pasta.is_dir():
        parser.error(f"Pasta de CSVs inexistente: {args.pasta}")
    dados, linhas = carregar(args.pasta)
    resultado = arredondar(calcular(dados, linhas))
    destino = Path(__file__).resolve().parents[1] / "evidencias" / "referencia_pandas.json"
    destino.parent.mkdir(parents=True, exist_ok=True)
    destino.write_text(json.dumps(resultado, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    contagens, pareto = resultado["contagens"], resultado["p6_pareto_vendedores"]
    print(f"JSON gravado: {destino}")
    print(f"Pedidos entregues: {contagens['pedidos_entregues']}; "
          f"atraso global: {resultado['p1_nivel_servico']['taxa_atraso_global']:.4f}")
    print(f"Inconsistências temporais: {contagens['pedidos_com_inconsistencia_temporal']}; "
          f"multivendedor: {contagens['pedidos_multivendedor']}")
    print(f"Origem dos atrasos: {resultado['p2_origem_atraso']['origem_atraso']}")
    print(f"Pareto: {pareto['vendedores_para_80_pct']} de {pareto['vendedores_elegiveis']} vendedores; "
          f"{pareto['total_postagens_tardias']} postagens tardias")
    print("Interpretações: RN07 usa só as três condições explícitas; datas ausentes não geram flag; "
          "medianas ignoram ausências por etapa.")
    print("RN08/RN09: comparação sem data não indica postagem tardia; pedido atrasado assim recebe origem transporte.")
    print("RN14/RN15: distância nula fica fora das faixas; participação usa todos os atrasados de vendedor único.")
    print("RN17: Pareto por volume de postagens tardias, incluindo o vendedor que alcança ou supera 80%.")


if __name__ == "__main__":
    main()
