"""Referência independente das RN01 a RN19; taxas e participações entre 0 e 1.

RN02 limita somente a série mensal; as demais análises usam todo o universo RN01.
RN07 exclui pedidos somente de P2. Medianas ignoram etapas ausentes, sem imputar.
RN08 compara timestamps; sem limite/postagem, não há evidência de postagem tardia.
RN09 classifica esses atrasos como transporte, conforme o 'caso contrário'.
RN10 dá menor prioridade a timestamp ausente; empate usa o maior review_id.
RN12 inclui as bordas. Distâncias nulas ficam fora das faixas, mas não das rotas.
RN16 não especifica os rótulos ingleses manuais; os adotados não afetam métricas.
Empates nos rankings usam volume e identificador; Pareto ordena por contagem.
"""

import argparse
import json
import os
from pathlib import Path

import numpy as np
import pandas as pd


PADRAO = Path(r"C:\Users\tiago\.cache\kagglehub\datasets\olistbr\brazilian-ecommerce\versions\2")
ARQUIVOS = {nome: f"olist_{nome}_dataset.csv" for nome in (
    "customers", "geolocation", "orders", "order_items", "order_payments",
    "order_reviews", "products", "sellers",
)}
ARQUIVOS["translation"] = "product_category_name_translation.csv"
ETAPAS = {
    "aprovacao": ("order_approved_at", "order_purchase_timestamp"),
    "postagem": ("order_delivered_carrier_date", "order_approved_at"),
    "transporte": ("order_delivered_customer_date", "order_delivered_carrier_date"),
}


def carregar(pasta):
    tipos = {f"{nome}_zip_code_prefix": "str" for nome in (
        "customer", "seller", "geolocation",
    )}
    tabelas = {nome: pd.read_csv(pasta / arquivo, dtype=tipos)
               for nome, arquivo in ARQUIVOS.items()}
    for tabela in tabelas.values():
        for coluna in tabela:
            if coluna.endswith(("_date", "_timestamp")) or coluna == "order_approved_at":
                tabela[coluna] = pd.to_datetime(tabela[coluna], errors="raise")
    return tabelas


def deduplicar_avaliacoes(avaliacoes):
    # RN10: datas ausentes têm menor prioridade; a maior chave vence o empate.
    return avaliacoes.sort_values(
        ["review_answer_timestamp", "review_id"], na_position="first",
    ).drop_duplicates("order_id", keep="last")


def coordenadas(geo):
    validos = (geo.geolocation_lat.between(-33.75, 5.27)
               & geo.geolocation_lng.between(-73.99, -34.79))
    medianas = geo.loc[validos].groupby("geolocation_zip_code_prefix")[
        ["geolocation_lat", "geolocation_lng"]
    ].median()
    return medianas, int((~validos).sum())


def preparar_pedidos(tabelas):
    itens = tabelas["order_items"].groupby("order_id").agg(
        vendedores=("seller_id", "nunique"), seller_id=("seller_id", "first"),
        limite_envio=("shipping_limit_date", "max"),
        valor_pedido=("price", "sum"), frete_pedido=("freight_value", "sum"),
    )  # RN18/RN19: reais nominais; pagamentos permanecem separados.
    pedidos = tabelas["orders"]
    pedidos = pedidos.loc[(pedidos.order_status == "delivered")
                          & pedidos.order_delivered_customer_date.notna()].copy()
    pedidos = pedidos.merge(itens, on="order_id", how="left", validate="1:1")
    pedidos = pedidos.merge(tabelas["customers"], on="customer_id", how="left", validate="m:1")
    pedidos["dias_atraso"] = (pedidos.order_delivered_customer_date.dt.normalize()
                              - pedidos.order_estimated_delivery_date.dt.normalize()).dt.days
    pedidos["atrasado"] = pedidos.dias_atraso > 0
    pedidos["folga_dias"] = -pedidos.dias_atraso
    for etapa, (fim, inicio) in ETAPAS.items():
        pedidos[etapa] = (pedidos[fim] - pedidos[inicio]).dt.total_seconds() / 86400
    pedidos["inconsistencia_temporal"] = pedidos[list(ETAPAS)].lt(0).any(axis=1)
    pedidos["postagem_tardia"] = pedidos.order_delivered_carrier_date > pedidos.limite_envio
    return pedidos


def taxas(pedidos, coluna):
    return pedidos.groupby(coluna, observed=False).agg(
        pedidos=("order_id", "size"), atrasados=("atrasado", "sum"),
        taxa_atraso=("atrasado", "mean"),
    ).reset_index()


def haversine(pedidos, geo):
    pontos = []
    for origem in ("seller", "customer"):
        prefixos = pedidos[f"{origem}_zip_code_prefix"]
        pontos.extend(prefixos.map(geo[coluna]).to_numpy(dtype=float)
                      for coluna in ("geolocation_lat", "geolocation_lng"))
    lat1, lon1, lat2, lon2 = np.radians(pontos)
    arco = (np.sin((lat2 - lat1) / 2) ** 2
            + np.cos(lat1) * np.cos(lat2) * np.sin((lon2 - lon1) / 2) ** 2)
    return 2 * 6371 * np.arcsin(np.sqrt(np.clip(arco, 0, 1)))


def analisar(t):
    p = preparar_pedidos(t)
    geo, descartados = coordenadas(t["geolocation"])
    avaliacoes = deduplicar_avaliacoes(t["order_reviews"])
    traducao = t["translation"].set_index("product_category_name").product_category_name_english.to_dict()
    traducao.update(pc_gamer="gaming_pc",
                    portateis_cozinha_e_preparadores_de_alimentos="portable_kitchen_and_food_preparation_appliances")
    categorias = t["products"].product_category_name
    categorias_ingles = categorias.map(traducao).mask(categorias.isna(), "sem_categoria")
    if categorias_ingles.isna().any():
        raise ValueError("Categoria preenchida sem tradução prevista na RN16")
    resultado = {"contagens": {
        "linhas_por_arquivo": {arquivo: len(t[nome]) for nome, arquivo in ARQUIVOS.items()},
        "pedidos_entregues": len(p),
        "pedidos_com_inconsistencia_temporal": p.inconsistencia_temporal.sum(),
        "pedidos_multivendedor": p.vendedores.gt(1).sum(),
        "avaliacoes_duplicadas_removidas": len(t["order_reviews"]) - len(avaliacoes),
        "pontos_geo_fora_brasil": descartados, "prefixos_com_coordenada": len(geo),
        "produtos_sem_categoria": categorias_ingles.eq("sem_categoria").sum(),
    }}
    p["mes"] = p.order_purchase_timestamp.dt.strftime("%Y-%m")
    meses = pd.period_range("2017-01", "2018-08", freq="M").astype(str)
    mensal = taxas(p, "mes").set_index("mes").reindex(meses)
    mensal["pedidos"] = mensal.pedidos.fillna(0).astype(int)
    resultado["p1_nivel_servico"] = {
        "atrasados": int(p.atrasado.sum()),
        "taxa_atraso_global": p.atrasado.mean(),
        "serie_mensal": mensal.reset_index(names="mes")[["mes", "pedidos", "taxa_atraso"]].to_dict("records"),
    }
    validos = p.loc[~p.inconsistencia_temporal]
    atrasados = validos.loc[validos.atrasado]
    resultado["p2_origem_atraso"] = {
        "pedidos_atrasados_validos": len(atrasados),
        "participacao_vendedor": atrasados.postagem_tardia.mean(),
        "participacao_transporte": (~atrasados.postagem_tardia).mean(),
        "medianas_etapas_dias": {
            nome: validos.loc[validos.atrasado == flag, list(ETAPAS)].median().to_dict()
            for nome, flag in (("atrasados", True), ("no_prazo", False))
        },
    }
    unicos = p.loc[p.vendedores == 1].merge(t["sellers"], on="seller_id", how="left", validate="m:1")
    unicos["rota"] = unicos.seller_state + "->" + unicos.customer_state
    unicos["distancia_km"] = haversine(unicos, geo)
    rotas = taxas(unicos, "rota").query("pedidos >= 300").sort_values(
        ["taxa_atraso", "pedidos", "rota"], ascending=[False, False, True],
    ).head(10)
    total_atrasados = unicos.atrasado.sum()
    rotas["participacao_atrasos"] = rotas.atrasados / total_atrasados if total_atrasados else np.nan
    unicos["faixa_km"] = pd.cut(
        unicos.distancia_km, [0, 100, 500, 1000, 2000, np.inf], right=False,
        labels=["[0,100)", "[100,500)", "[500,1000)", "[1000,2000)", "[2000,inf)"],
    )
    resultado["p3_rotas"] = {
        "top_10": rotas.to_dict("records"),
        "por_distancia": taxas(unicos, "faixa_km").to_dict("records"),
        "pedidos_sem_distancia": unicos.distancia_km.isna().sum(),
    }
    calibracao = p.groupby("customer_state").agg(
        pedidos=("order_id", "size"), mediana_folga_dias=("folga_dias", "median"),
        taxa_atraso=("atrasado", "mean"),
    ).reset_index().rename(columns={"customer_state": "uf"})
    resultado["p4_calibracao"] = calibracao.sort_values(
        ["mediana_folga_dias", "uf"], ascending=[False, True],
    ).to_dict("records")
    notas = p.merge(avaliacoes[["order_id", "review_score"]], on="order_id", how="inner", validate="1:1")
    notas["avaliacao_ruim"] = notas.review_score.isin([1, 2])
    notas["faixa_atraso"] = pd.cut(
        notas.dias_atraso, [-np.inf, 0, 3, 7, 14, np.inf],
        labels=["no prazo ou antecipado (<=0)", "1-3", "4-7", "8-14", "15+ dias"],
    )
    resultado["p5_avaliacao"] = notas.groupby("faixa_atraso", observed=False).agg(
        pedidos_com_avaliacao=("order_id", "size"), nota_media=("review_score", "mean"),
        taxa_avaliacao_ruim=("avaliacao_ruim", "mean"),
    ).reset_index().to_dict("records")
    vendedores = unicos.groupby("seller_id").agg(
        pedidos=("order_id", "size"), postagens_tardias=("postagem_tardia", "sum"),
        taxa_postagem_tardia=("postagem_tardia", "mean"),
    ).reset_index().query("pedidos >= 30").sort_values(
        ["postagens_tardias", "seller_id"], ascending=[False, True],
    )
    total = int(vendedores.postagens_tardias.sum())
    # Corte inteiro: menor prefixo com soma >= 80%, sem arredondar antes do corte.
    quantidade = 0
    if total:
        alvo = (4 * total + 4) // 5
        quantidade = int(vendedores.postagens_tardias.cumsum().searchsorted(alvo) + 1)
    resultado["p6_pareto_vendedores"] = {
        "vendedores_elegiveis": len(vendedores), "total_postagens_tardias": total,
        "vendedores_para_80_porcento": quantidade,
        "participacao_vendedores_para_80_porcento": quantidade / len(vendedores) if len(vendedores) else None,
        "top_10": vendedores.head(10).to_dict("records"),
    }
    return resultado


def normalizar(valor):
    if isinstance(valor, dict):
        return {chave: normalizar(item) for chave, item in valor.items()}
    if isinstance(valor, list):
        return [normalizar(item) for item in valor]
    if isinstance(valor, np.generic):
        valor = valor.item()
    if pd.isna(valor):
        return None
    return round(valor, 4) if isinstance(valor, float) else valor


def main():
    parser = argparse.ArgumentParser(description="Validação independente em pandas das RN01 a RN19")
    parser.add_argument("pasta_raw", nargs="?", type=Path,
                        default=Path(os.environ.get("OLIST_RAW_DIR", PADRAO)))
    args = parser.parse_args()
    resultado = normalizar(analisar(carregar(args.pasta_raw)))
    destino = Path(__file__).resolve().parents[1] / "evidencias" / "referencia_pandas.json"
    destino.parent.mkdir(parents=True, exist_ok=True)
    destino.write_text(json.dumps(resultado, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    contagens, pareto = resultado["contagens"], resultado["p6_pareto_vendedores"]
    print(f"Pedidos entregues: {contagens['pedidos_entregues']}")
    print(f"Taxa global de atraso: {resultado['p1_nivel_servico']['taxa_atraso_global']}")
    print(f"Inconsistências temporais: {contagens['pedidos_com_inconsistencia_temporal']}")
    print(f"Vendedores elegíveis: {pareto['vendedores_elegiveis']}; para 80%: {pareto['vendedores_para_80_porcento']}")
    print(f"Arquivo gravado: {destino}")
    print("Interpretações e lacunas das regras:\n" + __doc__)


if __name__ == "__main__":
    main()
