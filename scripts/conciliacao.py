"""Concilia os resumos publicados, sem importar ou executar seus produtores.

Uso: python scripts/conciliacao.py
Contagens ausentes não são estimadas a partir de taxas arredondadas.
"""

import json
import sys
from dataclasses import dataclass, field
from decimal import Decimal
from itertools import zip_longest
from pathlib import Path


RAIZ = Path(__file__).resolve().parents[1]
TOLERANCIAS = {
    "contagem": Decimal(0),
    "taxa": Decimal("0.0002"),
    "media": Decimal("0.06"),
}
DISTANCIAS = {
    "1. até 100 km": "[0,100)",
    "2. 100 a 500 km": "[100,500)",
    "3. 500 a 1.000 km": "[500,1000)",
    "4. 1.000 a 2.000 km": "[1000,2000)",
    "5. 2.000 km ou mais": "[2000,inf)",
}
FAIXAS_ATRASO = {
    "no prazo ou antecipado": "no prazo ou antecipado (<=0)",
    "1 a 3 dias": "1-3",
    "4 a 7 dias": "4-7",
    "8 a 14 dias": "8-14",
    "15 dias ou mais": "15+ dias",
}
PERGUNTAS = {
    "P1": "Nível de serviço",
    "P2": "Origem do atraso",
    "P3": "Rotas e distância",
    "P4": "Calibração por UF",
    "P5": "Avaliação por faixa de atraso",
    "P6": "Concentração nos vendedores",
}


def decimal(valor):
    if isinstance(valor, (int, Decimal)) and not isinstance(valor, bool):
        numero = Decimal(valor)
        if numero.is_finite():
            return numero
    return None


def numero_br(valor, tipo):
    numero = decimal(valor)
    if numero is None:
        return "ausente" if valor is None else str(valor)
    casas = 0 if tipo == "contagem" and numero == numero.to_integral_value() else 4
    return f"{numero:,.{casas}f}".translate(str.maketrans({",": ".", ".": ","}))


def celula(valor):
    return str(valor).replace("|", "&#124;").replace("\n", " ").replace("\r", " ")


@dataclass
class Comparacao:
    pergunta: str
    metrica: str
    databricks: object
    pandas: object
    tipo: str
    motivo: str
    resultado: str = field(init=False, default="DIVERGENTE")
    diferenca: Decimal | None = field(init=False, default=None)

    def __post_init__(self):
        if self.tipo == "texto":
            if isinstance(self.databricks, str) and self.databricks == self.pandas:
                self.resultado = "ok"
            return
        valores = (decimal(self.databricks), decimal(self.pandas))
        if any(v is None for v in valores):
            return
        a, b = valores
        self.diferenca = abs(a - b)
        if self.tipo == "contagem" and any(v < 0 or v != v.to_integral_value() for v in valores):
            self.motivo = "Contagem inválida: exige inteiro não negativo, sem arredondamento."
            return
        if self.tipo == "taxa" and any(not 0 <= v <= 1 for v in valores):
            self.motivo = "Taxa inválida: o valor deve estar entre 0 e 1."
            return
        if self.diferenca <= TOLERANCIAS[self.tipo]:
            self.resultado = "ok"

    def linha(self):
        if self.tipo == "texto":
            diferenca = "igual" if self.resultado == "ok" else "diferente"
        else:
            diferenca = ("não calculável" if self.diferenca is None
                         else numero_br(self.diferenca, self.tipo))
        campos = (self.metrica, numero_br(self.databricks, self.tipo),
                  numero_br(self.pandas, self.tipo), diferenca, self.resultado)
        return "| " + " | ".join(celula(c) for c in campos) + " |"


def indexar(linhas, chave, aliases=None):
    indice = {}
    for linha in linhas:
        nome = linha[chave]
        if aliases:
            nome = aliases.get(nome, nome)
        if nome in indice:
            raise ValueError(f"Chave duplicada em {chave}: {nome}")
        indice[nome] = linha
    return indice


def chaves(a, b):
    # A união evita esconder meses, UFs ou faixas ausentes em apenas uma fonte.
    return dict.fromkeys((*a, *b))


def conciliar(db, ref):
    comparacoes = []
    notas = {p: [] for p in PERGUNTAS}

    def adicionar(pergunta, metrica, a, b, tipo="contagem", motivo=None):
        linha = Comparacao(pergunta, metrica, a, b, tipo, motivo or
                           "Conferir os filtros, a agregação e os campos exportados nas duas fontes.")
        comparacoes.append(linha)
        return linha

    global_db = db["p1"]["global"]
    p1 = ref["p1_nivel_servico"]
    adicionar("P1", "Global: pedidos entregues", global_db.get("pedidos_entregues"),
              ref["contagens"].get("pedidos_entregues"))
    adicionar("P1", "Global: atrasados", global_db.get("atrasados"), p1.get("atrasados"),
              motivo="O JSON pandas não publica a contagem global de atrasados. A taxa com "
              "quatro casas decimais não permite recuperá-la exatamente; não é evidência de erro nos dados.")
    adicionar("P1", "Global: taxa de atraso", global_db.get("taxa_atraso"),
              p1.get("taxa_atraso_global"), "taxa")
    mensal_db = indexar(db["p1"]["serie"], "ano_mes")
    mensal_ref = indexar(p1["serie_mensal"], "mes")
    for mes in sorted(chaves(mensal_db, mensal_ref)):
        a, b = mensal_db.get(mes, {}), mensal_ref.get(mes, {})
        adicionar("P1", f"{mes}: pedidos", a.get("pedidos"), b.get("pedidos"))
        adicionar("P1", f"{mes}: taxa de atraso", a.get("taxa_atraso"), b.get("taxa_atraso"), "taxa")

    p2 = ref["p2_origem_atraso"]
    origens = indexar(db["p2"]["origem"], "origem_atraso")
    etapas = indexar(db["p2"]["etapas_medianas"], "grupo",
                     {"atrasado": "atrasados", "no prazo": "no_prazo"})
    total_ref = decimal(p2.get("pedidos_atrasados_validos"))
    total_etapas = decimal(etapas.get("atrasados", {}).get("pedidos"))
    contagens = [decimal(linha.get("pedidos")) for linha in origens.values()]
    total_origens = sum(contagens, Decimal(0)) if all(v is not None for v in contagens) else None
    adicionar("P2", "Atrasados válidos nas etapas", total_etapas, total_ref)
    linha_total = adicionar("P2", "Atrasados na divisão por origem", total_origens, total_ref,
                           motivo="A base por origem difere da base de atrasados válidos; "
                           "verificar origens nulas e a consistência das participações.")

    # Só explica um déficit pequeno confirmado pelo total das etapas e pelas
    # participações de ambas as origens. Não perdoa qualquer diferença de total.
    faltantes = (total_ref - total_origens
                 if total_ref is not None and total_origens is not None else Decimal(0))
    pode_explicar = (
        total_ref is not None and total_ref == total_etapas and total_ref > 0
        and total_ref == total_ref.to_integral_value()
        and total_origens is not None and total_origens > 0
        and 0 < faltantes / total_ref <= TOLERANCIAS["taxa"]
        and set(origens) == {"vendedor", "transporte"}
        and all(v is not None and v >= 0 and v == v.to_integral_value() for v in contagens)
    )
    ajustadas = {}
    if pode_explicar:
        for origem, linha in origens.items():
            quantidade = decimal(linha.get("pedidos"))
            ajustadas[origem] = (quantidade + (faltantes if origem == "transporte" else 0)) / total_ref
            taxa_db = decimal(linha.get("participacao"))
            taxa_ref = decimal(p2.get(f"participacao_{origem}"))
            if (taxa_db is None or taxa_ref is None or not 0 <= taxa_db <= 1
                    or not 0 <= taxa_ref <= 1
                    or abs(taxa_db - quantidade / total_origens) > TOLERANCIAS["taxa"]
                    or abs(taxa_ref - ajustadas[origem]) > TOLERANCIAS["taxa"]):
                pode_explicar = False

    explicacao = (
        "O Databricks exclui da divisão por origem os atrasados com origem nula "
        "(sem data de entrega à transportadora); o pandas pode classificá-los como transporte."
    )
    notas["P2"].append(explicacao)
    if pode_explicar:
        linha_total.resultado = "diferença explicada"
        linha_total.motivo = (
            f"{numero_br(faltantes, 'contagem')} pedido(s) a menos na divisão por origem. "
            + explicacao
        )
        notas["P2"].append(
            f"Na base comum de {numero_br(total_ref, 'contagem')} atrasados válidos, "
            "as participações calculadas com as contagens Databricks e os casos sem origem "
            f"atribuídos a transporte seriam {numero_br(ajustadas['vendedor'], 'taxa')} "
            f"(vendedor) e {numero_br(ajustadas['transporte'], 'taxa')} (transporte). "
            "Ambas concordam com o pandas dentro da tolerância. A tabela preserva os valores publicados."
        )
    participacoes_ref = {k.removeprefix("participacao_"): v for k, v in p2.items()
                         if k.startswith("participacao_")}
    for origem in chaves(origens, participacoes_ref):
        linha = adicionar("P2", f"Origem {origem}: participação",
                          origens.get(origem, {}).get("participacao"),
                          participacoes_ref.get(origem), "taxa")
        if pode_explicar and linha.resultado == "DIVERGENTE":
            linha.resultado = "diferença explicada"
            linha.motivo = "A participação concorda após usar a base comum. " + explicacao
    medianas_ref = p2["medianas_etapas_dias"]
    for grupo in chaves(etapas, medianas_ref):
        for etapa in ("aprovacao", "postagem", "transporte"):
            adicionar("P2", f"{grupo.replace('_', ' ')}: mediana de {etapa} (dias)",
                      etapas.get(grupo, {}).get(etapa), medianas_ref.get(grupo, {}).get(etapa), "media")

    def ranking(pergunta, linhas_db, linhas_ref, identificador, campos):
        adicionar(pergunta, "Top 10: quantidade de registros", len(linhas_db), len(linhas_ref))
        # Inclui todas as posições; uma lista menor que dez também gera ausências.
        for posicao, (a, b) in enumerate(zip_longest(linhas_db, linhas_ref), 1):
            a, b = a or {}, b or {}
            prefixo = f"Top {posicao}"
            adicionar(pergunta, f"{prefixo}: {identificador}", a.get(identificador),
                      b.get(identificador), "texto", "Identificador ou posição diferente no ranking; "
                      "conferir elegibilidade e critérios de desempate.")
            for campo, tipo in campos:
                adicionar(pergunta, f"{prefixo}: {campo}", a.get(campo), b.get(campo), tipo)
        for posicao in range(max(len(linhas_db), len(linhas_ref)) + 1, 11):
            adicionar(pergunta, f"Top {posicao}: {identificador}", None, None, "texto",
                      "Posição ausente nas duas listas de top 10.")

    p3 = ref["p3_rotas"]
    ranking("P3", db["p3"]["top10"], p3["top_10"], "rota",
            (("pedidos", "contagem"), ("atrasados", "contagem"),
             ("taxa_atraso", "taxa"), ("participacao_atrasos", "taxa")))
    participacoes = [decimal(r.get("participacao_atrasos")) for r in p3["top_10"]]
    soma_participacoes = (sum(participacoes, Decimal(0))
                          if all(v is not None for v in participacoes) else None)
    adicionar("P3", "Top 10: participação total nos atrasos", db["p3"].get("participacao_top10"),
              soma_participacoes, "taxa")
    distancia_db = indexar(db["p3"]["distancia"], "faixa_distancia", DISTANCIAS)
    distancia_ref = indexar(p3["por_distancia"], "faixa_km")
    notas["P3"].append(
        "As faixas de distância são alinhadas pelos intervalos em km, com limite superior aberto. "
        "A participação total do top 10 no pandas é a soma das participações publicadas."
    )
    for faixa in chaves(distancia_db, distancia_ref):
        a, b = distancia_db.get(faixa, {}), distancia_ref.get(faixa, {})
        adicionar("P3", f"Distância {faixa} km: pedidos", a.get("pedidos"), b.get("pedidos"),
                  motivo="Causa provável: notebooks/03_modelo_gold.py arredonda distancia_km "
                  "para uma casa antes de formar as faixas; o pandas usa a distância sem arredondar. "
                  "Pedidos perto dos limites podem mudar de faixa. A contagem exige igualdade exata.")
        adicionar("P3", f"Distância {faixa} km: taxa de atraso", a.get("taxa_atraso"),
                  b.get("taxa_atraso"), "taxa")

    uf_db, uf_ref = indexar(db["p4"], "uf"), indexar(ref["p4_calibracao"], "uf")
    for uf in sorted(chaves(uf_db, uf_ref)):
        a, b = uf_db.get(uf, {}), uf_ref.get(uf, {})
        adicionar("P4", f"{uf}: pedidos", a.get("pedidos"), b.get("pedidos"))
        adicionar("P4", f"{uf}: folga mediana (dias)", a.get("folga_mediana"),
                  b.get("mediana_folga_dias"), "media")
        adicionar("P4", f"{uf}: taxa de atraso", a.get("taxa_atraso"), b.get("taxa_atraso"), "taxa")

    notas_db = indexar(db["p5"], "faixa_atraso", FAIXAS_ATRASO)
    notas_ref = indexar(ref["p5_avaliacao"], "faixa_atraso")
    for faixa in chaves(notas_db, notas_ref):
        a, b = notas_db.get(faixa, {}), notas_ref.get(faixa, {})
        for campo, tipo in (("pedidos_com_avaliacao", "contagem"),
                            ("nota_media", "media"), ("taxa_avaliacao_ruim", "taxa")):
            adicionar("P5", f"{faixa}: {campo}", a.get(campo), b.get(campo), tipo)

    p6_db, p6_ref = db["p6"], ref["p6_pareto_vendedores"]
    for campo_db, campo_ref, tipo in (
        ("vendedores_elegiveis", "vendedores_elegiveis", "contagem"),
        ("postagens_tardias", "total_postagens_tardias", "contagem"),
        ("vendedores_para_80pct", "vendedores_para_80_porcento", "contagem"),
        ("fracao_vendedores_80pct", "participacao_vendedores_para_80_porcento", "taxa"),
    ):
        adicionar("P6", campo_db, p6_db.get(campo_db), p6_ref.get(campo_ref), tipo)
    ranking("P6", p6_db["top10"], p6_ref["top_10"], "seller_id",
            (("pedidos", "contagem"), ("postagens_tardias", "contagem"),
             ("taxa_postagem_tardia", "taxa")))
    return comparacoes, notas


def resumo(comparacoes):
    return (f"{sum(c.resultado == 'ok' for c in comparacoes)} de "
            f"{len(comparacoes)} comparações dentro da tolerância")


def relatorio(comparacoes, notas):
    explicadas = sum(c.resultado == "diferença explicada" for c in comparacoes)
    divergentes = sum(c.resultado == "DIVERGENTE" for c in comparacoes)
    linhas = [
        "# Conciliação Databricks e pandas", "",
        "Compara evidencias/resultado_databricks.json, produzido pelo notebook 05 sobre a gold, "
        "com evidencias/referencia_pandas.json, calculado dos CSVs brutos. "
        "Contagens exigem igualdade exata; taxas e participações entre 0 e 1 admitem diferença "
        "absoluta de até 0,0002; medianas em dias e médias, até 0,06. "
        "A tolerância contempla arredondamentos Spark HALF_UP e numpy half-even. "
        "A coluna diferença mostra valores absolutos. Ausências são DIVERGENTE.", "",
        resumo(comparacoes), "",
        f"Diferenças explicadas: {explicadas}. DIVERGENTE: {divergentes}. "
        "Somente linhas com resultado ok entram no numerador do resumo.", "",
    ]
    for pergunta, titulo in PERGUNTAS.items():
        linhas.extend([f"## {pergunta}: {titulo}", "",
                       "| métrica | Databricks | pandas | diferença | resultado |",
                       "| --- | --- | --- | --- | --- |"])
        grupo = [c for c in comparacoes if c.pergunta == pergunta]
        linhas.extend(c.linha() for c in grupo)
        linhas.append("")
        for nota in notas[pergunta]:
            linhas.extend([nota, ""])
        for c in grupo:
            if c.resultado != "ok":
                linhas.extend([f"- {c.resultado}, {c.metrica}: {c.motivo}", ""])
    return "\n".join(linhas)


def rejeitar_constante(valor):
    raise ValueError(f"Constante numérica inválida no JSON: {valor}")


def main():
    try:
        fontes = []
        for nome in ("resultado_databricks.json", "referencia_pandas.json"):
            with (RAIZ / "evidencias" / nome).open(encoding="utf-8-sig") as arquivo:
                fontes.append(json.load(arquivo, parse_float=Decimal,
                                        parse_constant=rejeitar_constante))
        comparacoes, notas = conciliar(*fontes)
        destino = RAIZ / "evidencias" / "conciliacao.md"
        destino.write_text(relatorio(comparacoes, notas), encoding="utf-8")
    except (OSError, ValueError, KeyError, TypeError) as erro:
        print(f"Falha na conciliação: {erro}", file=sys.stderr)
        return 1
    print(resumo(comparacoes))
    for c in comparacoes:
        if c.resultado == "DIVERGENTE":
            print(f"DIVERGENTE: {c.pergunta}, {c.metrica}: "
                  f"Databricks={numero_br(c.databricks, c.tipo)}; "
                  f"pandas={numero_br(c.pandas, c.tipo)}. {c.motivo}")
    return int(any(c.resultado == "DIVERGENTE" for c in comparacoes))


if __name__ == "__main__":
    sys.exit(main())
