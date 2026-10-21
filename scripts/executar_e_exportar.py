"""Executa o job do pipeline no Databricks e baixa as evidências da execução.

Uso: python scripts/executar_e_exportar.py [--perfil olist] [--sem-executar]

Requer a Databricks CLI autenticada (databricks auth login --profile olist). O script:
1. atualiza o Git folder do workspace com o último commit do GitHub;
2. dispara o job mvp_olist_pipeline e espera o fim;
3. exporta cada notebook executado, com saídas, em evidencias/execucao_databricks/*.html;
4. baixa os gráficos e o resumo gerados no volume olist_gold.graficos.
"""
import argparse, base64, json, re, shutil, subprocess, time, urllib.parse
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
EVID = RAIZ / "evidencias"
DB = shutil.which("databricks") or "databricks"
JOB, REPO = "mvp_olist_pipeline", "/Workspace/Users/tiagondim@gmail.com/mvp-olist-pipeline"


def cli(*args, perfil):
    saida = subprocess.run([DB, *args, "-p", perfil, "-o", "json"], capture_output=True, text=True, encoding="utf-8", check=True).stdout
    return json.loads(saida) if saida.strip() else {}


def celulas_sem_saida(html):
    modelo = re.search(r"__DATABRICKS_NOTEBOOK_MODEL = '([^']+)'", html).group(1)
    modelo = json.loads(urllib.parse.unquote(base64.b64decode(modelo).decode()))
    return [i for i, c in enumerate(modelo["commands"]) if not c["command"].startswith("%md") and not c.get("results")]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--perfil", default="olist")
    ap.add_argument("--sem-executar", action="store_true", help="só exporta a última execução")
    a = ap.parse_args()

    job_id = next(j["job_id"] for j in cli("jobs", "list", perfil=a.perfil) if j["settings"]["name"] == JOB)
    if a.sem_executar:
        run = cli("jobs", "list-runs", "--job-id", str(job_id), "--limit", "1", perfil=a.perfil)[0]
        run = cli("jobs", "get-run", str(run["run_id"]), perfil=a.perfil)
    else:
        repo_id = cli("workspace", "get-status", REPO, perfil=a.perfil)["object_id"]
        cli("repos", "update", str(repo_id), "--branch", "main", perfil=a.perfil)
        run = cli("jobs", "run-now", str(job_id), "--timeout", "60m", perfil=a.perfil)
    print("Execução", run["run_id"], run["state"].get("result_state"), run.get("run_page_url"))

    pasta = EVID / "execucao_databricks"
    pasta.mkdir(parents=True, exist_ok=True)
    for t in sorted(run["tasks"], key=lambda t: t["task_key"]):
        # As saídas são gravadas de forma assíncrona; repete a exportação até virem todas
        for tentativa in range(10):
            html = cli("jobs", "export-run", str(t["run_id"]), "--views-to-export", "CODE", perfil=a.perfil)["views"][0]["content"]
            faltam = celulas_sem_saida(html)
            if not faltam:
                break
            time.sleep(15)
        (pasta / f"{t['task_key']}.html").write_text(html, encoding="utf-8")
        duracao = round(t.get("execution_duration", 0) / 1000)
        print(f"  {t['task_key']}: {t['state'].get('result_state')}, {duracao} s, células sem saída: {faltam or 'nenhuma'}")

    graficos = EVID / "graficos"
    subprocess.run([DB, "fs", "cp", "-r", "--overwrite", "dbfs:/Volumes/workspace/olist_gold/graficos", str(graficos),
                    "-p", a.perfil], check=True, capture_output=True)
    (graficos / "resultado_databricks.json").replace(EVID / "resultado_databricks.json")
    print("Gráficos e resumo baixados em", EVID)
    registrar_persistencia(a.perfil)


def sql(consulta, perfil, warehouse):
    corpo = json.dumps({"warehouse_id": warehouse, "statement": consulta, "wait_timeout": "50s"})
    r = subprocess.run([DB, "api", "post", "/api/2.0/sql/statements", "--json", corpo, "-p", perfil],
                       capture_output=True, text=True, encoding="utf-8", check=True)
    r = json.loads(r.stdout)
    assert r["status"]["state"] == "SUCCEEDED", r["status"]
    cols = [c["name"] for c in r["manifest"]["schema"]["columns"]]
    return cols, r.get("result", {}).get("data_array", [])


def registrar_persistencia(perfil):
    """Consulta o Unity Catalog e grava a prova de que as tabelas estão persistidas na nuvem."""
    warehouse = cli("warehouses", "list", perfil=perfil)[0]["id"]
    _, tabelas = sql("""SELECT table_schema, table_name, data_source_format, created, comment
                        FROM workspace.information_schema.tables
                        WHERE table_schema LIKE 'olist_%' ORDER BY table_schema, table_name""", perfil, warehouse)
    contagens = " UNION ALL ".join(f"SELECT '{s}.{t}' AS tabela, count(*) AS linhas FROM workspace.{s}.{t}"
                                   for s, t, *_ in tabelas)
    linhas = dict(sql(contagens, perfil, warehouse)[1])
    md = ["# Persistência na nuvem", "",
          "Tabelas Delta no Unity Catalog do workspace Databricks Free Edition (catálogo `workspace`), "
          "consultadas pela API de SQL depois da execução do job. Gerado por `scripts/executar_e_exportar.py`.", "",
          "| Esquema | Tabela | Formato | Linhas | Criada em (UTC) | Descrição |", "|---|---|---|---:|---|---|"]
    for s, t, fmt, criada, comentario in tabelas:
        n = f"{int(linhas[f'{s}.{t}']):,}".replace(",", ".")
        md.append(f"| {s} | {t} | {fmt} | {n} | {criada[:19].replace('T', ' ')} | {comentario or ''} |")
    (EVID / "persistencia_nuvem.md").write_text("\n".join(md) + "\n", encoding="utf-8")
    print(f"Persistência registrada: {len(tabelas)} tabelas")


if __name__ == "__main__":
    main()
