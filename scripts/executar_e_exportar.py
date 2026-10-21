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
    saida = subprocess.run([DB, *args, "-p", perfil, "-o", "json"], capture_output=True, text=True, check=True).stdout
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
        repo = next(r for r in cli("repos", "list", perfil=a.perfil) if r["path"] == REPO)
        cli("repos", "update", str(repo["id"]), "--branch", "main", perfil=a.perfil)
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


if __name__ == "__main__":
    main()
