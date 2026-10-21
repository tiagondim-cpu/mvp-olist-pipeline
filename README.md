# MVP · Onde o prazo de entrega da Olist falha e quanto isso custa em satisfação

Pipeline de dados em nuvem (Databricks Free Edition, arquitetura medalhão com Delta Lake e Unity Catalog) que coleta, modela, carrega e analisa 99.441 pedidos do marketplace Olist para responder seis perguntas de nível de serviço logístico.

| Campo | Informação |
|---|---|
| Estudante | Tiago Andre Gondim |
| Matrícula | 231013476 |
| Instituição | Universidade de Brasília, Departamento de Engenharia de Produção |
| Professor | André Luiz Marques Serrano |
| Trabalho | MVP (*minimum viable product*), pipeline de dados em nuvem, individual |
| Plataforma | Databricks Free Edition (Spark, Delta Lake, Unity Catalog, Lakeflow Jobs) |

> **Resultados:** em execução na plataforma. Esta seção será preenchida com os números obtidos no Databricks e conciliados com a validação independente.

## 1. Objetivo

### Problema

Um marketplace promete ao cliente uma data de entrega no momento da compra. Quando a promessa falha, o cliente avalia mal e o marketplace perde reputação, mesmo sem controlar diretamente quem posta (o vendedor) nem quem transporta (a transportadora). A operação logística precisa saber **onde agir primeiro**: cobrar os vendedores que postam tarde, renegociar ou trocar transportadoras em rotas críticas, ou recalibrar a data prometida.

Este MVP é a menor versão de um **painel de priorização logística** para o gestor de operações do marketplace. Ele precisa mostrar, com dados, em que etapa, em que rota e em quais vendedores o prazo falha, e quanto essa falha pesa na avaliação do cliente.

### Hipótese a testar

> **H1.** A maior parte dos atrasos nasce na postagem tardia do vendedor e se concentra em poucos vendedores. Se verdadeira, um programa dirigido a esses vendedores é a alavanca de maior retorno.
>
> **Métrica que confirma ou refuta:** participação dos pedidos atrasados em que o vendedor postou depois do prazo-limite de envio (P2) e concentração das postagens tardias nos vendedores (P6). H1 se sustenta se mais da metade dos atrasos tiver origem no vendedor.

### Perguntas de negócio

| Nº | Pergunta | Decisão que apoia |
|---|---|---|
| P1 | Qual a proporção de pedidos entregues depois da data prometida, como ela evoluiu mês a mês entre janeiro de 2017 e agosto de 2018 e em quais meses houve picos? | Dimensionar o problema e planejar capacidade para os meses críticos |
| P2 | Nos pedidos atrasados, o atraso nasce na postagem do vendedor (postagem depois do prazo-limite de envio) ou no transporte? Qual etapa do lead time mais se alonga nos pedidos atrasados? | Escolher entre agir sobre vendedores ou sobre transportadoras |
| P3 | Quais rotas (UF do vendedor para UF do cliente, com pelo menos 300 pedidos) têm maior taxa de atraso e quanto do total de atrasos elas concentram? Como a distância se relaciona com o atraso? | Priorizar rotas para renegociação de frete e troca de transportadora |
| P4 | O prazo prometido está calibrado por UF de destino? Em quais UFs a promessa é folgada demais e em quais é violada com frequência? | Recalibrar a data prometida por região |
| P5 | Quanto a nota média e a proporção de avaliações ruins (1 ou 2) pioram por faixa de dias de atraso? | Definir gatilho de contato proativo e compensação |
| P6 | Entre vendedores com pelo menos 30 pedidos entregues, qual fração deles responde por 80% das postagens tardias? | Desenhar um programa de desempenho de vendedores |

### Viabilidade: as três dimensões do MVP

| Dimensão | Evidência esperada neste MVP |
|---|---|
| Técnica | Pipeline executado ponta a ponta no Databricks, com tabelas Delta persistidas e job orquestrado |
| Financeira | Execução no plano gratuito; custo e tempo de execução medidos e estimados para um plano pago (seção 7) |
| Desejabilidade | As seis perguntas respondidas com número, discussão e decisão sugerida (seção 6) |

## 2. Dados

| Item | Valor |
|---|---|
| Conjunto | [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce), versão 2 |
| Conteúdo | 99.441 pedidos de setembro/2016 a outubro/2018 em nove arquivos CSV relacionados |
| Volume | 126 MB, 1.550.922 linhas no total |
| Licença | CC BY-NC-SA 4.0: uso não comercial, com atribuição, derivados pela mesma licença |
| Dados pessoais | Anonimizados na origem: clientes e vendedores aparecem como códigos de 32 caracteres e CEP com 5 dígitos |
| Data da coleta | registrada pelo pipeline na tabela `olist_bronze.registro_coleta` |

Os dados brutos não estão versionados aqui. O notebook 01 baixa os arquivos direto da fonte com o pacote oficial `kagglehub`, fixando a versão 2 para que qualquer reexecução leia os mesmos dados.

## 3. Arquitetura do pipeline

```text
Kaggle (9 CSV) ──► Volume UC ──► BRONZE ──────► SILVER ───────────► GOLD ─────────────────► Análise
                  arquivos      9 tabelas      tipagem, limpeza,    modelo constelação:    SQL + gráficos
                  originais     Delta, tudo    deduplicação,        3 fatos, 4 dimensões   P1 a P6
                                como texto     regras RN01 a RN19   + tabelas de qualidade
   01_coleta_bronze             02_transformacao_silver   03_modelo_gold   04_qualidade_dados   05_analise_perguntas
```

Os cinco notebooks rodam em sequência como um *job* do Lakeflow Jobs (`scripts/job_pipeline_olist.json`). Cada notebook termina com verificações que interrompem o job se algo sair do esperado: contagens da bronze iguais às dos arquivos, chaves únicas na silver, catálogo idêntico ao modelo na gold e integridade referencial na qualidade.

## 4. Modelagem

Esquema constelação: três tabelas fato compartilham quatro dimensões conformadas. O diagrama e a justificativa de cada grão estão em [`catalogo/modelo_dados.md`](catalogo/modelo_dados.md).

O **Catálogo de Dados** completo (75 atributos, com tipo, descrição, domínio mínimo e máximo ou categorias válidas, obrigatoriedade, significado do nulo e linhagem) está em [`catalogo/catalogo_dados.csv`](catalogo/catalogo_dados.csv) e em versão legível em [`catalogo/catalogo_dados.md`](catalogo/catalogo_dados.md). O mesmo arquivo é lido pelo notebook 03, que grava cada descrição como comentário de coluna no Unity Catalog, e pelo notebook 04, que confere cada coluna contra o domínio declarado.

## 5. Transformação e carga

As 19 regras de negócio aplicadas na transformação, como conversões de data, deduplicação, conciliação de geolocalização e atribuição da origem do atraso, estão em [`catalogo/regras_de_negocio.md`](catalogo/regras_de_negocio.md), cada uma com o motivo e o notebook que a aplica.

## 6. Resultados

*A preencher com a execução no Databricks.*

## 7. Autoavaliação

*A preencher ao final.*

## 8. Como reproduzir

1. Crie uma conta gratuita no [Databricks Free Edition](https://www.databricks.com/learn/free-edition).
2. Em **Workspace > Create > Git folder**, informe a URL deste repositório.
3. Abra `notebooks/01_coleta_bronze` e execute; depois 02, 03, 04 e 05, nesta ordem. Alternativamente, crie o job descrito em `scripts/job_pipeline_olist.json`.
4. O parâmetro `catalogo` (padrão `workspace`) define o catálogo de destino.

## Estrutura do repositório

```text
README.md                         este documento
LICENSE                           licença do código (MIT) e registro da licença dos dados
notebooks/                        cinco notebooks Databricks, em ordem de execução
scripts/                          definição do job e validação independente em pandas
catalogo/                         catálogo de dados, regras de negócio e modelo
evidencias/                       capturas de tela da execução na nuvem
```
