# CineData Analytics — Pipeline de Dados (Bronze → Silver → Gold)

Pipeline ETL end-to-end em **Databricks (PySpark + SQL + Delta)** seguindo a **Arquitetura Medalhão**, sobre uma base de filmes TMDB/IMDb. Projeto de Engenharia de Dados do Rocket Lab 2026.2 (Visagio / CESAR School).

O produto final é um Star Schema na camada Gold, usado para consultas de negócio, para contexto de GenAI/RAG e pelo agente Text-to-SQL (ver `cinedata-agent/README.md`).

---

## 1. Visão geral

```text
Volume (CSVs + API BCB)
        │
        ▼
 Landing_to_Bronze   dados brutos, sem alterar conteúdo; + ingestion_datetime
        │
        ▼
 Bronze_to_Silver    limpeza, tipagem, deduplicação, normalização
        │
        ▼
 Silver_to_Gold      Star Schema (fato + dimensões + bridges) + contexto GenAI
```

Orquestração: um Databricks Workflow (`job/job.yaml`) com três tarefas encadeadas.

## 2. Estrutura do repositório

```text
.
├── notebooks/
│   ├── Landing_to_Bronze.ipynb
│   ├── Bronze_to_Silver.ipynb
│   └── Silver_to_Gold.ipynb
├── job/
│   └── job.yaml                       # Workflow do Databricks
├── cotacao_json/
│   └── cotacao_dolar_manual.json      # fallback da cotação do dólar
└── cinedata-agent/                    # agente Text-to-SQL (GenAI)
```

## 3. Pré-requisitos e configuração

- Workspace Databricks com Unity Catalog
- Catálogo **`meu_catalog`** e Volume **`/Volumes/meu_catalog/default/inputs`**

Arquivos que devem estar no Volume `inputs`:

| Arquivo | Tabela Bronze |
|---|---|
| `movies_info_TMDB_IMDB.csv` | `tb_movies_info` |
| `movies_financials_IMDB_TMDB.csv` | `tb_movies_financials` |
| `movies_metrics_IMDB_TMDB.csv` | `tb_movies_metrics` |
| `credits_and_tags_IMDB_TMDB.csv` | `tb_credits_and_tags` |
| `movies_reviews.csv` | `tb_movies_reviews` |
| `cotacao_dolar_manual.json` | fallback para `tb_cotacao_dolar` |

> O nome do arquivo de info é `movies_info_TMDB_IMDB.csv` (TMDB antes de IMDB); os demais usam `IMDB_TMDB`.

**Para usar outro catálogo ou volume**, edite a célula de configuração no início de cada notebook (`catalog`, `schema`, `volume`). O nome `meu_catalog` está fixo no código, inclusive em alguns comandos SQL da Bronze (`meu_catalog.bronze.tb_cotacao_dolar`), então faça uma busca por `meu_catalog` e troque em todos.

## 4. Como executar

**Manual:** importe os três notebooks no workspace e execute no databricks, nesta ordem: `Landing_to_Bronze` → `Bronze_to_Silver` → `Silver_to_Gold`.

**Via Workflow:** crie o Job a partir de `job/job.yaml` (ajuste os `notebook_path` para o seu workspace). Resumo do Job:

| Tarefa | Notebook | Depende de |
|---|---|---|
| `to_Bronze` | `Landing_to_Bronze` | — |
| `to_Silver` | `Bronze_to_Silver` | `to_Bronze` |
| `to_Gold` | `Silver_to_Gold` | `to_Silver` |

Agendamento: diário às **02:00:38**, fuso `America/Recife`. Fila habilitada, `PERFORMANCE_OPTIMIZED`.

## 5. Camada Bronze (`Landing_to_Bronze`)

Princípio: **guardar o dado como veio**. A única coluna adicionada é `ingestion_datetime`.

- CSVs lidos sem inferência de tipos (tudo string).
- `tb_movies_metrics` e `tb_credits_and_tags` têm aspas não fechadas em alguns registros (texto de sinopse/tagline "vaza" para outras colunas, o *Column Shift*). Por isso são lidos com schema explícito em `PERMISSIVE` e coluna `_corrupt_record`, sem derrubar o pipeline. A correção é responsabilidade da Silver.
- **Reset controlado:** o notebook faz `DROP TABLE IF EXISTS` das seis tabelas Bronze antes da carga, para não misturar execuções anteriores. Isso significa que a Bronze **é recriada a cada execução** (não acumula histórico).
- **Cotação do dólar:** consulta a API PTAX do Banco Central (Olinda) para o intervalo definido pelos widgets `data_inicio` e `data_fim` (formato `MM-DD-AAAA`; padrão: últimos 7 dias). Se a API estiver inacessível por rede/timeout, usa o fallback `cotacao_dolar_manual.json`. Erros HTTP (4xx/5xx) **não** caem no fallback: falham de forma visível.

## 6. Camada Silver (`Bronze_to_Silver`)

Funções auxiliares comuns: `normalizar_texto`, `numero_limpo` (aceita `R$`, `USD`, `1.234,56`, `1,234.56`, sufixos `k`/`M`, e trata "unknown", "n/a", vazio como NULL), `inteiro_seguro`, `popularidade_limpa`, `adicionar_data_multiformato` (ISO, `d/M/yyyy`, `M-d-yyyy`), `entidade_valida`, `completude` e `hash_deterministico`.

Regras por tabela:

| Tabela Silver | Regras principais |
|---|---|
| `tb_info_filmes` | Um registro por filme. Em duplicidade, vence a ingestão mais recente, depois o registro mais completo, depois desempate por hash. Status traduzido para PT (`Lançado`, `Pós-Produção`, `Em Produção`, `Planejado`, `Rumores`, `Cancelado`, `Não Informado`). Datas multiformato; `ano_lancamento` derivado |
| `tb_financeiro_filmes` | Orçamento/receita ≤ 0 ou inválidos viram NULL. Conversão para BRL com a **cotação mais recente** da Silver. Lucro só quando há receita e orçamento. Margem percentual sem divisão por zero |
| `tb_metricas_engajamento` | Notas fora de 0–10 e contagens/popularidade negativas viram NULL. Popularidades inteiras entre 1888 e 2035 em linhas deslocadas são descartadas (formato de ano, resíduo de Column Shift) |
| `tb_avaliacoes_usuarios` | Deduplicação integral; notas fora de 0–10 viram NULL; comentário vazio vira `Sem comentário` |
| `tb_generos` | Separadores (`; , \|`) normalizados e gêneros explodidos. Só são aceitos os **19 gêneros válidos** do domínio (descarta resíduos deslocados) |
| `tb_pessoas_empresas` | Atores, diretores, roteiristas e produtoras em uma tabela com `tipo_entidade`. Nomes validados (descarta números, caminhos de imagem, frases longas), `initcap`, e `ordem_entidade` = posição original na lista |
| `tb_cotacao_dolar` | Série diária contínua entre a primeira e a última cotação, com *forward fill* para fins de semana e feriados; uma cotação por dia (a mais recente) |

Ao final há validações de volume, schema, duplicidade por chave, integridade referencial entre info/financeiro/métricas e sanidade dos domínios (gêneros distintos ≤ 19, popularidade máxima plausível, ausência de popularidades em forma de ano).

## 7. Camada Gold (`Silver_to_Gold`)

Star Schema com chaves substitutas geradas por `xxhash64`.

| Tabela | Grão / conteúdo |
|---|---|
| `dim_movies` | 1 linha por filme (todos os status): título, datas, duração, idioma, status, sinopse |
| `fact_movies_performance` | 1 linha por **filme lançado** (status `Lançado`, `data_lancamento` não nula e ≤ hoje): orçamento/receita/lucro em USD e BRL, popularidade, notas e votos TMDB e IMDb |
| `dim_genres` / `bridge_movie_genre` | Catálogo único de gêneros e relação N:N com filmes |
| `dim_people` / `bridge_movie_person` | Atores, diretores e roteiristas (`tipo_pessoa`) e relação N:N |
| `dim_companies` / `bridge_movie_company` | Produtoras e relação N:N |
| `dim_reviews` | Agregado por filme das avaliações de usuários: `qtd_avaliacoes_usuarios`, `nota_media_usuarios` |
| `gold_genai_movies_context` | Um texto corrido por filme (`llm_context_document`) para RAG, com fallbacks para campos nulos |

Cuidados de modelagem:

- Relações N:N passam por bridges para não multiplicar o grão da fato. Nas consultas, use `COUNT(DISTINCT sk_movie_id)` ou agregue antes de juntar.
- Um teste final falha o notebook se a fato tiver mais de uma linha por filme.
- Como a fato só tem filmes lançados e a `dim_movies` tem todos, consultas que partem da fato (ex.: receita total) cobrem apenas lançados.

### Perguntas de negócio respondidas (seção "Desafio")

1. Receita total em BRL
2. Top 5 filmes por popularidade
3. Quantidade de filmes por gênero
4. Top 10 por receita, com `RANK()`
5. Ator com mais participações nos últimos 2 anos
6. Produtora com maior lucro nos últimos 5 anos

Em 5 e 6, a janela é calculada a partir da **maior data de lançamento** de filmes lançados (não da data de hoje).

## 8. Decisões e limitações conhecidas

- **Cotação única.** Todos os valores em BRL usam a cotação mais recente disponível, não a cotação da data de lançamento do filme.
- **Fallback da cotação.** O JSON contém apenas cotações de 10 a 17/09/2026 e o notebook filtra o fallback pelo intervalo dos widgets. Se a API do BCB estiver inacessível (por exemplo, o Databricks não resolver o DNS) e o intervalo não cruzar essas datas, o notebook falha com "Nenhuma cotação para ..." de forma intencional. Em execuções agendadas depois dessa janela, isso só funciona se a API responder, ou se o JSON for atualizado, ou se os widgets apontarem para o período coberto.
- **Column Shift não é reconstruído.** Valores deslocados são descartados ou anulados, nunca "consertados" automaticamente.
- **Bronze recriada a cada execução** (ver seção 5): não há histórico de ingestões.
- **Catálogo fixo** `meu_catalog` (ver seção 3).
- **Dedup na Gold.** `dedup_por_filme` mantém uma linha por `id_filme` sem critério de qualidade; a escolha real do melhor registro já foi feita na Silver.

## 9. Entregáveis

- Notebooks `.ipynb` das três camadas
- `job.yaml` do Databricks Workflow
- Print da execução bem-sucedida do Job
- Repositório GitHub público
