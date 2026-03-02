# Passo a passo – Implementação no Databricks

**Projeto:** API MXM – Pipeline de dados (Forge → Bronze → Silver → Gold)  
**Última atualização:** 02/03/2025

Este documento descreve a sequência de passos para implementar o pipeline no **Databricks**, desde a configuração do ambiente até a execução dos jobs de ingestão e transformação.

---

## Pré-requisitos

- Workspace Databricks (Azure, AWS ou GCP)
- Acesso à API MXM-WebManager (credenciais: UserName, Password, EnvironmentName; opcional: client_id/client_secret para token OAuth2)
- Repositório do projeto versionado (ex.: Git) e integrado ao Databricks Repos (recomendado) ou importação manual dos notebooks/scripts

---

## 1. Configuração do ambiente

### 1.1 Catálogo e schemas (Unity Catalog)

Se usar **Unity Catalog**, crie o catálogo e os schemas por camada:

```sql
-- Opcional: criar catálogo (ou usar o default do workspace)
CREATE CATALOG IF NOT EXISTS mxm_pipeline;

-- Schemas por camada (Forge, Bronze, Silver, Gold)
CREATE SCHEMA IF NOT EXISTS mxm_pipeline.forge;
CREATE SCHEMA IF NOT EXISTS mxm_pipeline.bronze;
CREATE SCHEMA IF NOT EXISTS mxm_pipeline.silver;
CREATE SCHEMA IF NOT EXISTS mxm_pipeline.gold;
```

Se **não** usar Unity Catalog, utilize o Hive Metastore e databases no mesmo padrão (ex.: `forge`, `bronze`, `silver`, `gold`).

### 1.2 Mount ou volume para paths (opcional)

Se os dados forem persistidos em **cloud storage** (ADLS, S3, GCS), configure um mount ou use Volumes:

**Exemplo (Azure ADLS):**

```python
# Configurar mount (executar uma vez no cluster)
configs = {"fs.azure.account.auth.type": "OAuth", ...}  # preencher conforme doc Databricks
dbutils.fs.mount(source="abfss://container@storage.dfs.core.windows.net/mxm", mount_point="/mnt/mxm", extra_configs=configs)
```

Paths do projeto ficam, por exemplo:

- `/mnt/mxm/QVDForge`
- `/mnt/mxm/QVDBronze`
- `/mnt/mxm/QVDSilver`
- `/mnt/mxm/QVDGold`

Atualize `config/paths_and_prefixes.yaml` (ou variáveis de ambiente) com o `base_path` usado nos jobs.

### 1.3 Variáveis de ambiente / Secret Scope (credenciais API MXM)

Guarde as credenciais da API MXM em **Databricks Secret Scope** (recomendado):

1. Criar Secret Scope (ex.: `mxm-api`).
2. Incluir secrets: `username`, `password`, `environment-name`; se usar OAuth2: `client-id`, `client-secret`.
3. Nos notebooks, acessar com `dbutils.secrets.get(scope="mxm-api", key="username")` (e equivalentes).

Não commitar usuário/senha no repositório.

---

## 2. Camada Forge (logs)

### 2.1 Tabela de logs

Crie a tabela de logs do pipeline no schema Forge (prefixo `frg_fin_`):

```sql
CREATE TABLE IF NOT EXISTS mxm_pipeline.forge.frg_fin_pipeline_log (
  log_id STRING,
  run_id STRING,
  job_name STRING,
  layer STRING,
  table_name STRING,
  status STRING,
  rows_processed LONG,
  started_at TIMESTAMP,
  finished_at TIMESTAMP,
  error_message STRING
)
USING DELTA
LOCATION '/mnt/mxm/QVDForge/frg_fin_pipeline_log';  -- ajustar path conforme mount
```

### 2.2 Registrar logs nos jobs

Nos notebooks de ingestão (raw_to_bronze) e transformação (bronze_to_silver), ao iniciar e ao finalizar, insira registros nessa tabela (ex.: com Spark SQL ou append em Delta). Use isso para monitoramento e rastreabilidade.

---

## 3. Camada Bronze (ingestão direta – prefixo brz_t_fin_)

### 3.1 Repositório e notebook executor

1. Conectar o repositório do projeto ao workspace (Databricks Repos) ou fazer upload da pasta `src/framework/raw_to_bronze`.
2. Abrir o notebook `raw_to_bronze_whl_executor_util.ipynb`.
3. Ajustar no notebook:
   - Caminho base dos configs (ex.: `Repos/<repo>/src/framework/raw_to_bronze/financeiro/api_mxm/ingestion_configs`).
   - Leitura das credenciais (Secret Scope) e base URL da API MXM.

### 3.2 Criar tabelas Bronze por entidade

Para cada entidade (ex.: clientes, títulos a receber), crie a tabela Bronze com prefixo **brz_t_fin_** e colunas de auditoria:

```sql
-- Exemplo: brz_t_fin_clientes
CREATE TABLE IF NOT EXISTS mxm_pipeline.bronze.brz_t_fin_clientes (
  -- colunas do payload da API (ajustar conforme swagger/response)
  id STRING,
  nome STRING,
  cpf_cnpj STRING,
  -- auditoria
  _ingested_at TIMESTAMP,
  _source_endpoint STRING,
  _process_id STRING
)
USING DELTA
LOCATION '/mnt/mxm/QVDBronze/brz_t_fin_clientes';
```

Repita o padrão para outras tabelas (ex.: `brz_t_fin_titulos_receber`), alinhado aos arquivos em `ddl/schema_updates/` e `ingestion_configs/*.json`.

### 3.3 Lógica de ingestão no notebook

1. Obter token (se OAuth2 estiver habilitado) ou montar o body com `AuthenticationToken` (UserName, Password, EnvironmentName).
2. Para cada arquivo em `ingestion_configs/*.json`:
   - Ler `api_endpoint`, `table_name` (ex.: `brz_t_fin_clientes`).
   - Chamar a API (POST com JSON).
   - Inserir resposta (ou lista de registros) em DataFrame; adicionar `_ingested_at`, `_source_endpoint`, `_process_id`.
   - Gravar em Delta na tabela Bronze correspondente (append ou merge, conforme estratégia).
3. Registrar início/fim na tabela Forge `frg_fin_pipeline_log`.

### 3.4 Job Databricks (ingestão Bronze)

1. Em **Workflows** → **Create Job**.
2. Adicionar tarefa do tipo **Notebook**: apontar para `raw_to_bronze_whl_executor_util.ipynb`.
3. Configurar cluster (ex.: job cluster pequeno para API).
4. Agendar (ex.: diário) ou disparar por trigger.
5. Definir permissões (acesso ao Secret Scope, mount e catálogo/schemas).

---

## 4. Camada Silver (prefixo slv_fin_ – sem _t_)

### 4.1 Tabelas Silver

Crie as tabelas Silver (prefixo **slv_fin_**) a partir do schema desejado (tipos padronizados, sem colunas de auditoria da Bronze):

```sql
CREATE TABLE IF NOT EXISTS mxm_pipeline.silver.slv_fin_clientes (
  id STRING,
  nome STRING,
  cpf_cnpj STRING,
  data_atualizacao TIMESTAMP
)
USING DELTA
LOCATION '/mnt/mxm/QVDSilver/slv_fin_clientes';
```

### 4.2 Notebook Bronze → Silver

1. Abrir `bronze_to_silver_whl_executor_util.ipynb`.
2. Para cada config em `ingestion_configs/` (ex.: `financeiro_clientes.json`):
   - Ler `source_table` (ex.: `brz_t_fin_clientes`) e `table_name` (ex.: `slv_fin_clientes`).
   - Ler a tabela Bronze; aplicar limpeza, deduplicação, conversão de tipos.
   - Escrever na tabela Silver (overwrite ou merge, conforme regra).
3. Registrar execução em `frg_fin_pipeline_log`.

### 4.3 Job Databricks (Bronze → Silver)

1. Criar novo Job com tarefa apontando para `bronze_to_silver_whl_executor_util.ipynb`.
2. Definir dependência do job de ingestão Bronze (opcional): executar Silver após Bronze concluir.
3. Agendar ou encadear via trigger.

---

## 5. Camada Gold (prefixo gld_fin_ – sem _t_)

### 5.1 Tabelas Gold

Crie tabelas Gold para produtos de negócio (agregações, KPIs), por exemplo:

```sql
CREATE TABLE IF NOT EXISTS mxm_pipeline.gold.gld_fin_kpi_vendas (
  data_ref DATE,
  receita_total DOUBLE,
  quantidade_vendas LONG
)
USING DELTA
LOCATION '/mnt/mxm/QVDGold/gld_fin_kpi_vendas';
```

### 5.2 Notebook ou job Gold

Implemente um notebook (ex.: `silver_to_gold` ou scripts SQL) que leia das tabelas `slv_fin_*`, aplique agregações e grave nas tabelas `gld_fin_*`. Inclua no mesmo padrão de jobs e logs (Forge).

---

## 6. Resumo da nomenclatura

| Camada | Prefixo    | Exemplo de tabela        |
|--------|------------|---------------------------|
| Forge  | `frg_fin_` | `frg_fin_pipeline_log`   |
| Bronze | `brz_t_fin_` | `brz_t_fin_clientes`   |
| Silver | `slv_fin_` | `slv_fin_clientes`       |
| Gold   | `gld_fin_` | `gld_fin_kpi_vendas`     |

**Regra:** Apenas Bronze utiliza **_t_** no prefixo.

---

## 7. Ordem sugerida de implementação

1. **Ambiente:** Catálogo/schemas, mount (se aplicável), Secret Scope com credenciais API.
2. **Forge:** Criar tabela de logs e chamadas de registro nos notebooks.
3. **Bronze:** Criar uma tabela piloto (ex.: `brz_t_fin_clientes`); implementar ingestão no notebook; testar; criar job.
4. **Silver:** Criar tabela piloto `slv_fin_clientes`; implementar transformação no notebook; testar; criar job encadeado.
5. **Gold:** Definir um KPI; criar tabela e notebook/job.
6. **CI/CD (opcional):** Configurar pipeline Git com build/test e deploy de notebooks e jobs (ex.: Databricks Asset Bundles ou Jobs API).

---

## 8. Referências no repositório

- Arquitetura e convenções: [docs/ARQUITETURA_PIPELINE_DADOS.md](./ARQUITETURA_PIPELINE_DADOS.md)
- API MXM: [docs/API_MXM_DOCUMENTACAO.md](./API_MXM_DOCUMENTACAO.md) e [docs/swagger.json](./swagger.json)
- Paths e prefixos: [config/paths_and_prefixes.yaml](../config/paths_and_prefixes.yaml) e [src/config/paths.py](../src/config/paths.py)
