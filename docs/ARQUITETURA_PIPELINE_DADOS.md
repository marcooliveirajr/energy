# Documento de Arquitetura de Pipeline de Dados

**Projeto:** API MXM – Ingestão e Arquitetura de Dados (Databricks)  
**Padrão:** Enterprise Data Product Standards (EDPS) – WDiscovery  
**Última atualização:** 02/03/2025

---

## 1. Introdução

Este documento adapta os padrões **Enterprise Data Product Standards (EDPS)** definidos pela WDiscovery ao projeto de pipeline de dados que consome a **API MXM-WebManager**, ingere dados no **Databricks** e os disponibiliza em camadas Bronze, Silver e Gold. A arquitetura segue princípios **FAIR** (Findable, Accessible, Interoperable, Reusable) e adota a **medallion architecture** como padrão fundamental.

### 1.1 Objetivos do Documento

- Definir arquitetura padronizada para o pipeline baseada em camadas **Bronze**, **Silver** e **Gold**
- Estabelecer padrões de interoperabilidade entre as camadas
- Promover governança orientada por metadados
- Suportar automação e integração **CI/CD**
- Preparar para especificações legíveis por máquina (descritores de produto)

---

## 2. Convenções de Nomenclatura e Paths

### 2.1 Paths por Camada

| Camada   | Variável   | Path (exemplo) | Uso no projeto                          |
|----------|------------|----------------|-----------------------------------------|
| Arquivo  | `ARQPath`  | `/Arquivo`     | Arquivos de apoio, configs, referência  |
| Forge    | `FRGPath`  | `/QVDForge`    | **Logs** (rastreio, auditoria do pipeline) |
| Bronze   | `TBRZPath` | `/QVDBronze`   | **Dados brutos** – destino direto da ingestão (não há camada Raw) |
| Silver   | `SLRPath`  | `/QVDSilver`   | Dados limpos e enriquecidos             |
| Gold     | `GLDPath`  | `/QVDGold`     | Dados prontos para negócio              |

No **Databricks**, esses paths podem ser mapeados para:

- **DBFS:** `dbfs:/mnt/<mount>/QVDForge`, `dbfs:/mnt/.../QVDBronze`, etc., ou
- **Unity Catalog:** `catalog.schema` com convenção de schemas `forge`, `bronze`, `silver`, `gold` e tabelas com prefixos abaixo.

### 2.2 Prefixos de Tabelas/Objetos

| Camada | Prefixo      | Exemplo de tabela        | Descrição                    |
|--------|---------------|---------------------------|------------------------------|
| Forge  | `frg_fin_`    | `frg_fin_pipeline_log` | Logs do pipeline             |
| Bronze | `brz_t_fin_`  | `brz_t_fin_clientes`   | Dados brutos – **única camada com _t_** |
| Silver | `slv_fin_`    | `slv_fin_clientes`     | Entidades limpas e conformadas|
| Gold   | `gld_fin_`    | `gld_fin_kpi_vendas`   | Produtos para negócio        |

**Regra do _t_:** Apenas a camada **Bronze** usa **_t_** no prefixo (`brz_t_fin_`). Forge, Silver e Gold não usam _t_. O sufixo `_fin_` indica o domínio (ex.: financeiro).

### 2.3 Resumo para Implementação

```text
ARQPath   = '/Arquivo'
FRGPath   = '/QVDForge'    # Forge: logs
TBRZPath  = '/QVDBronze'   # Bronze: dados brutos (destino direto da ingestão)
SLRPath   = '/QVDSilver'   # Camada Silver
GLDPath   = '/QVDGold'     # Camada Gold

ForgePrefixo  = 'frg_fin_'
BronzePrefixo = 'brz_t_fin_'   # única camada com _t_ (tabela)
SilverPrefixo = 'slv_fin_'
GoldPrefixo   = 'gld_fin_'
```

---

## 3. Princípios de Design para Produtos de Dados

Todo produto de dados desenvolvido no projeto **DEVE** seguir estes princípios:

| Princípio     | Descrição |
|---------------|-----------|
| **Findable**  | O produto DEVE ser descoberto via catálogo com identificador único e metadados ricos |
| **Accessible**| Dados e metadados DEVEM ser recuperáveis usando protocolos padrão com controles de autenticação |
| **Interoperable** | Schema, estrutura e semântica DEVEM estar alinhados com padrões compartilhados |
| **Reusable**  | O produto DEVE incluir metadados contextuais, linhagem e anotações de qualidade |
| **Observable**| Produtos DEVEM emitir sinais (métricas, logs) para monitoramento de freshness, SLA e anomalias |
| **Explainable** | Transformações DEVEM ser transparentes e rastreáveis com linhagem de dados |
| **Governed**  | Políticas de governança DEVEM ser codificadas e aplicadas |
| **Composable**| O produto DEVE ser modular e integrável via interfaces consistentes |
| **Versioned** | Schema, dados e metadados DEVEM ter controle de versão |
| **Validated** | Produtos DEVEM passar por verificações automatizadas de qualidade |
| **Self-Describing** | O produto DEVE incluir arquivo descritor válido (ex.: dataproduct.yaml) |

---

## 4. Arquitetura Medallion Adaptada

O pipeline é **Forge → Bronze → Silver → Gold**. **Não há camada Raw** (dados brutos vão direto para Bronze).

```text
┌─────────────────┐
│  INGESTÃO (API) │ → Dados da API MXM-WebManager
└────────┬────────┘
         ▼
┌─────────────────┐
│  CAMADA FORGE   │ → Logs (rastreio, auditoria do pipeline)
│  (QVDForge)     │    Prefixo: frg_fin_
└────────┬────────┘
         ▼
┌─────────────────┐
│  CAMADA BRONZE  │ → Dados brutos – destino direto da ingestão
│  (QVDBronze)    │    Prefixo: brz_t_fin_ (única camada com _t_)
└────────┬────────┘
         ▼
┌─────────────────┐
│  CAMADA SILVER  │ → Dados limpos e enriquecidos
│  (QVDSilver)    │    Prefixo: slv_fin_
└────────┬────────┘
         ▼
┌─────────────────┐
│   CAMADA GOLD   │ → Dados prontos para negócio
│  (QVDGold)      │    Prefixo: gld_fin_
└─────────────────┘
```

### 4.1 Camada Forge (`FRGPath` / `QVDForge`)

**Propósito:** Armazenar **logs** do pipeline (execuções, erros, métricas). Prefixo: `frg_fin_`.

### 4.2 Camada Bronze (`TBRZPath` / `QVDBronze`)

**Propósito:** Receber os dados brutos **diretamente** da ingestão (não há camada Raw). Preservar dados fonte em forma original, com rastreabilidade, auditabilidade e imutabilidade.

| Tipo de Ativo                         | Obrigatoriedade | Descrição |
|---------------------------------------|-----------------|-----------|
| Arquivos Fonte de Ingestão            | REQUERIDO       | Payloads originais da API (JSON) ou arquivos sem transformação de negócio |
| Tabela Alinhada à Fonte (SCD1)        | REQUERIDO       | Espelho da fonte com colunas de auditoria (`_ingested_at`, `_source_file`, `_process_id`) |
| Tabela Alinhada à Fonte (SCD2)        | OPCIONAL        | Visão histórica com dimensões lentamente mutáveis |

**Padrões operacionais:**

| Elemento              | Especificação |
|-----------------------|---------------|
| x-dataLayer           | Bronze        |
| Formatos de Saída     | Delta Table   |
| Estratégia de Ingestão| CDC, Carga Completa ou Time Series conforme endpoint |
| Arquivo Fonte         | Registrado como volume (ex.: diretório raw em DBFS) |
| Retenção              | Arquivos fonte: 2 meses / Tabelas: sem purge |
| Acesso                | Limitado a engenheiros de dados |
| Transformações        | Nenhuma transformação de negócio; apenas auditoria |

**Exemplo de tabela Bronze (API MXM):** `brz_t_fin_clientes`, `brz_t_fin_titulos_receber` — uma tabela por entidade/endpoint ingerido.

### 4.2 Camada Silver (`SLRPath` / `QVDSilver`)

**Propósito:** Aplicar lógica de negócio, padronização e qualidade para análise.

| Tipo de Ativo              | Descrição |
|----------------------------|-----------|
| Produto Alinhado à Fonte   | Tipos padronizados, tratamento de NULL, nomes amigáveis |
| Produto Silver             | Entidades consolidadas, deduplicadas, sem agregações |

**Padrões operacionais:**

| Elemento          | Especificação |
|-------------------|---------------|
| x-dataLayer       | Silver        |
| Formatos de Saída | Delta Table (ou Iceberg se necessário) |
| Acesso            | Usuários finais, aplicações globais |
| Retenção          | Sem purge     |
| Transformações    | Deduplicação, mapeamento de referência, dimensões conformadas |
| Proibido          | Agregação ou redução de linhas além de filtros lógicos |

**Exemplo:** `slv_fin_clientes`, `slv_fin_titulos_receber` — entidades prontas para consumo analítico e para alimentar Gold.

### 4.3 Camada Gold (`GLDPath` / `QVDGold`)

**Propósito:** Dados finais para negócio: dashboards, relatórios e modelos.

| Tipo de Ativo   | Descrição |
|-----------------|-----------|
| Produto Gold    | Totalmente transformado, agregado e sumarizado |

**Padrões operacionais:**

| Elemento          | Especificação |
|-------------------|---------------|
| x-dataLayer       | Gold          |
| Formatos de Saída | Delta Table   |
| Acesso            | Usuários de negócio, dashboards, ferramentas analíticas |
| Retenção          | Sem purge     |
| Transformações    | Agregações, sumarizações, KPIs, métricas de negócio |

**Exemplo:** `gld_fin_kpi_vendas`, `gld_fin_resumo_cobranca` — tabelas otimizadas para consumo.

---

## 5. Padrões Declarativos por Camada

### 5.1 Bronze

| Princípio     | Padrão Declarativo |
|---------------|--------------------|
| Findable      | Registro em catálogo com metadados técnicos (tags, ownership) |
| Accessible    | Controle de acesso via políticas (RBAC/ABAC) |
| Interoperable | Schemas em Delta Lake |
| Reusable      | Schemas reutilizáveis e versionados |
| Observable    | Monitoramento de frescor, métricas de qualidade e saúde do pipeline |
| Explainable   | Linhagem mantida via documentação |
| Governed      | Políticas de acesso, privacidade e retenção definidas |
| Composable    | Arquitetado para consumo por Silver |
| Versioned     | Dados e schemas com controle de versão |
| Validated     | Testes automatizados (schema, nulos) |

### 5.2 Silver

| Princípio       | Padrão Declarativo |
|-----------------|--------------------|
| Findable        | Catálogo técnico + catálogo de negócio |
| Accessible      | Exposição via padrões de integração + OAuth2.0/MFA + HTTPS |
| Interoperable   | Delta Lake ou Iceberg |
| Reusable        | Versionamento + documentação de licenciamento e uso |
| Observable      | Monitoramento (ex.: Soda, Datadog) + alertas de SLA |
| Explainable     | Documentação de features + linhagem |
| Governed        | Ownership definido + políticas de retenção |
| Composable      | Consumo via Delta Sharing, GraphQL, Jobs |
| Versioned       | Versionamento + changelogs |
| Validated       | Testes automatizados |
| Self-Describing | Arquivo descritor (dataproduct.yaml / EDPDS) |

### 5.3 Gold

| Princípio       | Padrão Declarativo |
|-----------------|--------------------|
| Findable        | Catálogo técnico + catálogo de negócio |
| Accessible      | Padrões de integração + OAuth2.0/MFA + HTTPS |
| Interoperable   | Delta Lake ou Iceberg |
| Reusable        | Schemas reutilizáveis + documentação |
| Observable      | Monitoramento completo + alertas |
| Explainable     | Documentação completa + linhagem |
| Governed        | Políticas definidas + ownership |
| Composable      | Múltiplos padrões de consumo |
| Versioned       | Controle de versão rigoroso |
| Validated       | Validações automatizadas |
| Self-Describing | Descritor EDPDS (recomendado) |

---

## 6. Padrões de Integração e Acesso

### 6.1 Padrões de Integração Suportados

Os produtos de dados DEVEM ser expostos via um dos seguintes:

1. Conexão SQL Warehouse (Databricks)
2. Endpoints GraphQL
3. Integração de Catálogo (federada ou nativa)
4. Delta Sharing
5. Ferramentas de orquestração (Apache Airflow, Dagster, Jobs Databricks)
6. APIs RESTful

### 6.2 Requisitos de Segurança

- **Autenticação:** OAuth2.0 obrigatório onde aplicável
- **Multifator:** MFA obrigatório para acesso a dados sensíveis
- **Criptografia:** HTTPS com certificados SSL/TLS
- **Autorização:** Controle de acesso baseado em papéis (RBAC)

---

## 7. Stack Tecnológico (Projeto API MXM + Databricks)

### 7.1 Ingestão e Bronze

- **Fonte:** API MXM-WebManager (REST, JSON)
- **Ingestão:** Databricks Jobs, Spark/PySpark (requests ou similar para HTTP)
- **Armazenamento:** Delta Lake (path `QVDBronze` para dados brutos)
- **Catálogo:** Databricks Unity Catalog (ou Hive Metastore)

### 7.2 Silver

- **Processamento:** Apache Spark (PySpark), eventualmente dbt
- **Qualidade:** Great Expectations, Soda Core (ou testes unitários em CI)
- **Armazenamento:** Delta Lake (`QVDSilver`)
- **Catálogo:** Schema registry / Unity Catalog

### 7.3 Gold

- **Agregações:** SQL, PySpark
- **Disponibilização:** Views materializadas, SQL Warehouse
- **Visualização:** Dashboards no Databricks ou ferramentas externas (opcional)

### 7.4 Orquestração e Observabilidade

- **Orquestração:** Databricks Workflows, Apache Airflow ou Dagster
- **Monitoramento:** Logs e métricas do Databricks; integração com Prometheus/Grafana ou Datadog (adaptado)
- **Linhagem:** OpenLineage, Marquez (adaptado), ou documentação em dataproduct.yaml

---

## 8. Metadados e Auto-descrição

### 8.1 Descritor de Produto de Dados (dataproduct.yaml)

Todo produto de dados DEVE incluir um arquivo descritor na raiz do produto ou em pasta `docs/dataproducts/`:

```yaml
version: 1.0.0
kind: DataProduct
metadata:
  name: nome_do_produto
  domain: domínio
  layer: bronze|silver|gold
  owner:
    name: Nome do Responsável
    email: email@exemplo.com
  description: Descrição detalhada
spec:
  type: table|file|api
  format: delta|parquet|iceberg
  schema:
    - name: coluna1
      type: string
      description: Descrição
      nullable: false
    - name: coluna2
      type: integer
      description: Descrição
      nullable: true
  tags:
    - tag1
    - tag2
  version: 1.0.0
  lineage:
    sources:
      - fonte1
      - fonte2
  quality:
    freshness_sla: 24h
    tests:
      - name: not_null_check
      - name: unique_check
  access:
    authentication: oauth2
    protocols:
      - sql
      - api
```

### 8.2 Colunas de Auditoria (Bronze)

Tabelas Bronze devem incluir (quando aplicável):

- `_ingested_at` (timestamp)
- `_source_file` ou `_source_endpoint` (string)
- `_process_id` (string, código do processo da API MXM quando for gravação assíncrona)

---

## 9. Exemplo Prático: Pipeline de Dados da API MXM

### 9.1 Contexto

- **Objetivo:** Ingerir dados da API MXM-WebManager (ex.: clientes, títulos a receber), persistir em Bronze, curar em Silver e disponibilizar KPIs em Gold.
- **Paths:** `QVDForge` → `QVDBronze` → `QVDSilver` → `QVDGold` com prefixos `frg_fin_`, `brz_t_fin_` (única com _t_), `slv_fin_`, `gld_fin_`.

### 9.2 Bronze – Ingestão da API

- Chamada à API (ex.: `POST /webmanager/api/InterfacedoCliente/ObterClientes`).
- Payload bruto gravado direto em tabela Bronze `brz_t_fin_clientes` com colunas de auditoria.
- Formato: Delta Table em `TBRZPath` (ou schema `bronze` no catálogo).

### 9.3 Silver – Limpeza e Conformação

- Leitura de `brz_t_fin_clientes`.
- Padronização de nomes, tipos, tratamento de nulos e deduplicação.
- Gravação em `slv_fin_clientes` em `SLRPath` (QVDSilver).

### 9.4 Gold – Produto para Negócio

- Leitura de `slv_fin_*` (e outras Silver necessárias).
- Agregações, KPIs e métricas (ex.: totais por região, situação financeira).
- Gravação em tabelas `gld_fin_*` em `GLDPath` (QVDGold).

### 9.5 Descritor (Gold) – Exemplo

Exemplo para um produto Gold `gld_fin_kpi_vendas`:

```yaml
version: 1.0.0
kind: DataProduct
metadata:
  name: gld_fin_kpi_vendas
  domain: financeiro
  layer: gold
  owner:
    name: "Equipe de Dados"
    email: "dados@exemplo.com"
  description: "KPIs de vendas e receita para dashboards de negócio"
spec:
  type: table
  format: delta
  schema:
    - name: data_ref
      type: date
      description: "Data de referência"
      nullable: false
    - name: receita_total
      type: double
      description: "Soma das vendas no período"
      nullable: false
    - name: quantidade_vendas
      type: long
      description: "Número de vendas"
      nullable: false
  tags:
    - vendas
    - kpi
    - gold
  version: 1.0.0
  lineage:
    sources:
      - slv_fin_vendas
      - slv_fin_clientes
  quality:
    freshness_sla: 24h
    tests:
      - name: not_null_data_ref
      - name: receita_nao_negativa
  access:
    authentication: oauth2
    protocols:
      - sql
```

---

## 10. Estrutura de pastas do framework

O código do pipeline está organizado em `src/framework/` com dois estágios e **um domínio** (financeiro/api_mxm):

- **raw_to_bronze/** – Ingestão direta da API MXM para Bronze (dados brutos): notebook executor, por domínio: `ddl/schema_updates/`, `ingestion_configs/`, `views/`.
- **bronze_to_silver/** – Transformação Bronze → Silver: idem, com configs e DDL para tabelas `slv_fin_*` (Silver sem _t_).

Detalhes em [src/framework/README.md](../src/framework/README.md).

## 11. Referências

- Documentação da API MXM: [docs/API_MXM_DOCUMENTACAO.md](./API_MXM_DOCUMENTACAO.md) e [docs/swagger.json](./swagger.json)
- Padrões EDPS (WDiscovery) – documento base
- Databricks Medallion Architecture
- Unity Catalog e Delta Lake – documentação oficial Databricks

teste CI/CD
