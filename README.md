# Energy – Pipeline de dados API MXM + Databricks

[![Validate and Deploy to Databricks](https://github.com/marcooliveirajr/energy/actions/workflows/databricks.yml/badge.svg)](https://github.com/marcooliveirajr/energy/actions/workflows/databricks.yml)

Repositório do projeto de **arquitetura de dados** com ingestão da **API MXM-WebManager**, processamento no **Databricks** (Forge → Bronze → Silver → Gold) e disponibilização para o negócio.
com CI/CD

**Repositório:** [https://github.com/marcooliveirajr/energy](https://github.com/marcooliveirajr/energy)

test 4 cicd
---

## Estrutura

```
energy/
├── .github/workflows/     # GitHub Actions (validação e deploy Databricks)
├── config/               # Paths e prefixos (YAML)
├── docs/                 # Documentação (arquitetura, API MXM, integração GitHub/Databricks)
├── src/
│   ├── config/           # Constantes Python (paths, prefixos)
│   └── framework/        # Pipeline raw_to_bronze e bronze_to_silver
│       ├── raw_to_bronze/
│       └── bronze_to_silver/
└── README.md
```

---

## Documentação

| Documento | Descrição |
|-----------|-----------|
| [docs/ARQUITETURA_PIPELINE_DADOS.md](docs/ARQUITETURA_PIPELINE_DADOS.md) | Arquitetura medallion (Forge, Bronze, Silver, Gold), nomenclatura (_t_ apenas em Bronze) |
| [docs/PASSO_A_PASSO_IMPLEMENTACAO_DATABRICKS.md](docs/PASSO_A_PASSO_IMPLEMENTACAO_DATABRICKS.md) | Implementação no Databricks (ambiente, schemas, jobs) |
| [docs/INTEGRACAO_GITHUB_DATABRICKS.md](docs/INTEGRACAO_GITHUB_DATABRICKS.md) | Integração GitHub + Databricks (Repos e GitHub Actions) |
| [docs/API_MXM_DOCUMENTACAO.md](docs/API_MXM_DOCUMENTACAO.md) | Resumo da API MXM (autenticação, endpoints) |

---

## Integração GitHub + Databricks

- **Databricks Repos:** clone do repositório no workspace — URL `https://github.com/marcooliveirajr/energy.git`.
- **GitHub Actions:** workflow em `.github/workflows/databricks.yml` — validação em push/PR; deploy opcional na branch `main` (requer secrets `DATABRICKS_HOST` e `DATABRICKS_TOKEN`).

Ver [docs/INTEGRACAO_GITHUB_DATABRICKS.md](docs/INTEGRACAO_GITHUB_DATABRICKS.md).

---

## Convenções de nomenclatura

| Camada | Prefixo | Exemplo |
|--------|---------|---------|
| Forge | `frg_fin_` | `frg_fin_pipeline_log` |
| Bronze | `brz_t_fin_` | `brz_t_fin_clientes` |
| Silver | `slv_fin_` | `slv_fin_clientes` |
| Gold | `gld_fin_` | `gld_fin_kpi_vendas` |

Apenas a camada **Bronze** utiliza **_t_** no prefixo.
