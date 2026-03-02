# Framework do Pipeline de Dados

Estrutura de estágios do pipeline (ingestão direta para Bronze → Silver), alinhada à arquitetura medallion e ao documento [docs/ARQUITETURA_PIPELINE_DADOS.md](../../docs/ARQUITETURA_PIPELINE_DADOS.md).

## Estrutura (1 domínio)

```
src/framework/
├── README.md
├── raw_to_bronze/
│   ├── raw_to_bronze_whl_executor_util.ipynb
│   └── financeiro/
│       └── api_mxm/
│           ├── ddl/schema_updates/    # SQL de schema Bronze (brz_t_fin_* – única camada com _t_)
│           ├── ingestion_configs/     # JSON por tabela
│           └── views/                 # Views sobre Bronze
└── bronze_to_silver/
    ├── README.md
    ├── bronze_to_silver_whl_executor_util.ipynb
    └── financeiro/
        └── api_mxm/
            ├── ddl/schema_updates/    # SQL de schema Silver (slv_fin_*)
            ├── ingestion_configs/     # JSON por tabela
            └── views/                 # Views sobre Silver
```

## Domínio atual

- **financeiro / api_mxm**: ingestão e transformação de dados da API MXM-WebManager (clientes, títulos a receber, etc.).

## Convenções

- Paths e prefixos: `config/paths_and_prefixes.yaml` e `src.config.paths`.
- Bronze: prefixo `brz_t_fin_` (única camada com _t_), path `/QVDBronze`.
- Silver: prefixo `slv_fin_` (sem _t_), path `/QVDSilver`.

Para adicionar novos domínios no futuro, replicar a estrutura `financeiro/api_mxm` em cada estágio (raw_to_bronze e bronze_to_silver).
