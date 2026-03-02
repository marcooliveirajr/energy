# Estágio Bronze → Silver

Este estágio do pipeline aplica limpeza, padronização e enriquecimento dos dados da camada **Bronze** para a camada **Silver**, conforme a arquitetura medallion (EDPS).

## Responsabilidades

- Ler tabelas Bronze (prefixo `brz_t_fin_*`, única camada com _t_) do domínio configurado
- Aplicar deduplicação, tratamento de nulos e mapeamento de referência
- Gravar tabelas Silver (prefixo `slv_fin_*`, sem _t_) em Delta
- **Não** aplicar agregações ou redução de linhas além de filtros lógicos

## Estrutura por domínio

```
bronze_to_silver/
├── README.md
├── bronze_to_silver_whl_executor_util.ipynb
└── financeiro/
    └── api_mxm/
        ├── ddl/schema_updates/
        ├── ingestion_configs/
        └── views/
```

## Execução

Utilizar o notebook `bronze_to_silver_whl_executor_util.ipynb` para orquestrar a transformação por domínio/fonte. Configurações por tabela em `ingestion_configs/`.
