# Resources – Config para Databricks

JSON **sem credentials**: apenas estrutura de conexão, destino e tabelas. A senha do MySQL é configurada no **Databricks Secrets** (scope/key no job).

- **acesso_extract_dev.json** – env DEV
- **acesso_extract_prod.json** – env PRD

Estrutura:

```json
{
  "connection": {
    "host_primary": "...",
    "host_replica": "...",
    "port": 3306,
    "database": "db_energia_rzk",
    "user": "amee"
  },
  "target": {
    "bronze_base": "s3://thopenenergy/DEV/bronze",
    "forge_base": "s3://thopenenergy/DEV/forge",
    "domain": "acesso"
  },
  "recordsets": [
    {
      "source_table": "tb_...",
      "target_table": "raw_t_acesso_...",
      "enabled": 1,
      "columns": [{"name": "...", "datatype": "INTEGER|VARCHAR(255)|TIMESTAMP|..."}]
    }
  ]
}
```

Gerar JSON completo (todas as tabelas do CSV):

```bash
python scripts/csv_to_seeds_config.py --csv arq_acesso_tabela_coluna.csv --resources-dir resources
```
