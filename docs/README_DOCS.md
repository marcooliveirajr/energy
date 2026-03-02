# Documentação do Projeto – API MXM + Databricks

Pasta central da documentação do projeto de **arquitetura de dados** com Databricks: ingestão da API MXM-WebManager e disponibilização para o negócio.

## Conteúdo

| Documento | Descrição |
|-----------|-----------|
| [ARQUITETURA_PIPELINE_DADOS.md](./ARQUITETURA_PIPELINE_DADOS.md) | **Arquitetura do pipeline** – EDPS, Forge → Bronze → Silver → Gold, paths, prefixos (_t_ apenas em Bronze) |
| [PASSO_A_PASSO_IMPLEMENTACAO_DATABRICKS.md](./PASSO_A_PASSO_IMPLEMENTACAO_DATABRICKS.md) | **Passo a passo** – Implementação no Databricks (ambiente, Forge, Bronze, Silver, Gold, jobs) |
| [INTEGRACAO_GITHUB_DATABRICKS.md](./INTEGRACAO_GITHUB_DATABRICKS.md) | **Integração GitHub + Databricks** – Repos ([energy](https://github.com/marcooliveirajr/energy)) e GitHub Actions |
| [INTEGRACAO_GITLAB_DATABRICKS.md](./INTEGRACAO_GITLAB_DATABRICKS.md) | Integração GitLab + Databricks (alternativa) |
| [API_MXM_DOCUMENTACAO.md](./API_MXM_DOCUMENTACAO.md) | Resumo da API MXM (autenticação, base URL, grupos de endpoints, uso no pipeline) |
| [swagger.json](./swagger.json) | **Especificação OpenAPI (Swagger 2.0)** completa – todos os endpoints, parâmetros e schemas |
| [SCRAPING_NOTAS.md](./SCRAPING_NOTAS.md) | Notas da tentativa inicial de scraping do site; documentação passou a ser o swagger.json |
| [dataproducts/dataproduct.yaml.template](./dataproducts/dataproduct.yaml.template) | Template do descritor de produto de dados (EDPS) |

## Fonte da documentação da API

A documentação oficial da API está no arquivo **`docs/swagger.json`** (OpenAPI/Swagger 2.0), que contém:

- **Centenas de endpoints** organizados por módulo (Cliente, Contas a Pagar/Receber, Contabilidade, Cadastros, etc.)
- **Autenticação:** OAuth 2.0 (Identity Server) + objeto `AuthenticationToken` (UserName, Password, EnvironmentName)
- **Processamento assíncrono:** gravações retornam código de processo; status consultado via APIs ConsultaporProcesso
- **Definições** de request/response (schemas) para cada operação

Para detalhes de um endpoint específico (parâmetros, body, respostas), use o `swagger.json` ou ferramentas que importem OpenAPI (Postman, Swagger UI, etc.).

## Uso no projeto

- **Pipeline:** ingestão API → Bronze → Silver → Gold.
- **CI/CD:** scripts e pipelines versionados e publicados via pipeline de CI/CD.
- **Negócio:** dados disponibilizados em camada Gold para relatórios e análises.

Siga a metodologia e a estrutura de pastas definidas para evoluir scripts e esta documentação.
