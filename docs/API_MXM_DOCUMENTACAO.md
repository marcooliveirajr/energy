# Documentação da API MXM-WebManager Interface

**Fonte:** Especificação OpenAPI (Swagger 2.0) — arquivo [`docs/swagger.json`](./swagger.json)  
**Documentação online:** [https://documentacaointerface.mxm.com.br/api](https://documentacaointerface.mxm.com.br/api)  
**Última atualização:** 02/03/2025  
**Projeto:** Arquitetura de dados Databricks – ingestão via API e disponibilização para o negócio

---

## 1. Visão geral

A **API MXM-WebManager** é a interface de integração do ERP WebManager versão 10 da MXM. O documento oficial está no **Swagger 2.0** em `docs/swagger.json`, com centenas de endpoints REST para cadastros, contabilidade, contas a pagar/receber, clientes, contratos, compras, etc.

### 1.1 Metadados do Swagger

| Campo | Valor |
|-------|--------|
| **Título** | Documentação das API's de ERP WebManager versão 10 |
| **Versão da documentação** | 3 |
| **Contato** | interface@mxm.com.br |
| **Licença** | MXM Sistemas – http://www.mxm.com.br |

### 1.2 Conceitos (resumo da documentação oficial)

- **REST:** APIs seguem o estilo REST (Transferência de Estado Representacional).
- **Codificação:** Recomendado **UTF-8** para envio de JSON (evitar caracteres especiais como `»` serem enviados como `?` ou nulo).
- **Processamento assíncrono:** Muitas operações de gravação retornam um **código de processo** (sequencial). O status deve ser consultado via APIs **ConsultaporProcesso** (ou **ConsultaporProcessoeSequencia**) ou nos relatórios do WebManager (ex.: *Dashboard de integração*, *Resultado das importações de modelo*).
- **Processos de importação:** Para usar APIs de gravação, o processo correspondente deve estar **ativo** nos cadastros de processos de importação do sistema de integração. Em caso de “Processo XXX não ativo”, contatar SAU/TI da MXM.

---

## 2. Base URL e ambiente

| Item | Valor |
|------|--------|
| **Host (placeholder no Swagger)** | `seuenderecodowebmanager` — substituir pelo endereço real do WebManager do cliente |
| **Esquema** | `https` |
| **Base path da API** | `/webmanager/api/` |
| **URL completa (exemplo)** | `https://<seuenderecodowebmanager>/webmanager/api/` |

O endereço real do WebManager (produção/homologação) deve ser definido por ambiente e obtido com a MXM ou com o executivo de conta.

---

## 3. Autenticação

A API utiliza **dois mecanismos** (ambos podem ser exigidos conforme configuração):

### 3.1 Token OAuth 2.0 (opcional por instalação)

- **Fluxo:** Client Credentials (OAuth 2.0).  
- **Servidor de identidade:** `https://identityserver.mxmwebmanager.com.br/connect/token`  
- **Alternativa via WebManager:** `POST /webmanager/api/identity/token` (repassa a chamada ao Identity Server).
- **Corpo da requisição (form-urlencoded):**
  - `grant_type`: `client_credentials` (obrigatório)
  - `scope`: opcional (não utilizado na versão atual)
- **Credenciais:** `client_id` e `client_secret` fornecidos pela MXM, enviados como **Basic Auth**.
- **Resposta (200):**
  - `access_token`: token JWT
  - `expires_in`: tempo em segundos até expiração (ex.: 3600)
  - `token_type`: `Bearer`
- **Uso:** Se a validação de token estiver habilitada no WebManager (`ativarValidacaoDeTokenParaAPIs` no mxm.config), enviar em todas as requisições:  
  **Header:** `Authorization: Bearer {token JWT}`

### 3.2 Objeto de autenticação (obrigatório nos POSTs de negócio)

Em **todos** os processos de gravação, consulta ou exclusão que usam **POST**, o corpo do JSON deve incluir o grupo **AuthenticationToken** com:

| Atributo | Descrição |
|----------|-----------|
| **UserName** | Usuário (solicitar à MXM) |
| **Password** | Senha (solicitar à MXM) |
| **EnvironmentName** | Nome do ambiente (ex.: `MXMDS`) |

Essas informações são **obrigatórias** e devem ser solicitadas ao **Executivo de Conta** na MXM.

---

## 4. Padrões de resposta e consulta de processo

### 4.1 Retorno padrão de gravação (assíncrono)

Muitos endpoints de **Gravar** retornam:

```json
{
  "Success": true,
  "Data": "821",
  "Messages": [ "..." ]
}
```

- **Success:** indica sucesso da aceitação do processo.
- **Data:** **código do processo** (sequencial) — usar para consultar o status.
- **Messages:** detalhes do retorno.

### 4.2 Consulta de status do processo

- **ConsultaporProcesso:** consulta pelo código retornado em `Data`.
- **ConsultaporProcessoeSequencia:** quando houver sequência de registro (processo + sequência).

Os detalhes dos schemas de resposta estão em `swagger.json` (definitions).

---

## 5. Grupos de endpoints (módulos da API)

A lista abaixo resume os **grupos** de endpoints presentes no `swagger.json`. O arquivo completo contém **centenas de paths**; para contrato exato (parâmetros, body, response), use `docs/swagger.json`.

| Módulo / Tag | Exemplos de paths | Uso típico no pipeline |
|--------------|-------------------|--------------------------|
| **Identity Server** | `POST /connect/token`, `POST /webmanager/api/identity/token` | Obter token (ingestão) |
| **Acréscimo / Decréscimo** | Gravar, ConsultaporProcesso, RetornaTodosAcrescimosDecrescimos | Cadastros |
| **Adiantamento / Devolução** | InterfacedaAntecipacao (Gravar, Consultar, ObterSaldo, RequisiçãoAdiantamento...) | Financeiro |
| **Interface Anexo** | GravarAnexoSincrono, ConsultarAnexo, ExcluirAnexo | Anexos |
| **Cadastros Comuns** | CategoriaFuncional, EstruturaFuncional, GrupoCotacaoFornecedor, UnidadeMedida, UnidadeProduto, Kit | Cadastros base |
| **Contabilidade** | Gravar, ConsultaporProcesso, ConsultaSaldo, ConsultaLancamentos, Balancete (Contabil/CentroCusto, PorPeriodo), Parâmetros contábeis | Contabilidade |
| **Tipo de Operação** | Gravar, ConsultaporProcesso, RetornaTodosTiposdeOperacao | Cadastros |
| **Operadora Cartão Crédito** | ObterPorCodigo, ObterPorDescricao, ObterPorBandeira | Cadastros |
| **Impostos** | GravarImposto, ConsultarImpostoPorProcesso, ConsultarImpostos | Fiscal |
| **Cadastros Financeiro** | Banco, Beneficiário, RelEmpresaFilialBeneficiário (GravarSincrono/Consultar) | Financeiro |
| **Centro de Custo** | InterfacedoCentrodeCusto (Gravar, Consulta), CentrodeCusto (Consulta por código/modelo/descrição) | Custo |
| **Classificação Fiscal** | Gravar, ConsultaporProcesso | Fiscal |
| **Cliente** | InterfacedoCliente (Gravar, Alterar, GravarSincrono, ConsultaporProcesso, ObterClientes...), Cliente (ConsultarSituacaoFinanceira, PorDataCadastro) | **Ingestão para negócio** |
| **Grupo de Cotação** | GravarCotacao, ConsultaCotacao, ExcluirCotacao | Compras |
| **Compras** | MapaCotacao, InformaçãoCotacao, AprovacaoMapaCotacao, EventosdaFichaFinanceira | Compras |
| **Contato Cliente** | ConsultaContatoCliente, Gravar, GravarSincrono, ExcluirSincrono, ConsultaporProcesso | CRM/Cadastros |
| **Contas a Pagar/Receber** | Gravar, GravarSincrono, ConsultaporProcesso, ConsultaTituloPagar/Receber, BaixarListaTitulos, ConsultarTituloPagarPorDatas, FichaFinanceiraDoCliente... | **Ingestão para negócio** |
| **Fatura** | ConsultarTitulosReceberFaturaPorStatus | Financeiro |
| **Contrato de Compras** | ConsultaContratodeCompras, ConsultaPreContratoComPedido, GravarAprovacaoPagamentoContrato | Compras |
| **Contrato de Vendas** | Gravar, ConsultaContratodeVendas, ConsultaLiberacaoPedido, ConsultaPropostaComercial, AssinaturaContratoVendas, PrestadorServico... | Vendas |
| **Custos** | LancamentoMensuracaoProducao, SelecaoValoresCentroCustoGerencial | Custos |
| **Importação EFD** | ImportarNfeCteCteosNfce, ImportarNfse | Fiscal |
| **Email** | ConsultarEMail, ReenvioDeEmail | Comunicação |

Há ainda outros módulos (Fornecedor, Produto, Pedido, Nota Fiscal, etc.) no `swagger.json`. Para **ingestão no Databricks**, os mais relevantes costumam ser:

- **Cliente** (ObterClientes, ConsultarClientePorDataCadastro, ConsultarSituacaoFinanceira...)
- **Contas a Pagar/Receber** (consulta de títulos, ficha financeira)
- **Contabilidade** (balancete, lançamentos, saldo)
- **Contrato de Vendas** (consulta contratos, liberação de pedido)

---

## 6. Parâmetros comuns (query/body)

Conforme `swagger.json`, diversos endpoints utilizam parâmetros como:

| Parâmetro | Descrição | Exemplo |
|-----------|-----------|---------|
| **environmentName** | Nome do ambiente | `MXMDS` |
| **codigodaEmpresa** | Código da empresa (até 4 caracteres) | `EMPR` |
| **codigodaFilial** | Código da filial (até 4 caracteres) | `FILI` |
| **sequenciadoProcesso** | Número do processo | `821` |
| **sequenciadoRegistro** | Sequência do registro | `2` |
| **dataEmissaoInicial / dataEmissaoFinal** | Período de emissão | `2019-03-01` |
| **codigoClienteFornecedor** | Código do cliente ou fornecedor | `2441567004` |
| **clienteFornecedor** | `C` (Cliente) ou `F` (Fornecedor) | `C` |

Os nomes exatos e obrigatoriedade variam por endpoint; consultar `docs/swagger.json` por path.

---

## 7. Uso no projeto (Databricks)

| Item | Descrição |
|------|-----------|
| **Objetivo** | Ingestão de dados da API MXM no Databricks |
| **Pipeline** | API → Bronze (dados brutos direto) → Silver (curated) → Gold (negócio) |
| **CI/CD** | Pipelines e jobs versionados e implantados via CI/CD |
| **Consumidores** | Área de negócio (relatórios, dashboards, aplicações) |
| **Documentação de referência** | `docs/swagger.json` para contratos exatos (request/response) |

---

## 8. Próximos passos

1. **Definir** a base URL (host) por ambiente (dev/homologação/produção).
2. **Escolher** os endpoints exatos para ingestão (por exemplo: Cliente, Contas a Pagar/Receber, Contabilidade) e documentar em `docs/endpoints/` se necessário.
3. **Implementar** autenticação (token + AuthenticationToken) e tratamento de processo assíncrono (ConsultaporProcesso) nos jobs de ingestão.
4. **Alinhar** com a estrutura de pastas e scripts do projeto (Databricks, CI/CD, camadas Bronze/Silver/Gold).

---

## 9. Referências

- **Especificação completa:** [`docs/swagger.json`](./swagger.json) (OpenAPI/Swagger 2.0)
- **Documentação online:** [https://documentacaointerface.mxm.com.br/api](https://documentacaointerface.mxm.com.br/api)
- **Contato API:** interface@mxm.com.br
- **Identity Server (token):** https://identityserver.mxmwebmanager.com.br/connect/token
