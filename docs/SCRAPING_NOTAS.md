# Notas do scraping – Documentação API MXM

**Data:** 02/03/2025  
**Objetivo:** Extrair documentação da API a partir do site oficial para uso no projeto Databricks.

**Atualização:** A documentação passou a ser o arquivo **`docs/swagger.json`** (OpenAPI/Swagger 2.0) fornecido pelo usuário. O resumo está em [API_MXM_DOCUMENTACAO.md](./API_MXM_DOCUMENTACAO.md).

---

## URLs consultadas

| URL | Resultado |
|-----|-----------|
| `https://documentacaointerface.mxm.com.br/api` | 200 – Conteúdo textual: apenas título "Documentação MXM-WebManager Interface" |
| `https://documentacaointerface.mxm.com.br/` | 200 – Mesmo título, conteúdo mínimo |
| `https://documentacaointerface.mxm.com.br/api/docs` | 404 |
| `https://documentacaointerface.mxm.com.br/swagger` | 404 |
| `https://documentacaointerface.mxm.com.br/api/swagger.json` | 404 |
| `https://documentacaointerface.mxm.com.br/api/v1` | 404 |

---

## Conclusão

- O domínio **documentacaointerface.mxm.com.br** responde, mas o conteúdo útil (endpoints, parâmetros, exemplos) não aparece no HTML estático.
- É provável que a documentação seja renderizada via **JavaScript (SPA)** ou que exista em subpastas não testadas.
- Não foi encontrado arquivo **OpenAPI/Swagger** nas URLs comuns (`/swagger`, `/api/swagger.json`).

---

## Recomendações

1. **Fornecer documentação manualmente:** Se você tiver PDF, Word ou prints da documentação MXM, podemos transcrever para `API_MXM_DOCUMENTACAO.md`.
2. **Verificar com a MXM:** Solicitar especificação OpenAPI (Swagger) ou lista de endpoints com autenticação.
3. **Inspecionar no navegador:** Abrir o site, usar DevTools (Network) ao navegar na documentação e identificar chamadas XHR/fetch que carregam JSON com a lista de endpoints.
4. **Testes exploratórios:** Com base em nomes comuns (ex.: `/api/usuarios`, `/api/clientes`), testar endpoints se houver ambiente de homologação disponível.

Com essas informações, a documentação em `docs/API_MXM_DOCUMENTACAO.md` pode ser completada e usada para desenhar o pipeline de ingestão no Databricks.
