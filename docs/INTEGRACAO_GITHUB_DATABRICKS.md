# Integração GitHub + Databricks

**Repositório:** [https://github.com/marcooliveirajr/energy](https://github.com/marcooliveirajr/energy)  
**Projeto:** API MXM – Pipeline de dados (Energy)  
**Última atualização:** 02/03/2025

Este documento descreve como integrar o repositório **GitHub** com o **Databricks**, em duas frentes: **Databricks Repos** (código versionado no workspace) e **CI/CD com GitHub Actions** (validação e deploy opcional).

---

## Visão geral

| Integração | Objetivo |
|------------|----------|
| **Databricks Repos + GitHub** | Clonar o repositório no workspace Databricks; editar e executar notebooks/scripts com sync Git. |
| **GitHub Actions + Databricks** | Pipeline que valida o código (push/PR) e, opcionalmente, faz deploy no Databricks na branch `main`. |

**URL do repositório:** `https://github.com/marcooliveirajr/energy.git`

---

## Parte 1 – Databricks Repos com GitHub

O **Repos** permite que o Databricks use um repositório Git (GitHub, GitLab, etc.) como pasta versionada no workspace.

### 1.1 Pré-requisitos

- Repositório no **GitHub:** [marcooliveirajr/energy](https://github.com/marcooliveirajr/energy).
- Token de acesso GitHub com permissão **repo** (para clone; para push a partir do Databricks use escopo que inclua escrita).
- Workspace Databricks com **Repos** habilitado.

### 1.2 Token de acesso GitHub

1. No GitHub: **Settings** (do usuário) → **Developer settings** → **Personal access tokens** → **Tokens (classic)** ou **Fine-grained tokens**.
2. **Classic:** gerar token com escopo `repo` (acesso completo a repositórios privados e públicos).
3. **Fine-grained:** selecionar o repositório `marcooliveirajr/energy` e permissões **Contents: Read** (e **Read and write** se for fazer push do Databricks).
4. Copiar o token e guardá-lo com segurança (será usado no Databricks).

### 1.3 Configurar Git no Databricks (uma vez por workspace)

1. No Databricks: **Settings** (ícone engrenagem) → **Workspace settings** → **Git integration**.
2. Clicar em **Add Git provider**.
3. Escolher **GitHub** (ou **GitHub Enterprise** se for self-hosted).
4. Preencher:
   - **Git provider URL:** `https://github.com` (ou URL do GitHub Enterprise).
   - **Personal access token:** o token criado no passo 1.2.
5. Salvar.

### 1.4 Criar Repo no workspace

1. No menu lateral: **Repos** (ou **Workspace** → pasta **Repos**).
2. Clicar em **Add Repo**.
3. Preencher:
   - **Repository URL:**  
     `https://github.com/marcooliveirajr/energy.git`
   - **Git provider:** o provider GitHub configurado no passo 1.3.
   - **Repository name:** `energy` (nome exibido no workspace).
   - **Branch:** `main` (ou `master`, conforme a branch padrão do repo).
4. Clicar em **Create Repo**.

O Databricks fará o clone. A árvore do projeto aparecerá em **Workspace** → **Repos** → **\<seu-usuario\>** → **energy**.

### 1.5 Usar o Repo nos notebooks e jobs

- **Notebooks:** Salvar em **Repos** → **energy** (ex.: `Repos/<user>/energy/src/framework/raw_to_bronze/...`). Assim os paths relativos (ex.: `ingestion_configs/*.json`) funcionam corretamente.
- **Jobs:** Nas tarefas de job, escolher **Source: Repos** e selecionar o repositório **energy** e o caminho do notebook (ex.: `src/framework/raw_to_bronze/raw_to_bronze_whl_executor_util.ipynb`).
- **Sincronizar:** No Repo, usar **Pull** para trazer alterações do GitHub; **Push** (se o token tiver permissão) para enviar mudanças do workspace de volta ao GitHub.

### 1.6 Estrutura do repositório

Manter a estrutura para que paths relativos funcionem no Databricks:

```
energy/
├── .github/workflows/     # GitHub Actions
├── config/
├── docs/
├── src/
│   ├── config/
│   └── framework/
│       ├── raw_to_bronze/
│       └── bronze_to_silver/
├── .gitignore
└── README.md
```

---

## Parte 2 – CI/CD com GitHub Actions

O pipeline no GitHub valida o código e, opcionalmente, sincroniza o conteúdo com o workspace Databricks.

### 2.1 Pré-requisitos

- **Databricks host e token:** URL do workspace (ex.: `https://adb-xxxxx.azuredatabricks.net`) e um **Personal Access Token (PAT)** do Databricks com permissão para workspace/jobs.
- **Secrets no GitHub:** configurar no repositório **Settings** → **Secrets and variables** → **Actions**.

### 2.2 Secrets no GitHub

Em **GitHub** → repositório **energy** → **Settings** → **Secrets and variables** → **Actions**, adicionar:

| Secret | Descrição | Uso |
|--------|-----------|-----|
| `DATABRICKS_HOST` | URL do workspace (ex.: `https://adb-xxxxx.azuredatabricks.net`) | Deploy |
| `DATABRICKS_TOKEN` | PAT do Databricks | Deploy |

Só são necessários se for habilitar o job de deploy no workflow.

### 2.3 Workflow (`.github/workflows/databricks.yml`)

O repositório inclui um workflow que:

- **validate:** roda em todo push e em pull requests; valida a estrutura e compila módulos Python (ex.: `src/config/paths.py`).
- **deploy-databricks:** opcional; roda apenas na branch `main` após push. Usa Databricks CLI para sincronizar o repositório com um path no workspace (ex.: `/Workspace/Repos/deploy/energy`). Para ativar, descomentar o job no arquivo e garantir que os secrets `DATABRICKS_HOST` e `DATABRICKS_TOKEN` existam.

### 2.4 Ativar o deploy

1. Adicionar os secrets `DATABRICKS_HOST` e `DATABRICKS_TOKEN` no repositório (ver 2.2).
2. Editar `.github/workflows/databricks.yml` e descomentar o job `deploy-databricks` (e remover o `if: false` se estiver usado para desabilitar).
3. Fazer push na branch `main`; o workflow rodará e, na etapa de deploy, enviará o código para o Databricks.

### 2.5 Apenas validação (sem deploy)

Se não configurar os secrets e deixar o job de deploy comentado ou desabilitado, o pipeline continuará rodando apenas a etapa de **validate**, útil para checagem em pull requests.

---

## Parte 3 – Resumo prático

### Só usar Repos (desenvolvimento no Databricks)

1. Criar **Personal Access Token** no GitHub (escopo `repo`).
2. Em **Databricks** → **Settings** → **Git integration** → adicionar provider **GitHub** (URL + token).
3. **Repos** → **Add Repo** → URL: `https://github.com/marcooliveirajr/energy.git` → criar.
4. Desenvolver e rodar notebooks em **Repos/.../energy**; usar **Pull** para atualizar a partir do GitHub.

### Repos + GitHub Actions (validação e deploy)

1. Fazer os passos da Parte 1 (Repos).
2. Criar **PAT no Databricks**; cadastrar `DATABRICKS_HOST` e `DATABRICKS_TOKEN` em **GitHub** → **Settings** → **Secrets and variables** → **Actions**.
3. O workflow em `.github/workflows/databricks.yml` já valida em push/PR; descomentar e ajustar o job `deploy-databricks` para publicar no workspace na branch `main`.

### Boas práticas

- **Não** commitar credenciais (API MXM, Databricks). Usar Secret Scope no Databricks e Secrets no GitHub.
- Manter a **branch `main`** estável; usar branches para features e abrir **Pull Request** antes do merge.
- Se fizer push do Databricks para o GitHub, usar um token com permissão de escrita e alinhar com a equipe quem pode fazer push.

---

## Referências

- [Databricks – Git integration (Repos)](https://docs.databricks.com/repos/index.html)
- [Databricks – Connect to GitHub](https://docs.databricks.com/repos/git-operations-with-repos.html#github)
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html)
- [GitHub Actions](https://docs.github.com/en/actions)
- Repositório: [https://github.com/marcooliveirajr/energy](https://github.com/marcooliveirajr/energy)
