# Integração GitLab + Databricks

**Projeto:** API MXM – Pipeline de dados  
**Última atualização:** 02/03/2025

Este documento descreve como integrar o repositório **GitLab** com o **Databricks**, em duas frentes: **Databricks Repos** (código versionado no workspace) e **CI/CD com GitLab** (pipelines que deployam ou validam no Databricks).

---

## Visão geral

| Integração | Objetivo |
|------------|----------|
| **Databricks Repos + GitLab** | Clonar o repositório GitLab no workspace Databricks; editar e executar notebooks/scripts direto no Databricks com sync Git. |
| **GitLab CI/CD + Databricks** | Pipeline no GitLab que valida código, faz deploy de jobs/notebooks e (opcional) dispara execuções no Databricks. |

---

## Parte 1 – Databricks Repos com GitLab

O **Repos** permite que o Databricks use um repositório Git (GitLab, GitHub, etc.) como pasta versionada no workspace.

### 1.1 Pré-requisitos

- Repositório do projeto no **GitLab** (ex.: `https://gitlab.com/sua-org/api-mxm-databricks`).
- Acesso de **leitura** ao repo (para clone); se quiser push a partir do Databricks, use token com permissão de escrita.
- Workspace Databricks com **Repos** habilitado (geralmente já habilitado em workspaces modernos).

### 1.2 Token de acesso GitLab

1. No GitLab: **User Settings** → **Access Tokens** (ou **Group/Project** → **Settings** → **Repository** → **Deploy tokens**).
2. Criar um token com escopo:
   - **read_repository** (só clone no Databricks), ou
   - **read_repository** + **write_repository** (se for fazer commit/push a partir do Databricks).
3. Copiar o token e guardá-lo de forma segura (será usado no Databricks).

### 1.3 Configurar Git no Databricks (uma vez por workspace)

1. No Databricks: **Settings** (ícone engrenagem) → **Workspace settings** → **Git integration**.
2. Clicar em **Add Git provider**.
3. Escolher **GitLab** (ou **Other** se GitLab self-managed).
4. Preencher:
   - **Git provider URL:** `https://gitlab.com` (ou URL do seu GitLab, ex.: `https://gitlab.suaempresa.com`).
   - **Personal access token:** o token criado no passo 1.2.
5. Salvar.

### 1.4 Criar Repo no workspace

1. No menu lateral: **Repos** (ou **Workspace** → pasta **Repos**).
2. Clicar em **Add Repo**.
3. Preencher:
   - **Repository URL:** URL do projeto GitLab, ex.:
     - HTTPS: `https://gitlab.com/sua-org/api-mxm-databricks.git`
     - Ou SSH: `git@gitlab.com:sua-org/api-mxm-databricks.git` (se SSH estiver configurado).
   - **Git provider:** o provider GitLab configurado no passo 1.3.
   - **Repository name:** ex. `api-mxm-databricks` (nome exibido no workspace).
   - **Branch:** `main` ou `master` (branch padrão).
4. Clicar em **Create Repo**.

O Databricks fará o clone. A árvore do projeto aparecerá em **Workspace** → **Repos** → **\<seu-usuario\>** → **api-mxm-databricks**.

### 1.5 Usar o Repo nos notebooks e jobs

- **Notebooks:** Ao criar um notebook, salvar em **Repos** → **api-mxm-databricks** (ex.: `Repos/<user>/api-mxm-databricks/src/framework/raw_to_bronze/...`). Assim o caminho dos configs (ex.: `ingestion_configs/*.json`) pode ser relativo ao repo.
- **Jobs:** Nas tarefas de job, escolher **Source: Repos** e selecionar o repositório e o caminho do notebook (ex.: `src/framework/raw_to_bronze/raw_to_bronze_whl_executor_util.ipynb`).
- **Sincronizar:** No Repo, usar **Pull** para trazer alterações do GitLab; **Push** (se tiver permissão) para enviar mudanças feitas no workspace de volta ao GitLab.

### 1.6 Estrutura recomendada no GitLab

Manter a mesma estrutura do projeto para que paths relativos funcionem no Databricks:

```
api-mxm-databricks/
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

## Parte 2 – CI/CD GitLab com Databricks

O pipeline GitLab pode validar o código e, opcionalmente, fazer deploy de jobs/notebooks no Databricks usando **Databricks CLI** ou **Databricks Asset Bundles**.

### 2.1 Pré-requisitos

- **Databricks host e token:** URL do workspace (ex.: `https://adb-xxxxx.azuredatabricks.net`) e um **personal access token** (PAT) do Databricks com permissão para criar/atualizar jobs e workspace.
- **GitLab CI/CD:** projeto com pipeline habilitado (`.gitlab-ci.yml` na raiz).

### 2.2 Variáveis no GitLab (recomendado: protegidas e mascaradas)

Em **GitLab** → **Project** → **Settings** → **CI/CD** → **Variables**, cadastre:

| Variável | Descrição | Protegida | Mascarada |
|----------|-----------|-----------|-----------|
| `DATABRICKS_HOST` | URL do workspace (ex.: `https://adb-xxxxx.azuredatabricks.net`) | Sim | Não |
| `DATABRICKS_TOKEN` | PAT do Databricks | Sim | Sim |

Para múltiplos ambientes (dev/prod), use variables com scope (ex.: `DATABRICKS_HOST` com environment `production`).

### 2.3 Opção A – Databricks CLI (sync de workspace / jobs)

O pipeline pode instalar a **Databricks CLI**, autenticar e usar comandos para sincronizar pastas ou importar jobs.

**Exemplo de estágio no `.gitlab-ci.yml`:**

```yaml
deploy-databricks:
  stage: deploy
  image: python:3.10-slim
  before_script:
    - pip install databricks-cli
    - mkdir -p ~/.databrickscfg
    - echo "[DEFAULT]" > ~/.databrickscfg
    - echo "host = $DATABRICKS_HOST" >> ~/.databrickscfg
    - echo "token = $DATABRICKS_TOKEN" >> ~/.databrickscfg
  script:
    # Sincronizar pasta do repo para um path no workspace (ex.: /Workspace/Repos/deploy/api-mxm-databricks)
    - databricks workspace import_dir . /Workspace/Repos/deploy/api-mxm-databricks -o
  only:
    - main
```

Se os jobs forem definidos em JSON (exportados do Databricks), pode-se usar `databricks jobs create --json-file job.json` ou `jobs reset --job-id ... --json-file job.json`.

### 2.4 Opção B – Databricks Asset Bundles (DAB)

Com **Databricks Asset Bundles**, o projeto passa a ter uma pasta `src` (ou similar) com definição de recursos (jobs, pipelines) em YAML. O GitLab CI roda `databricks bundle deploy` para publicar no workspace.

1. Instalar **Databricks CLI** com suporte a Bundles (versão recente).
2. No repositório, criar a estrutura do bundle (ex.: `databricks.yml` e pastas de recursos).
3. No pipeline:

```yaml
deploy-bundle:
  stage: deploy
  image: python:3.10-slim
  before_script:
    - pip install databricks-cli
    - echo "[DEFAULT]" > ~/.databrickscfg && echo "host = $DATABRICKS_HOST" >> ~/.databrickscfg && echo "token = $DATABRICKS_TOKEN" >> ~/.databrickscfg
  script:
    - databricks bundle deploy -t production
  only:
    - main
```

(Requer que o projeto tenha sido inicializado com `databricks bundle init` e configurado para o ambiente `production`.)

### 2.5 Validação no pipeline (sem deploy)

Para apenas validar o código em todo push/merge request (lint, testes, checagem de sintaxe):

```yaml
validate:
  stage: test
  image: python:3.10
  before_script:
    - pip install -r requirements-dev.txt   # se houver
  script:
    - python -m py_compile src/config/paths.py   # exemplo
    - echo "Validação concluída"
```

---

## Parte 3 – Resumo prático

### Só usar Repos (desenvolvimento no Databricks)

1. Criar token no GitLab (read_repository).
2. Em **Databricks** → **Settings** → **Git integration** → adicionar provider **GitLab** com a URL do GitLab e o token.
3. **Repos** → **Add Repo** → colar URL do repositório GitLab → criar.
4. Desenvolver e rodar notebooks em **Repos/.../api-mxm-databricks**; usar **Pull** para atualizar a partir do GitLab.

### Repos + CI/CD (deploy a partir do GitLab)

1. Fazer os passos da Parte 1 (Repos) para desenvolvimento.
2. Criar PAT no Databricks; cadastrar `DATABRICKS_HOST` e `DATABRICKS_TOKEN` em **GitLab** → **Settings** → **CI/CD** → **Variables**.
3. Adicionar `.gitlab-ci.yml` na raiz do projeto (ex.: estágio de deploy com Databricks CLI ou Asset Bundles).
4. A cada push na branch configurada (ex.: `main`), o pipeline roda e pode atualizar o workspace ou os jobs no Databricks.

### Boas práticas

- **Não** commitar credenciais (API MXM, Databricks). Usar Secret Scope no Databricks e Variables no GitLab.
- Manter **branch principal** estável; usar branches para features e merge request antes de merge.
- Se usar push a partir do Databricks para o GitLab, usar um token com **write_repository** e manter uma política clara de quem faz push.

---

## Referências

- [Databricks – Git integration (Repos)](https://docs.databricks.com/repos/index.html)
- [Databricks – Connect to GitLab](https://docs.databricks.com/repos/git-operations-with-repos.html#gitlab)
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html)
- [Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/index.html)
- [GitLab CI/CD](https://docs.gitlab.com/ee/ci/)
