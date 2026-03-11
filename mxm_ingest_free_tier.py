# Databricks notebook source
# MAGIC %md
# MAGIC # Ingestão API MXM → S3 (Bronze + Forge) — Databricks Free
# MAGIC
# MAGIC **Uso:** Preencha as variáveis na célula de configuração abaixo.  
# MAGIC **Atenção:** Não use este arquivo com credenciais reais em repositório público. Em produção, use Databricks Secrets.
# MAGIC
# MAGIC %md
# MAGIC ## 1. Configuração (preencher e executar esta célula primeiro)
# MAGIC
# MAGIC # =============================================================================
# MAGIC # CONFIGURAÇÃO — Databricks Free (sem Secrets). Apenas para testes.
# MAGIC # Não faça commit com credenciais reais. Em produção use Secret Scope.
# MAGIC # =============================================================================
# MAGIC
# MAGIC ENV_LEVEL = "DEV"
# MAGIC
# MAGIC # Paths S3
# MAGIC BRONZE_BASE = f"s3://thopenenergy/{ENV_LEVEL}/bronze"
# MAGIC FORGE_BASE = f"s3://thopenenergy/{ENV_LEVEL}/forge"
# MAGIC
# MAGIC # API MXM — preencha com seus dados de teste
# MAGIC MXM_BASE_URL = "https://mxm-thopenergy.rsmbrasil.com.br"
# MAGIC MXM_USERNAME = "INTEGRACAO2"           # substituir se necessário
# MAGIC MXM_PASSWORD = "4ZvegmokkmLbonpW7Ci5hwgwu!"        # substituir pela senha real
# MAGIC MXM_ENVIRONMENT = "MXM-THOPENERGY"
# MAGIC
# MAGIC # Opcional: usar DBFS local se S3 não estiver disponível no Free (ex.: /tmp/mxm_bronze)
# MAGIC USE_DBFS_FALLBACK = False
# MAGIC DBFS_BRONZE = "/tmp/mxm_bronze"
# MAGIC DBFS_FORGE = "/tmp/mxm_forge"
# MAGIC
# MAGIC # =============================================================================
# MAGIC
# MAGIC def _get_bronze_base():
# MAGIC     if USE_DBFS_FALLBACK:
# MAGIC         return DBFS_BRONZE
# MAGIC     return BRONZE_BASE
# MAGIC
# MAGIC def _get_forge_base():
# MAGIC     if USE_DBFS_FALLBACK:
# MAGIC         return DBFS_FORGE
# MAGIC     return FORGE_BASE
# MAGIC
# MAGIC %md
# MAGIC ## 2. Funções de ingestão
# MAGIC
# MAGIC import json
# MAGIC import uuid
# MAGIC from datetime import datetime
# MAGIC
# MAGIC import requests
# MAGIC from pyspark.sql import functions as F
# MAGIC
# MAGIC
# MAGIC def build_auth_block():
# MAGIC     return {
# MAGIC         "AutheticationToken": {
# MAGIC             "Username": MXM_USERNAME,
# MAGIC             "Password": MXM_PASSWORD,
# MAGIC             "EnvironmentName": MXM_ENVIRONMENT,
# MAGIC         }
# MAGIC     }
# MAGIC
# MAGIC
# MAGIC def call_mxm(path: str, data: dict = None, timeout: int = 60) -> dict:
# MAGIC     """Chama um endpoint MXM (POST) com o corpo padrão + Data."""
# MAGIC     body = build_auth_block()
# MAGIC     body["Data"] = data or {}
# MAGIC     url = f"{MXM_BASE_URL}{path}"
# MAGIC     resp = requests.post(url, json=body, timeout=timeout)
# MAGIC     resp.raise_for_status()
# MAGIC     try:
# MAGIC         return resp.json()
# MAGIC     except Exception:
# MAGIC         return {"raw": resp.text}
# MAGIC
# MAGIC
# MAGIC def json_to_df(payload: dict | list):
# MAGIC     """Converte a resposta JSON da API em DataFrame Spark."""
# MAGIC     if isinstance(payload, dict):
# MAGIC         data_block = payload.get("Data") or payload.get("Result") or payload
# MAGIC     else:
# MAGIC         data_block = payload
# MAGIC
# MAGIC     if isinstance(data_block, dict):
# MAGIC         records = (
# MAGIC             data_block.get("Lista")
# MAGIC             or data_block.get("Items")
# MAGIC             or data_block.get("Registros")
# MAGIC             or [data_block]
# MAGIC         )
# MAGIC     else:
# MAGIC         records = data_block if isinstance(data_block, list) else []
# MAGIC
# MAGIC     rdd = spark.sparkContext.parallelize([json.dumps(records)])
# MAGIC     return spark.read.json(rdd)
# MAGIC
# MAGIC
# MAGIC def write_bronze(df, table_name: str):
# MAGIC     """Escreve DataFrame na camada Bronze (parquet, append)."""
# MAGIC     base = _get_bronze_base()
# MAGIC     path = f"{base.rstrip('/')}/{table_name}"
# MAGIC     now = datetime.utcnow()
# MAGIC     (
# MAGIC         df.withColumn("_ingestion_ts_utc", F.lit(now.isoformat()))
# MAGIC         .write.mode("append")
# MAGIC         .format("parquet")
# MAGIC         .save(path)
# MAGIC     )
# MAGIC
# MAGIC
# MAGIC def log_forge(
# MAGIC     endpoint_name: str,
# MAGIC     table_name: str,
# MAGIC     status: str,
# MAGIC     rows: int,
# MAGIC     error_message: str = None,
# MAGIC     exec_id: str = None,
# MAGIC ):
# MAGIC     """Registra log da execução na camada Forge (parquet)."""
# MAGIC     exec_id = exec_id or str(uuid.uuid4())
# MAGIC     now = datetime.utcnow()
# MAGIC     base = _get_forge_base()
# MAGIC     log_path = f"{base.rstrip('/')}/frg_fin_pipeline_log"
# MAGIC
# MAGIC     log_df = spark.createDataFrame(
# MAGIC         [
# MAGIC             {
# MAGIC                 "execution_id": exec_id,
# MAGIC                 "endpoint": endpoint_name,
# MAGIC                 "table_name": table_name,
# MAGIC                 "status": status,
# MAGIC                 "rows": rows,
# MAGIC                 "error_message": error_message or "",
# MAGIC                 "created_at_utc": now.isoformat(),
# MAGIC             }
# MAGIC         ]
# MAGIC     )
# MAGIC     log_df.write.mode("append").format("parquet").save(log_path)
# MAGIC
# MAGIC %md
# MAGIC ## 3. Definição dos endpoints POST (baseado na collection Postman)
# MAGIC
# MAGIC ENDPOINTS = [
# MAGIC     {"name": "ObterClientes", "path": "/webmanager/api/InterfacedoCliente/ObterClientesPost", "table": "brz_t_fin_clientes", "data": {}},
# MAGIC     {"name": "ConsultarFilial", "path": "/webmanager/api/InterfaceEmpresaFilial/ConsultarFilial", "table": "brz_t_fin_filiais", "data": {}},
# MAGIC     {"name": "ConsultarEmpresa", "path": "/webmanager/api/InterfaceEmpresaFilial/ConsultarEmpresa", "table": "brz_t_fin_empresas", "data": {}},
# MAGIC     {"name": "ConsultarFornecedor", "path": "/webmanager/api/InterfacedoFornecedor/Consulta", "table": "brz_t_fin_fornecedores", "data": {"Codigo": "26562346000177"}},
# MAGIC     {"name": "ConsultaLancamentos", "path": "/webmanager/api/InterfacedoFornecedor/Consulta", "table": "brz_t_fin_lancamentos", "data": {}},
# MAGIC     {"name": "ConsultaOrdemFaturamento", "path": "/webmanager/api/InterfacedaFatura/ConsultaOrdemdeFaturamento", "table": "brz_t_fin_ordem_faturamento", "data": {"SqProcesso": "", "CodigoEmpresa": "", "CodigoFilial": "", "CodigoFatura": "249", "DataFatura": "", "DataEntrada": "", "TipoOperacao": "", "Natureza": "", "CodigoEstoque": "", "CodigoAlmoxarifado": ""}},
# MAGIC     {"name": "ConsultaFaturas", "path": "/webmanager/api/InterfacedaFatura/ConsultaFaturas", "table": "brz_t_fin_faturas", "data": {"CodigoEmpresa": "0702", "DataFatura": "07/01/2026"}},
# MAGIC     {"name": "ConsultaCentrodeCusto", "path": "/webmanager/api/CentrodeCusto/ConsultaCentrodeCusto", "table": "brz_t_fin_centro_custo", "data": {"CodigodoPlanodeCentrodeCusto": "", "CodigodoCentrodeCusto": "", "Descricao": "", "DescricaoAlternativa": "", "Tipo": "", "TipoOrcamentario": "", "Status": "", "DataHoraPesquisaInicial": "01/01/2026 07:00:00", "DataHoraPesquisaFinal": "01/02/2026 18:00:00"}},
# MAGIC     {"name": "ConsultaPlanoContasReduzido", "path": "/webmanager/api/InterfacedaContabilidade/ConsultaPlanoContasContabeisReduzido", "table": "brz_t_fin_plano_contas_reduzido", "data": {}},
# MAGIC     {"name": "ConsultaSaldoContabil", "path": "/webmanager/api/InterfacedaContabilidade/ConsultaSaldo", "table": "brz_t_fin_saldo_contabil", "data": {"CodigoEmpresa": "0101", "AnoMesDe": "202601", "AnoMesAte": "202612", "CodigoContaContabil": "21201999999"}},
# MAGIC     {"name": "Produto_Servico", "path": "/webmanager/api/InterfacedoProduto/ConsultaProdutoServico", "table": "brz_t_fin_produtos_servicos", "data": {}},
# MAGIC     {"name": "Tipo_Operacao", "path": "/webmanager/api/TipodeOperacao/RetornaTodosTiposdeOperacao", "table": "brz_t_fin_tipo_operacao", "data": {}},
# MAGIC     {"name": "Titulos_Receber", "path": "/webmanager/api/InterfacedoContasPagarReceber/ConsultaTituloReceber", "table": "brz_t_fin_titulos_receber", "data": {"Origem": "", "Documento": "", "Cliente": "00001180000126", "Fornecedor": "", "EmpresaEmitente": "", "Filial": "", "DataDe": "", "DataAte": "", "DataCompetenciaDe": "", "DataCompetenciaAte": "", "NaoListarMovTesourariaDeTitulosPagos": "", "ContaCorrente": "", "NumeroLancamento": "", "Titulo": ""}},
# MAGIC     {"name": "Titulos_Pagar", "path": "/webmanager/api/api/InterfacedoContasPagarReceber/ConsultaTituloPagar", "table": "brz_t_fin_titulos_pagar", "data": {"Origem": "", "Documento": "", "Cliente": "00001180000126", "Fornecedor": "", "EmpresaEmitente": "", "Filial": "", "DataDe": "", "DataAte": "", "DataCompetenciaDe": "", "DataCompetenciaAte": "", "NaoListarMovTesourariaDeTitulosPagos": "", "ContaCorrente": "", "NumeroLancamento": "", "Titulo": ""}},
# MAGIC ]
# MAGIC
# MAGIC # Remover duplicata de ordem faturamento (já existe com outro nome)
# MAGIC _seen = set()
# MAGIC ENDPOINTS_UNIQ = []
# MAGIC for ep in ENDPOINTS:
# MAGIC     key = (ep["path"], ep["table"])
# MAGIC     if key in _seen:
# MAGIC         continue
# MAGIC     _seen.add(key)
# MAGIC     ENDPOINTS_UNIQ.append(ep)
# MAGIC
# MAGIC %md
# MAGIC ## 4. Executar pipeline
# MAGIC
# MAGIC for ep in ENDPOINTS_UNIQ:
# MAGIC     exec_id = str(uuid.uuid4())
# MAGIC     endpoint_name = ep["name"]
# MAGIC     table_name = ep["table"]
# MAGIC
# MAGIC     try:
# MAGIC         print(f"Executando: {endpoint_name} -> {table_name}")
# MAGIC         resp_json = call_mxm(ep["path"], ep.get("data"))
# MAGIC         df = json_to_df(resp_json)
# MAGIC         rows = df.count()
# MAGIC         print(f"  Linhas: {rows}")
# MAGIC
# MAGIC         if rows > 0:
# MAGIC             write_bronze(df, table_name)
# MAGIC         log_forge(endpoint_name, table_name, "SUCCESS", rows, None, exec_id)
# MAGIC
# MAGIC     except Exception as e:
# MAGIC         error_msg = str(e)[:4000]
# MAGIC         print(f"  ERRO: {error_msg}")
# MAGIC         log_forge(endpoint_name, table_name, "ERROR", 0, error_msg, exec_id)
# MAGIC         raise
# MAGIC
# MAGIC print("Pipeline concluído.")
