# Databricks notebook source

# MAGIC %md
# MAGIC # Teste Rápido da Extração
# MAGIC
# MAGIC Este notebook testa os pipelines

# COMMAND ----------

# Importar módulos - APENAS A RAIZ DO PROJETO
import sys
sys.path.append("/Workspace/thopenenergy")

from pipelines.mxm_extraction_pipeline import MXMExtractionPipeline
from pipelines.mxm_incremental_pipeline import MXMIncrementalPipeline
from config.endpoints_config import MXMEndpoints

print("✅ Módulos importados")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Testar Pipeline Normal

# COMMAND ----------

# Inicializar pipeline normal
normal_pipeline = MXMExtractionPipeline(spark)
print("✅ Pipeline normal inicializado")

# Testar um endpoint normal
#config = MXMEndpoints.get_endpoint("obter_clientes")
#if config:
#    print(f"\n🔍 Testando endpoint: {config.name}")
#    df = normal_pipeline.extract_endpoint(config)
#    print(f"📊 Registros: {df.count()}")
#    if df.count() > 0:
#        display(df.limit(3))

from src.extractors.incremental_extractors import FaturaExtractor
config = MXMEndpoints.get_endpoint("consultar_faturas")
extractor = FaturaExtractor(pipeline.token_manager, config, pipeline.secrets, spark)
raw_data = extractor.extract(data_inicio="01/02/2026", data_fim="28/02/2026")
print(f"Registros: {len(raw_data)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Testar Pipeline Incremental

# COMMAND ----------

# Inicializar pipeline incremental
incremental_pipeline = MXMIncrementalPipeline(spark)
print("✅ Pipeline incremental inicializado")

# Testar extração dos últimos 3 dias (método run_incremental, não run_daily_incremental)
print("\n🔍 Testando extração incremental (3 dias)...")
results = incremental_pipeline.run_incremental(days_back=3)

for endpoint, df in results.items():
    print(f"📊 {endpoint}: {df.count()} registros")
    if df.count() > 0:
        display(df.limit(2))