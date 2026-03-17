# Databricks notebook source

# MAGIC %md
# MAGIC # Teste do Extrator de Faturas (diário por empresa)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configurar ambiente

# COMMAND ----------

%pip install requests pandas

# COMMAND ----------

import sys
sys.path.append("/Workspace/thopenenergy")

from pipelines.mxm_incremental_pipeline import MXMIncrementalPipeline
from datetime import datetime, timedelta

# Reinicia o kernel para carregar novas definições (se necessário)
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Inicializar pipeline

# COMMAND ----------

pipeline = MXMIncrementalPipeline(spark)
print("✅ Pipeline inicializado")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Executar extração de faturas (últimos 3 dias)

# COMMAND ----------

dias = 3
df_faturas = pipeline.run_faturas(days_back=dias)

print(f"📊 Total de registros: {df_faturas.count()}")
display(df_faturas.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Salvar resultados (opcional)

# COMMAND ----------

# pipeline.save_results(df_faturas, table_name="mxm_faturas", mode="append")