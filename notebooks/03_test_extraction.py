# Databricks notebook source

%pip install requests pandas

import sys
sys.path.append("/Workspace/thopenenergy")

from pipelines.mxm_incremental_pipeline import MXMIncrementalPipeline
from datetime import datetime, timedelta

# Reinicia kernel para carregar novas definições
dbutils.library.restartPython()

# COMMAND ----------

pipeline = MXMIncrementalPipeline(spark)
print("✅ Pipeline inicializado")

# COMMAND ----------

# Testar com poucos dias (ex: 3)
dias = 3
resultados = pipeline.run_incremental(days_back=dias)

for endpoint, df in resultados.items():
    print(f"{endpoint}: {df.count()} registros")
    display(df.limit(3))

# COMMAND ----------

# Verificar erros
erros = pipeline.get_extraction_log()
if erros:
    print("Erros:", erros)
else:
    print("Sem erros.")