# Databricks notebook source

# MAGIC %md
# MAGIC # Executar Extração MXM API - Endpoints Normais
# MAGIC
# MAGIC Este notebook executa a extração dos endpoints que já funcionaram

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configurar parâmetros

# COMMAND ----------

dbutils.widgets.text("target_database", "bronze", "Database destino")

target_database = dbutils.widgets.get("target_database")

print("📋 Parâmetros:")
print(f"   - Database: {target_database}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Importar módulos

# COMMAND ----------

import sys
sys.path.append("/Workspace/Repos/mxm_api_extraction")

from pipelines.mxm_extraction_pipeline import MXMExtractionPipeline
from config.endpoints_config import MXMEndpoints

print("✅ Módulos importados")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Inicializar pipeline

# COMMAND ----------

# DEFININDO A VARIÁVEL pipeline AQUI!
pipeline = MXMExtractionPipeline(spark)
print("✅ Pipeline inicializado")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Executar extração dos endpoints normais

# COMMAND ----------

print("🚀 Iniciando extração de endpoints normais...")

# Executa apenas endpoints normais
results = pipeline.run_normal_extraction()

print(f"✅ Extração concluída - {len(results)} endpoints processados")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Salvar resultados

# COMMAND ----------

if results:
    pipeline.save_results(results, mode="append")
    print("✅ Resultados salvos no Delta Lake")
else:
    print("ℹ️ Nenhum dado para salvar")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Visualizar resultados

# COMMAND ----------

for endpoint, df in results.items():
    count = df.count()
    print(f"📊 {endpoint}: {count} registros")
    
    if count > 0:
        print(f"   Primeiros registros:")
        display(df.limit(3))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Verificar erros

# COMMAND ----------

errors = pipeline.get_extraction_log()
if errors:
    print("⚠️ Erros encontrados:")
    for error in errors:
        print(f"   - {error}")
else:
    print("✅ Nenhum erro encontrado")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Criar tabela de auditoria

# COMMAND ----------

from datetime import datetime

if results:
    audit_df = spark.createDataFrame([{
        "execution_id": spark.sparkContext.applicationId,
        "execution_date": datetime.now().date(),
        "execution_type": "normal",
        "endpoints_processed": len(results),
        "endpoints_list": str(list(results.keys())),
        "errors_count": len(errors),
        "timestamp": datetime.now()
    }])

    audit_df.write.format("delta") \
        .mode("append") \
        .saveAsTable(f"{target_database}.mxm_extraction_audit")

    print("✅ Auditoria salva")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Resumo final

# COMMAND ----------

print("\n" + "="*60)
print("📊 RESUMO DA EXTRAÇÃO - ENDPOINTS NORMAIS")
print("="*60)

total_registros = 0
for endpoint, df in results.items():
    count = df.count()
    total_registros += count
    print(f"✅ {endpoint:<30} {count:>10} registros")

print("-"*60)
print(f"📈 TOTAL: {total_registros} registros")
print(f"⚠️  Erros: {len(errors)}")
print("="*60)

print("\n🎉 Extração concluída com sucesso!")