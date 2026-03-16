# Databricks notebook source

# MAGIC %md
# MAGIC # Executar Extração Incremental - Endpoints Problemáticos
# MAGIC
# MAGIC Este notebook executa extração incremental inteligente para endpoints que precisam de período.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Instalar dependências adicionais

# COMMAND ----------

%pip install requests pandas

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configurar parâmetros

# COMMAND ----------

dbutils.widgets.text("execution_type", "incremental", "Tipo (incremental/backfill)")
dbutils.widgets.text("days_back", "30", "Dias para trás (incremental)")
dbutils.widgets.text("start_date", "", "Data início (YYYY-MM-DD para backfill)")
dbutils.widgets.text("end_date", "", "Data fim (YYYY-MM-DD para backfill)")
dbutils.widgets.text("target_database", "bronze", "Database destino")

execution_type = dbutils.widgets.get("execution_type")
days_back = int(dbutils.widgets.get("days_back"))
start_date_str = dbutils.widgets.get("start_date")
end_date_str = dbutils.widgets.get("end_date")
target_database = dbutils.widgets.get("target_database")

print("📋 Parâmetros:")
print(f"   - Tipo: {execution_type}")
print(f"   - Dias para trás: {days_back}")
print(f"   - Database: {target_database}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Importar módulos

# COMMAND ----------

import sys
sys.path.append("/Workspace/thopenenergy")

from pipelines.mxm_incremental_pipeline import MXMIncrementalPipeline
from datetime import datetime, timedelta

print("✅ Módulos importados")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Inicializar pipeline

# COMMAND ----------

pipeline = MXMIncrementalPipeline(spark)
print("✅ Pipeline incremental inicializado")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Executar extração

# COMMAND ----------

results = {}

if execution_type == "incremental":
    print(f"🚀 Executando extração incremental dos últimos {days_back} dias")
    results = pipeline.run_incremental(days_back=days_back)
    
elif execution_type == "backfill":
    if not start_date_str or not end_date_str:
        raise ValueError("Para backfill, forneça start_date e end_date")
    
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()
    
    print(f"🚀 Executando backfill de {start_date} a {end_date}")
    # Se você tiver um método de backfill, use-o; caso contrário, chame run_incremental com o intervalo
    # Exemplo: results = pipeline.run_backfill(start_date, end_date)
    # Por enquanto, vamos apenas executar incremental com o intervalo total
    delta_days = (end_date - start_date).days
    results = pipeline.run_incremental(days_back=delta_days)

print(f"\n✅ Extração concluída - {len(results)} endpoints processados")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Salvar resultados

# COMMAND ----------

if results:
    pipeline.save_results(results, mode="append")
    print("✅ Resultados salvos no Delta Lake")
else:
    print("ℹ️ Nenhum dado para salvar")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Visualizar resultados

# COMMAND ----------

if results:
    for endpoint, df in results.items():
        count = df.count()
        print(f"📊 {endpoint}: {count} registros")
        
        if count > 0:
            print(f"   Primeiros registros:")
            display(df.limit(3))
            
            if "_reference_date" in df.columns:
                from pyspark.sql.functions import min, max
                date_range = df.agg(
                    min("_reference_date").alias("min_date"),
                    max("_reference_date").alias("max_date")
                ).collect()[0]
                print(f"   Período: {date_range['min_date']} a {date_range['max_date']}")
else:
    print("ℹ️ Nenhum resultado para visualizar")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Verificar erros

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
# MAGIC ## 9. Criar tabela de auditoria

# COMMAND ----------

from datetime import datetime

audit_df = spark.createDataFrame([{
    "execution_id": spark.sparkContext.applicationId,
    "execution_date": datetime.now().date(),
    "execution_type": execution_type,
    "days_back": days_back if execution_type == "incremental" else None,
    "start_date": start_date_str if execution_type == "backfill" else None,
    "end_date": end_date_str if execution_type == "backfill" else None,
    "endpoints_processed": len(results),
    "endpoints_list": str(list(results.keys())) if results else "",
    "errors_count": len(errors),
    "timestamp": datetime.now()
}])

audit_df.write.format("delta") \
    .mode("append") \
    .saveAsTable(f"{target_database}.mxm_incremental_audit")

print("✅ Auditoria salva")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Resumo final

# COMMAND ----------

print("\n" + "="*60)
print("📊 RESUMO DA EXTRAÇÃO INCREMENTAL")
print("="*60)

if results:
    total_registros = 0
    for endpoint, df in results.items():
        count = df.count()
        total_registros += count
        print(f"✅ {endpoint:<30} {count:>10} registros")
    
    print("-"*60)
    print(f"📈 TOTAL: {total_registros} registros")
else:
    print("ℹ️ Nenhum dado extraído")

print(f"⚠️  Erros: {len(errors)}")
print("="*60)

print("\n🎉 Extração incremental concluída!")