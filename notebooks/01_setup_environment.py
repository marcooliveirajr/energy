# Databricks notebook source

# MAGIC %md
# MAGIC # Setup do Ambiente

# COMMAND ----------

# Instala dependências
%pip install requests

# COMMAND ----------

# Cria databases
spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
spark.sql("CREATE DATABASE IF NOT EXISTS silver")
spark.sql("CREATE DATABASE IF NOT EXISTS gold")

print("Databases criadas com sucesso!")

# COMMAND ----------

# Configura secrets (executar apenas uma vez)
try:
    dbutils.secrets.createScope(
        scope="mxm",
        initial_manage_principal="users"
    )
    
    dbutils.secrets.put(
        scope="mxm",
        key="username",
        string_value="INTEGRACAO2"
    )
    
    dbutils.secrets.put(
        scope="mxm",
        key="password", 
        string_value="4ZvegmokkmLbonpW7Ci5hwgwu!"
    )
    
    dbutils.secrets.put(
        scope="mxm",
        key="environment",
        string_value="MXM-THOPENERGY"
    )
    
    dbutils.secrets.put(
        scope="mxm",
        key="client_id",
        string_value="projeto"
    )
    
    dbutils.secrets.put(
        scope="mxm",
        key="client_secret",
        string_value="projeto"
    )
    
    dbutils.secrets.put(
        scope="mxm",
        key="token_url",
        string_value="https://identityserver.mxmwebmanager.com.br/connect/token"
    )
    
    print("Secrets configurados com sucesso!")
    
except Exception as e:
    print(f"Erro: {str(e)}")
    print("Se já existir, ignore este erro")