import os
from typing import Dict

class SecretsManager:
    """Gerencia secrets para o Databricks"""
    
    @staticmethod
    def get_credentials() -> Dict[str, str]:
        """
        Obtém credenciais dos secrets do Databricks
        """
        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(spark)
            
            return {
                "username": dbutils.secrets.get(scope="mxm", key="username"),
                "password": dbutils.secrets.get(scope="mxm", key="password"),
                "environment": dbutils.secrets.get(scope="mxm", key="environment"),
                "client_id": dbutils.secrets.get(scope="mxm", key="client_id"),
                "client_secret": dbutils.secrets.get(scope="mxm", key="client_secret"),
                "token_url": dbutils.secrets.get(scope="mxm", key="token_url")
            }
        except:
            # Fallback para teste
            return {
                "username": "INTEGRACAO2",
                "password": "4ZvegmokkmLbonpW7Ci5hwgwu!",
                "environment": "MXM-THOPENERGY",
                "client_id": "projeto",
                "client_secret": "projeto",
                "token_url": "https://identityserver.mxmwebmanager.com.br/connect/token"
            }