from pyspark.sql import SparkSession
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class CheckpointManager:
    """
    Gerencia checkpoints de extração usando uma tabela Delta.
    Permite saber quais períodos já foram processados.
    """
    def __init__(self, spark: SparkSession, database: str = "bronze"):
        self.spark = spark
        self.database = database
        self.table_name = f"{database}.mxm_extraction_checkpoints"
        self._ensure_table()

    def _ensure_table(self):
        """Cria a tabela de checkpoints se não existir."""
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                endpoint STRING,
                parameter_key STRING,
                last_value STRING,
                last_success TIMESTAMP,
                created_at TIMESTAMP
            ) USING DELTA
        """)

    def get_last_checkpoint(self, endpoint: str, parameter_key: str) -> str | None:
        """Retorna o último valor processado para um endpoint e chave."""
        df = self.spark.sql(f"""
            SELECT last_value
            FROM {self.table_name}
            WHERE endpoint = '{endpoint}'
              AND parameter_key = '{parameter_key}'
            ORDER BY last_success DESC
            LIMIT 1
        """)
        rows = df.collect()
        return rows[0]["last_value"] if rows else None

    def update_checkpoint(self, endpoint: str, parameter_key: str, last_value: str):
        """Registra que um período foi processado com sucesso."""
        now = datetime.now()
        data = [(endpoint, parameter_key, last_value, now, now)]
        df = self.spark.createDataFrame(data, schema="""
            endpoint STRING,
            parameter_key STRING,
            last_value STRING,
            last_success TIMESTAMP,
            created_at TIMESTAMP
        """)
        df.write.format("delta").mode("append").saveAsTable(self.table_name)
        logger.info(f"Checkpoint: {endpoint} / {parameter_key} = {last_value}")