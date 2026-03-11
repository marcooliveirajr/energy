"""
Job Databricks: seeds_acesso_mysql_extract
MySQL (db_energia_rzk) -> Parquet na Bronze S3.

- Config: connection, target, recordsets (source_table, target_table, columns, enabled).
- Senha: Databricks Secrets (scope/key configuráveis no job).
- Performance: leitura JDBC com particionamento opcional; sem count() no loop (reduz custo).
"""

import json
import os
import uuid
from typing import Dict, List, Tuple, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp, lit, to_date, to_timestamp


# -----------------------------------------
# Config (JSON sem credentials)
# -----------------------------------------

def load_config(env: str, config_path: Optional[str] = None) -> dict:
    """Carrega JSON: connection, target, recordsets. Senha fica no Databricks Secrets."""
    path = config_path or os.environ.get("CONFIG_PATH")
    if path and os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    script_dir = os.getcwd()  # Changed from os.path.dirname(os.path.abspath(__file__))
    base_dir = os.path.dirname(script_dir)
    default = os.path.join(base_dir, "resources", f"acesso_extract_{env.lower()}.json")
    if os.path.exists(default):
        with open(default, "r", encoding="utf-8") as f:
            return json.load(f)
    for prefix in ("/dbfs/FileStore", "/Workspace"):
        candidate = f"{prefix}/acesso_extract_{env.lower()}.json"
        if os.path.exists(candidate):
            with open(candidate, "r", encoding="utf-8") as f:
                return json.load(f)
    raise FileNotFoundError(f"Config não encontrado para env={env}. Ex.: resources/acesso_extract_{env.lower()}.json")


# -----------------------------------------
# Columns: parse e cast (schema do JSON)
# -----------------------------------------

def parse_columns(columns_spec: list) -> Tuple[List[str], Dict[str, str]]:
    """columns_spec: [{name, datatype}]. Retorna (nomes, mapa nome -> datatype)."""
    if not columns_spec or not isinstance(columns_spec[0], dict):
        raise ValueError("'columns' deve ser lista de {name, datatype}.")
    names, type_map = [], {}
    for c in columns_spec:
        n, t = c.get("name"), c.get("datatype")
        if not n or not t:
            raise ValueError("Cada coluna deve ter 'name' e 'datatype'.")
        names.append(n)
        type_map[n] = t.strip().upper()
    return names, type_map


def cast_to_schema(df: DataFrame, type_map: Dict[str, str], log) -> DataFrame:
    """Ajusta tipos do DataFrame conforme datatypes do config."""
    for name, dtype in type_map.items():
        if name not in df.columns:
            log(f"[CAST] Coluna '{name}' não encontrada, ignorando.")
            continue
        dt = dtype.upper().strip()
        if dt.startswith("VARCHAR") or dt.startswith("CHAR") or dt == "TEXT":
            df = df.withColumn(name, col(name).cast("string"))
        elif dt in ("BOOLEAN", "BOOL"):
            df = df.withColumn(name, col(name).cast("boolean"))
        elif dt in ("INT", "INTEGER"):
            df = df.withColumn(name, col(name).cast("int"))
        elif dt == "BIGINT":
            df = df.withColumn(name, col(name).cast("long"))
        elif dt == "FLOAT":
            df = df.withColumn(name, col(name).cast("float"))
        elif dt in ("DOUBLE", "DOUBLE PRECISION"):
            df = df.withColumn(name, col(name).cast("double"))
        elif dt.startswith("DECIMAL"):
            try:
                inner = dt[dt.find("(") + 1 : dt.find(")")]
                p, s = int(inner.split(",")[0].strip()), int(inner.split(",")[1].strip())
            except Exception:
                p, s = 38, 10
            df = df.withColumn(name, col(name).cast(f"decimal({p},{s})"))
        elif dt == "DATE":
            df = df.withColumn(name, to_date(col(name)))
        elif dt == "TIMESTAMP":
            df = df.withColumn(name, to_timestamp(col(name)))
        else:
            df = df.withColumn(name, col(name).cast("string"))
    return df


# -----------------------------------------
# MySQL JDBC (com particionamento opcional para performance)
# -----------------------------------------

def read_mysql(
    spark: SparkSession,
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
    table: str,
    num_partitions: int = 4,
    partition_column: Optional[str] = None,
    lower_bound: Optional[int] = None,
    upper_bound: Optional[int] = None,
) -> DataFrame:
    """
    Lê tabela MySQL via JDBC.
    Com partition_column + lower_bound + upper_bound usa leitura paralela (melhor performance/custo).
    """
    url = f"jdbc:mysql://{host}:{port}/{database}?useSSL=true"
    props = {"user": user, "password": password, "driver": "com.mysql.cj.jdbc.Driver"}
    if partition_column is not None and lower_bound is not None and upper_bound is not None:
        return spark.read.jdbc(
            url=url,
            table=table,
            column=partition_column,
            lowerBound=lower_bound,
            upperBound=upper_bound,
            numPartitions=num_partitions,
            properties=props,
        )
    return spark.read.jdbc(url=url, table=table, properties=props)


# -----------------------------------------
# Main
# -----------------------------------------

def main():
    env = (os.environ.get("env") or os.environ.get("ENV") or "dev").lower()
    if "DATABRICKS_RUNTIME_VERSION" in os.environ:
        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(spark)
            w = dbutils.widgets.get("env")
            if w:
                env = w.lower()
        except Exception:
            pass

    spark = SparkSession.builder.getOrCreate()
    log = print

    log(f"[seeds_acesso_mysql_extract] env={env}")

    config = load_config(env)
    conn = config["connection"]
    target = config["target"]
    recordsets = config.get("recordsets", [])

    host_primary = conn["host_primary"]
    host_replica = conn.get("host_replica", host_primary)
    use_replica = os.environ.get("USE_REPLICA", "true").lower() == "true"
    host = host_replica if use_replica else host_primary
    port = int(conn.get("port", 3306))
    database = conn.get("database", "db_energia_rzk")
    user = conn.get("user", "amee")

    scope = os.environ.get("secret_scope", "acesso-mysql")
    key = os.environ.get("secret_key", "password")
    use_test_creds = os.environ.get("USE_TEST_CREDENTIALS", "false").lower() == "true"
    password = None
    if use_test_creds:
        try:
            import sys
            base_dir = os.path.dirname(os.getcwd())  # Changed from os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            if base_dir not in sys.path:
                sys.path.insert(0, base_dir)
            from acesso_credentials_test import MYSQL_PASSWORD
            password = MYSQL_PASSWORD
            log("[TEST] Senha carregada de acesso_credentials_test.py (apenas para teste)")
        except Exception as e:
            log(f"[TEST] Falha ao carregar acesso_credentials_test: {e}")
    if password is None:
        try:
            if "DATABRICKS_RUNTIME_VERSION" in os.environ:
                from pyspark.dbutils import DBUtils
                password = DBUtils(spark).secrets.get(scope=scope, key=key)
            else:
                password = os.environ.get("MYSQL_PASSWORD", "REPLACE_ME")
        except Exception:
            try:
                import sys
                base_dir = os.path.dirname(os.getcwd())  # Changed from os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                if base_dir not in sys.path:
                    sys.path.insert(0, base_dir)
                from acesso_credentials_test import MYSQL_PASSWORD
                password = MYSQL_PASSWORD
                log("[TEST] Senha do Databricks Secrets indisponível; usando acesso_credentials_test.py")
            except Exception:
                password = os.environ.get("MYSQL_PASSWORD", "REPLACE_ME")

    bronze_base = target["bronze_base"].rstrip("/")
    domain = target.get("domain", "acesso")

    process_id = os.environ.get("SPARK_JOB_ID") or str(uuid.uuid4())
    if "DATABRICKS_RUNTIME_VERSION" in os.environ:
        try:
            from pyspark.dbutils import DBUtils
            ctx = DBUtils(spark).notebook.entry_point.getDbutils().notebook().getContext()
            process_id = str(ctx.currentRunId().get())
        except Exception:
            pass

    ok, err = 0, 0
    for rs in recordsets:
        if rs.get("enabled", 1) != 1:
            continue
        source_table = rs["source_table"]
        target_table = rs["target_table"]
        columns_spec = rs.get("columns", [])
        if not columns_spec:
            log(f"  [SKIP] {source_table}: sem colunas")
            continue

        try:
            _, type_map = parse_columns(columns_spec)
            df = read_mysql(spark, host, port, database, user, password, source_table)
            df = df.withColumn("_ingested_at", current_timestamp()) \
                .withColumn("_source_system", lit("mysql_db_energia_rzk")) \
                .withColumn("_process_id", lit(process_id))
            df = cast_to_schema(df, type_map, log)
            out_path = f"{bronze_base}/{domain}/{target_table}"
            df.write.mode("overwrite").format("parquet").save(out_path)
            log(f"  OK {source_table} -> {out_path}")
            ok += 1
        except Exception as e:
            log(f"  ERRO {source_table}: {e}")
            err += 1
            raise

    log(f"[seeds_acesso_mysql_extract] fim. ok={ok}, erros={err}")


if __name__ == "__main__":
    main()
