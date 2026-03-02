"""
Constantes de paths e prefixos do pipeline - API MXM Databricks.
Alinhado a docs/ARQUITETURA_PIPELINE_DADOS.md e config/paths_and_prefixes.yaml.
Uso em jobs de ingestão (Bronze), transformação (Silver/Gold) e testes.
"""

# --- Paths por camada (sem Raw: dados brutos vão direto para Bronze) ---
ARQPath = "/Arquivo"
FRGPath = "/QVDForge"   # Forge: logs
TBRZPath = "/QVDBronze" # Bronze: dados brutos (destino direto da ingestão)
SLRPath = "/QVDSilver"
GLDPath = "/QVDGold"

# --- Prefixos: apenas Bronze tem "t" (tabela); Forge, Silver, Gold sem "t" ---
ForgePrefixo = "frg_fin_"
BronzePrefixo = "brz_t_fin_"   # única camada com _t_
SilverPrefixo = "slv_fin_"
GoldPrefixo = "gld_fin_"

# --- Acesso por nome de camada (útil em funções) ---
PATHS = {
    "arquivo": ARQPath,
    "forge": FRGPath,
    "bronze": TBRZPath,
    "silver": SLRPath,
    "gold": GLDPath,
}

PREFIXES = {
    "forge": ForgePrefixo,
    "bronze": BronzePrefixo,
    "silver": SilverPrefixo,
    "gold": GoldPrefixo,
}


def get_table_name(layer: str, entity: str) -> str:
    """
    Retorna o nome da tabela conforme convenção (ex.: bronze + clientes -> brz_t_fin_clientes; silver -> slv_fin_).
    layer: 'forge' | 'bronze' | 'silver' | 'gold'
    entity: nome da entidade sem prefixo (ex.: 'clientes', 'titulos_receber')
    """
    prefix = PREFIXES.get(layer.lower())
    if not prefix:
        raise ValueError(f"Camada inválida: {layer}. Use: forge, bronze, silver, gold.")
    return f"{prefix}{entity}"


def get_path(layer: str, base: str = "") -> str:
    """
    Retorna o path da camada, opcionalmente prefixado por base (ex.: mount DBFS).
    layer: 'arquivo' | 'forge' | 'bronze' | 'silver' | 'gold'
    base: path base (ex.: dbfs:/mnt/adls_mxm)
    """
    path = PATHS.get(layer.lower(), "")
    if base:
        return f"{base.rstrip('/')}{path}"
    return path
