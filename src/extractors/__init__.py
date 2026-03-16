"""
Módulo de extratores para API MXM
"""

from .base_extractor import BaseExtractor, GenericExtractor
from .cliente_extractor import ClienteExtractor
from .incremental_extractors import (
    FaturaExtractor,
    PedidosVendaExtractor,
    TitulosExtractor,
    SaldoContabilExtractor
)

__all__ = [
    'BaseExtractor',
    'GenericExtractor',
    'ClienteExtractor',
    'FaturaExtractor',
    'PedidosVendaExtractor',
    'TitulosExtractor',
    'SaldoContabilExtractor'
]