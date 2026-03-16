from dataclasses import dataclass
from typing import Dict, List, Optional, Any
from datetime import datetime

@dataclass
class EndpointConfig:
    """Configuração para cada endpoint da API"""
    name: str
    method: str
    path: str
    base_url: str = "https://mxm-thopenergy.rsmbrasil.com.br"
    default_data: Optional[Dict[str, Any]] = None
    table_name: str = ""
    extraction_type: str = "full"  # full, incremental, or problematic
    requires_date_range: bool = False  # Se precisa de período
    days_back: int = 30  # Dias para trás na extração incremental
    
class MXMEndpoints:
    """Centraliza todas as configurações dos endpoints"""
    
    DEFAULT_AUTH = {
        "Username": "INTEGRACAO2",
        "Password": "4ZvegmokkmLbonpW7Ci5hwgwu!",
        "EnvironmentName": "MXM-THOPENERGY"
    }
    
    ENDPOINTS = {
        # ✅ ENDPOINTS QUE FUNCIONAM (manter como estão)
        "obter_clientes": EndpointConfig(
            name="obter_clientes",
            method="POST",
            path="/webmanager/api/InterfacedoCliente/ObterClientesPost",
            table_name="mxm_clientes",
            extraction_type="full"
        ),
        "consultar_filial": EndpointConfig(
            name="consultar_filial",
            method="POST",
            path="/webmanager/api/InterfaceEmpresaFilial/ConsultarFilial",
            table_name="mxm_filiais",
            extraction_type="full"
        ),
        "consultar_empresa": EndpointConfig(
            name="consultar_empresa",
            method="POST",
            path="/webmanager/api/InterfaceEmpresaFilial/ConsultarEmpresa",
            table_name="mxm_empresas",
            extraction_type="full"
        ),
        "consulta_centro_custo": EndpointConfig(
            name="consulta_centro_custo",
            method="POST",
            path="/webmanager/api/CentrodeCusto/ConsultaCentrodeCusto",
            table_name="mxm_centros_custo",
            extraction_type="full"  # Já funcionou como full
        ),
        "consulta_plano_contas": EndpointConfig(
            name="consulta_plano_contas",
            method="POST",
            path="/webmanager/api/InterfacedaContabilidade/ConsultaPlanoContasContabeisReduzido",
            table_name="mxm_plano_contas",
            extraction_type="full"
        ),
        "produto_servico": EndpointConfig(
            name="produto_servico",
            method="POST",
            path="/webmanager/api/InterfacedoProduto/ConsultaProdutoServico",
            table_name="mxm_produtos_servicos",
            extraction_type="full"
        ),
        "tipo_operacao": EndpointConfig(
            name="tipo_operacao",
            method="POST",
            path="/webmanager/api/TipodeOperacao/RetornaTodosTiposdeOperacao",
            table_name="mxm_tipos_operacao",
            extraction_type="full"
        ),
        "ordem_faturamento": EndpointConfig(
            name="ordem_faturamento",
            method="POST",
            path="/webmanager/api/InterfacedaFatura/ConsultaOrdemdeFaturamento",
            table_name="mxm_ordens_faturamento",
            extraction_type="full"
        ),
        
        # ⚠️ ENDPOINTS PROBLEMÁTICOS (vão para o pipeline incremental)
        "consultar_fornecedor": EndpointConfig(
            name="consultar_fornecedor",
            method="POST",
            path="/webmanager/api/InterfacedoFornecedor/Consulta",
            table_name="mxm_fornecedores",
            extraction_type="problematic",
            requires_date_range=False,  # Não precisa data, mas é pesado
            days_back=0
        ),
        "consultar_faturas": EndpointConfig(
            name="consultar_faturas",
            method="POST",
            path="/webmanager/api/InterfacedaFatura/ConsultaFaturas",
            table_name="mxm_faturas",
            extraction_type="problematic",
            requires_date_range=True,
            days_back=90  # Últimos 90 dias
        ),
        "pedidos_vendas": EndpointConfig(
            name="pedidos_vendas",
            method="POST",  # Mudamos de GET para POST
            path="/webmanager/api/InterfacedoPedidodeVenda/ConsultarPedidoVendas",
            table_name="mxm_pedidos_venda",
            extraction_type="problematic",
            requires_date_range=True,
            days_back=30  # Últimos 30 dias
        ),
        "consulta_saldo_contabil": EndpointConfig(
            name="consulta_saldo_contabil",
            method="POST",
            path="/webmanager/api/InterfacedaContabilidade/ConsultaSaldo",
            table_name="mxm_saldos_contabeis",
            extraction_type="problematic",
            requires_date_range=True,
            days_back=90  # Últimos 90 dias
        ),
        "titulos_receber": EndpointConfig(
            name="titulos_receber",
            method="POST",
            path="/webmanager/api/InterfacedoContasPagarReceber/ConsultaTituloReceber",
            table_name="mxm_titulos_receber",
            extraction_type="problematic",
            requires_date_range=True,
            days_back=30  # Últimos 30 dias
        ),
        "titulos_pagar": EndpointConfig(
            name="titulos_pagar",
            method="POST",
            path="/webmanager/api/api/InterfacedoContasPagarReceber/ConsultaTituloPagar",
            table_name="mxm_titulos_pagar",
            extraction_type="problematic",
            requires_date_range=True,
            days_back=30  # Últimos 30 dias
        )
    }
    
    @classmethod
    def get_endpoint(cls, name: str) -> EndpointConfig:
        return cls.ENDPOINTS.get(name)
    
    @classmethod
    def get_all_endpoints(cls) -> List[EndpointConfig]:
        return list(cls.ENDPOINTS.values())
    
    @classmethod
    def get_normal_endpoints(cls) -> List[EndpointConfig]:
        """Retorna apenas endpoints que funcionam normalmente"""
        return [e for e in cls.ENDPOINTS.values() if e.extraction_type == "full"]
    
    @classmethod
    def get_problematic_endpoints(cls) -> List[EndpointConfig]:
        """Retorna endpoints problemáticos que precisam de incremental"""
        return [e for e in cls.ENDPOINTS.values() if e.extraction_type == "problematic"]