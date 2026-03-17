from dataclasses import dataclass
from typing import Dict, List, Optional

@dataclass
class EndpointConfig:
    name: str
    method: str
    path: str
    base_url: str = "https://mxm-thopenergy.rsmbrasil.com.br"
    default_data: Optional[Dict] = None
    table_name: str = ""
    extraction_type: str = "full"  # "full" ou "incremental"

class MXMEndpoints:
    ENDPOINTS = {
        # Endpoints que funcionaram (full)
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
            extraction_type="full"
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
        # Endpoints incrementais (problemáticos)
        "consultar_fornecedor": EndpointConfig(
            name="consultar_fornecedor",
            method="POST",
            path="/webmanager/api/InterfacedoFornecedor/Consulta",
            table_name="mxm_fornecedores",
            extraction_type="incremental"
        ),
        "consultar_faturas": EndpointConfig(
            name="consultar_faturas",
            method="POST",
            path="/webmanager/api/InterfacedaFatura/ConsultaFaturas",
            table_name="mxm_faturas",
            extraction_type="incremental"
        ),
        "pedidos_vendas": EndpointConfig(
            name="pedidos_vendas",
            method="POST",  # ou GET? Vamos manter POST como no Postman
            path="/webmanager/api/InterfacedoPedidodeVenda/ConsultarPedidoVendas",
            table_name="mxm_pedidos_venda",
            extraction_type="incremental"
        ),
        "consulta_saldo_contabil": EndpointConfig(
            name="consulta_saldo_contabil",
            method="POST",
            path="/webmanager/api/InterfacedaContabilidade/ConsultaSaldo",
            table_name="mxm_saldos_contabeis",
            extraction_type="incremental"
        ),
        "titulos_receber": EndpointConfig(
            name="titulos_receber",
            method="POST",
            path="/webmanager/api/InterfacedoContasPagarReceber/ConsultaTituloReceber",
            table_name="mxm_titulos_receber",
            extraction_type="incremental"
        ),
        "titulos_pagar": EndpointConfig(
            name="titulos_pagar",
            method="POST",
            path="/webmanager/api/InterfacedoContasPagarReceber/ConsultaTituloPagar",
            table_name="mxm_titulos_pagar",
            extraction_type="incremental"
        ),
    }

    @classmethod
    def get_endpoint(cls, name: str) -> Optional[EndpointConfig]:
        return cls.ENDPOINTS.get(name)

    @classmethod
    def get_full_endpoints(cls) -> List[EndpointConfig]:
        return [e for e in cls.ENDPOINTS.values() if e.extraction_type == "full"]

    @classmethod
    def get_incremental_endpoints(cls) -> List[EndpointConfig]:
        return [e for e in cls.ENDPOINTS.values() if e.extraction_type == "incremental"]