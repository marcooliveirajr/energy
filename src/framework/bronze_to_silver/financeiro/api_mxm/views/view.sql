-- View sobre camada Silver - domínio financeiro / fonte api_mxm
-- Expõe dados Silver para consumo analítico e camada Gold
-- Convenção: prefixo slv_fin_ (Silver não usa _t_; apenas Bronze usa brz_t_fin_)

-- Exemplo (ajustar nome da tabela e colunas):
-- CREATE OR REPLACE VIEW catalog.schema.vw_slv_fin_<entidade> AS
-- SELECT *
-- FROM catalog.schema.slv_fin_<entidade>;
