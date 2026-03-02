-- Schema updates: Silver (slv_fin_* – sem _t_, apenas Bronze usa _t_)
-- Domínio: financeiro / Fonte: api_mxm
-- Substituir table_name pelo nome real da tabela (ex.: slv_fin_clientes)

-- Exemplo: CREATE ou ALTER TABLE para tabela Silver
-- CREATE TABLE IF NOT EXISTS catalog.schema.slv_fin_<entidade> (
--   ... colunas padronizadas ...
-- )
-- USING DELTA
-- LOCATION 'path/QVDSilver/...';
