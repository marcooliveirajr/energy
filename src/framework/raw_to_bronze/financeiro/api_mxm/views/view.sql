-- View sobre camada Bronze - domínio financeiro / fonte api_mxm
-- Expõe dados Bronze para consumo interno (ex.: validação, bronze_to_silver)
-- Convenção: prefixo brz_t_fin_ (única camada que usa _t_ = tabela)

-- Exemplo (ajustar nome da tabela e colunas):
-- CREATE OR REPLACE VIEW catalog.schema.vw_brz_t_fin_<entidade> AS
-- SELECT *
--   , _ingested_at
--   , _source_file
-- FROM catalog.schema.brz_t_fin_<entidade>;
