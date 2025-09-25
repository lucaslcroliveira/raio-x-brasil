# %%
import basedosdados as bd

billing_id = 'teste-314900'

# Public Security query
query = """
  SELECT
    dados.quantidade_mortes_intervencao_policial_civil_fora_de_servico as quantidade_mortes_intervencao_policial_civil_fora_de_servico,
    dados.quantidade_feminicidio as quantidade_feminicidio,
    dados.quantidade_mortes_intervencao_policial_militar_fora_de_servico as quantidade_mortes_intervencao_policial_militar_fora_de_servico,
    dados.quantidade_furto_veiculos as quantidade_furto_veiculos,
    dados.quantidade_mortes_intervencao_policial_civil_em_servico as quantidade_mortes_intervencao_policial_civil_em_servico,
    dados.quantidade_estupro as quantidade_estupro,
    dados.quantidade_morte_policiais_civis_confronto_em_servico as quantidade_morte_policiais_civis_confronto_em_servico,
    dados.quantidade_mortes_intervencao_policial_militar_em_servico as quantidade_mortes_intervencao_policial_militar_em_servico,
    dados.quantidade_mortes_policiais_confronto as quantidade_mortes_policiais_confronto,
    dados.quantidade_mortes_violentas_intencionais as quantidade_mortes_violentas_intencionais,
    dados.quantidade_posse_uso_entorpecente as quantidade_posse_uso_entorpecente,
    dados.quantidade_morte_policiais_militares_fora_de_servico as quantidade_morte_policiais_militares_fora_de_servico,
    dados.quantidade_morte_policiais_civis_fora_de_servico as quantidade_morte_policiais_civis_fora_de_servico,
    dados.quantidade_latrocinio as quantidade_latrocinio,
    dados.quantidade_porte_ilegal_arma_de_fogo as quantidade_porte_ilegal_arma_de_fogo,
    dados.quantidade_mortes_intervencao_policial as quantidade_mortes_intervencao_policial,
    dados.ano as ano,
    dados.quantidade_roubo_furto_veiculos as quantidade_roubo_furto_veiculos,
    dados.quantidade_posse_ilegal_arma_de_fogo as quantidade_posse_ilegal_arma_de_fogo,
    dados.quantidade_lesao_corporal_dolosa_violencia_domestica as quantidade_lesao_corporal_dolosa_violencia_domestica,
    dados.quantidade_trafico_entorpecente as quantidade_trafico_entorpecente,
    dados.quantidade_roubo_veiculos as quantidade_roubo_veiculos,
    dados.quantidade_lesao_corporal_morte as quantidade_lesao_corporal_morte,
    dados.proporcao_mortes_intenvencao_policial_x_mortes_violentas_intencionais as proporcao_mortes_intenvencao_policial_x_mortes_violentas_intencionais,
    dados.quantidade_morte_policiais_militares_confronto_em_servico as quantidade_morte_policiais_militares_confronto_em_servico,
    dados.quantidade_posse_ilegal_porte_ilegal_arma_de_fogo as quantidade_posse_ilegal_porte_ilegal_arma_de_fogo,
    dados.sigla_uf AS sigla_uf,
    diretorio_sigla_uf.nome AS sigla_uf_nome,
    dados.id_municipio AS id_municipio,
    diretorio_id_municipio.nome AS id_municipio_nome,
    dados.grupo as grupo,
    dados.quantidade_homicidio_doloso as quantidade_homicidio_doloso
FROM `basedosdados.br_fbsp_absp.municipio` AS dados
LEFT JOIN (SELECT DISTINCT sigla,nome  FROM `basedosdados.br_bd_diretorios_brasil.uf`) AS diretorio_sigla_uf
    ON dados.sigla_uf = diretorio_sigla_uf.sigla
LEFT JOIN (SELECT DISTINCT id_municipio,nome  FROM `basedosdados.br_bd_diretorios_brasil.municipio`) AS diretorio_id_municipio
    ON dados.id_municipio = diretorio_id_municipio.id_municipio
"""

df_public_security = bd.read_sql(query = query, billing_project_id = billing_id)

# Mortality %%
query_mortality = """
  WITH 
dicionario_sexo AS (
    SELECT
        chave AS chave_sexo,
        valor AS descricao_sexo
    FROM `basedosdados.br_ms_sim.dicionario`
    WHERE
        TRUE
        AND nome_coluna = 'sexo'
        AND id_tabela = 'municipio_causa_idade_sexo_raca'
),
dicionario_raca_cor AS (
    SELECT
        chave AS chave_raca_cor,
        valor AS descricao_raca_cor
    FROM `basedosdados.br_ms_sim.dicionario`
    WHERE
        TRUE
        AND nome_coluna = 'raca_cor'
        AND id_tabela = 'municipio_causa_idade_sexo_raca'
)
SELECT
    dados.ano as ano,
    dados.sigla_uf AS sigla_uf,
    diretorio_sigla_uf.nome AS sigla_uf_nome,
    dados.id_municipio AS id_municipio,
    diretorio_id_municipio.nome AS id_municipio_nome,
    dados.causa_basica AS causa_basica,
    diretorio_causa_basica.descricao_subcategoria AS causa_basica_descricao_subcategoria,
    diretorio_causa_basica.descricao_categoria AS causa_basica_descricao_categoria,
    diretorio_causa_basica.descricao_capitulo AS causa_basica_descricao_capitulo,
    dados.idade as idade,
    descricao_sexo AS sexo,
    descricao_raca_cor AS raca_cor,
    dados.numero_obitos as numero_obitos
FROM `basedosdados.br_ms_sim.municipio_causa_idade_sexo_raca` AS dados
LEFT JOIN (SELECT DISTINCT sigla,nome  FROM `basedosdados.br_bd_diretorios_brasil.uf`) AS diretorio_sigla_uf
    ON dados.sigla_uf = diretorio_sigla_uf.sigla
LEFT JOIN (SELECT DISTINCT id_municipio,nome  FROM `basedosdados.br_bd_diretorios_brasil.municipio`) AS diretorio_id_municipio
    ON dados.id_municipio = diretorio_id_municipio.id_municipio
LEFT JOIN (SELECT DISTINCT subcategoria,descricao_subcategoria,descricao_categoria,descricao_capitulo  FROM `basedosdados.br_bd_diretorios_brasil.cid_10`) AS diretorio_causa_basica
    ON dados.causa_basica = diretorio_causa_basica.subcategoria
LEFT JOIN `dicionario_sexo`
    ON dados.sexo = chave_sexo
LEFT JOIN `dicionario_raca_cor`
    ON dados.raca_cor = chave_raca_cor
"""

df_mortality = bd.read_sql(query = query_mortality, billing_project_id = billing_id)