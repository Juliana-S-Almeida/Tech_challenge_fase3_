-- =============================================================================
-- Tech Challenge Fase 3 - State of Data Brasil
-- Consultas analiticas via Amazon Athena
-- =============================================================================
-- Todas as queries abaixo foram executadas no Athena Query Editor, consultando
-- as tabelas da camada Gold (banco gold_db) e da camada Silver (banco silver_db),
-- catalogadas via AWS Glue Data Catalog. Servem tanto para validacao cruzada
-- dos indicadores do relatorio executivo quanto para demonstrar a etapa de
-- "consultas analiticas" exigida pelo desafio.
--
-- Como rodar: Console AWS -> Amazon Athena -> Editor de consultas -> selecionar
-- o banco de dados indicado em cada secao -> colar e executar.
-- =============================================================================


-- =============================================================================
-- 0. VALIDACAO GERAL (banco: silver_db e gold_db)
-- =============================================================================

-- 0.1 Confirma o total de respondentes validos por ano na camada Silver
-- Esperado: 2023=5293, 2024=5215, 2025=3494 (total 14002)
SELECT ano_pesquisa, COUNT(*) AS total_respondentes
FROM silver_db.perfil_profissional
GROUP BY ano_pesquisa
ORDER BY ano_pesquisa;

-- 0.2 Confirma que a soma da tabela Gold de perfis bate com o total geral
-- Esperado: soma = 14002
SELECT SUM(total_respondentes) AS soma_geral
FROM gold_db.gold_perfis_profissionais;

-- 0.3 Lista todas as tabelas Gold catalogadas (confirma as 10 tabelas)
SHOW TABLES IN gold_db;


-- =============================================================================
-- 1. Como esta estruturado o mercado brasileiro de Dados?
-- (banco: gold_db, tabela: gold_estrutura_mercado)
-- =============================================================================

WITH totais_por_setor AS (
    SELECT setor, SUM(total_respondentes) AS total_setor
    FROM gold_db.gold_estrutura_mercado
    GROUP BY setor
),
total_geral AS (
    SELECT SUM(total_respondentes) AS total FROM gold_db.gold_estrutura_mercado
)
SELECT
    t.setor,
    t.total_setor AS total_respondentes,
    ROUND(t.total_setor * 100.0 / g.total, 1) AS percentual
FROM totais_por_setor t
CROSS JOIN total_geral g
WHERE t.setor NOT IN ('Não informado', 'Outra Opção')
ORDER BY t.total_setor DESC
LIMIT 5;


-- =============================================================================
-- 2. Quais perfis profissionais sao mais valorizados pelo mercado?
-- (banco: gold_db, tabela: gold_perfis_profissionais)
-- =============================================================================

WITH cargo_normalizado AS (
    SELECT
        CASE
            WHEN cargo = 'Engenheiro de Dados/Arquiteto de Dados/Data Engineer/Data Architect'
                THEN 'Engenheiro de Dados/Data Engineer/Data Architect'
            ELSE cargo
        END AS cargo,
        total_respondentes
    FROM gold_db.gold_perfis_profissionais
    WHERE cargo != 'Não aplicável'
),
total_aplicavel AS (
    SELECT SUM(total_respondentes) AS total FROM cargo_normalizado
)
SELECT
    c.cargo,
    SUM(c.total_respondentes) AS total_respondentes,
    ROUND(SUM(c.total_respondentes) * 100.0 / MAX(t.total), 1) AS percentual
FROM cargo_normalizado c
CROSS JOIN total_aplicavel t
WHERE c.cargo NOT IN ('Outra opção', 'Outra Opção')
GROUP BY c.cargo
ORDER BY total_respondentes DESC
LIMIT 5;

-- 2.1 Faixa salarial predominante por senioridade
SELECT
    senioridade,
    faixa_salarial,
    SUM(total_respondentes) AS total_respondentes,
    RANK() OVER (PARTITION BY senioridade ORDER BY SUM(total_respondentes) DESC) AS ranking
FROM gold_db.gold_perfis_profissionais
WHERE senioridade != 'Não aplicável' AND faixa_salarial != 'Não aplicável'
GROUP BY senioridade, faixa_salarial
ORDER BY senioridade, ranking
LIMIT 20;


-- =============================================================================
-- 3. Qual o cenario de diversidade de genero nas carreiras de dados?
-- (banco: gold_db, tabela: gold_diversidade)
-- =============================================================================
SELECT
    ano_pesquisa,
    genero,
    SUM(total_respondentes) AS total_respondentes,
    ROUND(SUM(total_respondentes) * 100.0 / SUM(SUM(total_respondentes)) OVER (PARTITION BY ano_pesquisa), 1) AS percentual_no_ano
FROM gold_db.gold_diversidade
GROUP BY ano_pesquisa, genero
ORDER BY ano_pesquisa, percentual_no_ano DESC;

-- 3.1 Teto de vidro: % de mulheres por nivel de senioridade
SELECT
    senioridade,
    SUM(CASE WHEN genero = 'Feminino' THEN total_respondentes ELSE 0 END) AS mulheres,
    SUM(total_respondentes) AS total,
    ROUND(SUM(CASE WHEN genero = 'Feminino' THEN total_respondentes ELSE 0 END) * 100.0 / SUM(total_respondentes), 1) AS pct_mulheres
FROM gold_db.gold_diversidade
WHERE senioridade != 'Não aplicável'
GROUP BY senioridade
ORDER BY
    CASE senioridade
        WHEN 'Júnior' THEN 1
        WHEN 'Pleno' THEN 2
        WHEN 'Sênior' THEN 3
        WHEN 'Especialista/Staff+' THEN 4
    END;


-- =============================================================================
-- 4. Quais tecnologias apresentam maior adocao entre os profissionais?
-- (banco: gold_db, tabela: gold_adocao_tecnologia)
-- =============================================================================

-- 4.1 Top linguagens (nota: pergunta de multipla escolha, itens combinados
-- como "SQL, Python" nao sao separados aqui - ver notebook para explosao)
SELECT item, SUM(total_respondentes) AS total_mencoes
FROM gold_db.gold_adocao_tecnologia
WHERE categoria = 'linguagem_preferida' AND item NOT IN ('Não informado', 'Nenhuma das opções')
GROUP BY item
ORDER BY total_mencoes DESC
LIMIT 10;

-- 4.2 Evolucao de adocao de cloud, por ano
SELECT
    ano_pesquisa,
    item AS provedor_cloud,
    SUM(total_respondentes) AS total_respondentes
FROM gold_db.gold_adocao_tecnologia
WHERE categoria = 'cloud_usada'
  AND item IN ('Amazon Web Services (AWS)', 'Azure (Microsoft)', 'Google Cloud (GCP)')
GROUP BY ano_pesquisa, item
ORDER BY ano_pesquisa, total_respondentes DESC;

-- 4.3 Ferramentas de BI mais usadas em 2025 

SELECT item, SUM(total_respondentes) AS total_respondentes
FROM gold_db.gold_adocao_tecnologia
WHERE categoria = 'ferramenta_bi_preferida'
  AND ano_pesquisa = 2025
  AND item NOT IN ('Não informado', 'Não tenho preferência / Não sei opinar')
GROUP BY item
ORDER BY total_respondentes DESC
LIMIT 5;


-- =============================================================================
-- 5. Qual o indice de adocao de Inteligencia Artificial e seu impacto?
-- (banco: gold_db, tabelas: gold_adocao_ia_prioridade, gold_adocao_ia_motivos)
-- =============================================================================

SELECT
    ano_pesquisa,
    CASE
        WHEN usa_ia_generativa = 'Não informado' THEN 'Não informado'
        WHEN usa_ia_generativa LIKE 'Não utilizo nenhum tipo%' AND usa_ia_generativa NOT LIKE '%,%' THEN 'Não utilizo'
        ELSE 'Utiliza'
    END AS categoria_uso,
    SUM(total_respondentes) AS total_respondentes
FROM gold_db.gold_adocao_ia_prioridade
GROUP BY
    ano_pesquisa,
    CASE
        WHEN usa_ia_generativa = 'Não informado' THEN 'Não informado'
        WHEN usa_ia_generativa LIKE 'Não utilizo nenhum tipo%' AND usa_ia_generativa NOT LIKE '%,%' THEN 'Não utilizo'
        ELSE 'Utiliza'
    END
ORDER BY ano_pesquisa, categoria_uso;

-- 5.1 Principais motivos para nao adotar IA generativa
SELECT item, SUM(total_mencoes) AS total_mencoes
FROM gold_db.gold_adocao_ia_motivos
GROUP BY item
ORDER BY total_mencoes DESC
LIMIT 5;


-- =============================================================================
-- 6. Existem diferencas relevantes entre regioes, senioridades ou modelos
--    de trabalho?
-- (banco: gold_db, tabela: gold_regiao_senioridade_modelo)
-- =============================================================================
SELECT
    regiao,
    SUM(total_respondentes) AS total_respondentes,
    ROUND(SUM(total_respondentes) * 100.0 / SUM(SUM(total_respondentes)) OVER (), 1) AS percentual
FROM gold_db.gold_regiao_senioridade_modelo
GROUP BY regiao
ORDER BY total_respondentes DESC;

-- 6.1 Distribuicao de senioridade por regiao (mistura de senioridade)
SELECT
    regiao,
    senioridade,
    SUM(total_respondentes) AS total_respondentes
FROM gold_db.gold_regiao_senioridade_modelo
WHERE senioridade != 'Não aplicável'
GROUP BY regiao, senioridade
ORDER BY regiao, senioridade;


-- =============================================================================
-- 7. Quais oportunidades e desafios para empresas que desejam investir em
--    Dados e IA?
-- (banco: gold_db, tabelas: gold_retencao, gold_desafios_gestor)
-- =============================================================================
SELECT
    ano_pesquisa,
    satisfeito_atualmente,
    planeja_mudar_6m,
    SUM(total_respondentes) AS total_respondentes
FROM gold_db.gold_retencao
GROUP BY ano_pesquisa, satisfeito_atualmente, planeja_mudar_6m
ORDER BY ano_pesquisa, total_respondentes DESC;

-- 7.1 % de respondentes insatisfeitos E buscando nova oportunidade

SELECT
    ano_pesquisa,
    SUM(total_respondentes) AS total_respondentes,
    SUM(CASE
        WHEN satisfeito_atualmente = 'Não' AND planeja_mudar_6m LIKE 'Estou em busca%'
        THEN total_respondentes ELSE 0
    END) AS insatisfeitos_buscando,
    ROUND(
        SUM(CASE
            WHEN satisfeito_atualmente = 'Não' AND planeja_mudar_6m LIKE 'Estou em busca%'
            THEN total_respondentes ELSE 0
        END) * 100.0 / SUM(total_respondentes), 1
    ) AS pct_insatisfeitos_buscando
FROM gold_db.gold_retencao
GROUP BY ano_pesquisa
ORDER BY ano_pesquisa;

-- 7.2 Principais desafios relatados por gestores de dados
SELECT item, SUM(total_mencoes) AS total_mencoes
FROM gold_db.gold_desafios_gestor
GROUP BY item
ORDER BY total_mencoes DESC
LIMIT 5;
