"""
Tech Challenge Fase 3 - State of Data Brasil
Glue Job: ETL Bronze -> Silver

Le os 3 CSVs originais (2023, 2024, 2025) diretamente do S3 (bronze/),
aplica padronizacao de nomes de coluna, tratamento de nulo e harmonizacao
de categorias, e grava duas tabelas na camada Silver, em Parquet
particionado por ano_pesquisa:

  1) silver/perfil_profissional/  -> uma linha por respondente (tabela "wide")
  2) silver/respostas_multipla_escolha/ -> uma linha por marcacao em pergunta
     de multipla escolha (cargos do time, cloud, banco de dados, motivos de
     nao usar IA, desafios do gestor) - tabela "long"

DECISAO ARQUITETURAL: a leitura e feita diretamente dos arquivos no S3 via
Spark (nao pela tabela do Glue Data Catalog da Bronze), porque o
classificador de CSV do crawler nao reconhece corretamente os cabecalhos
originais (em especial o formato de tupla Python do arquivo de 2023). O
parser de CSV do Spark e mais tolerante e resolve isso sem problemas.
Ver decisao documentada na Etapa 2 do projeto.

Toda a logica de mapeamento e tratamento foi validada previamente em um
prototipo pandas antes de ser traduzida para PySpark, evitando retrabalho
e consumo desnecessario de DPU-hora no ambiente AWS Academy Lab.
"""

import sys
import re

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

# ---------------------------------------------------------------------------
# Setup padrao do Glue Job
# ---------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

BUCKET = "techchallenge-fase3-fiap-761328510173"
BRONZE_PATHS = {
    2023: f"s3://{BUCKET}/bronze/2023/",
    2024: f"s3://{BUCKET}/bronze/2024/",
    2025: f"s3://{BUCKET}/bronze/2025/",
}
SILVER_WIDE_PATH = f"s3://{BUCKET}/silver/perfil_profissional/"
SILVER_LONG_PATH = f"s3://{BUCKET}/silver/respostas_multipla_escolha/"


# ---------------------------------------------------------------------------
# Leitura dos CSVs originais direto do S3
# ---------------------------------------------------------------------------
def read_csv(path):
    return (
        spark.read.option("header", "true")
        .option("multiLine", "true")
        .option("escape", '"')
        .option("quote", '"')
        .csv(path)
    )


df23_raw = read_csv(BRONZE_PATHS[2023])
df24_raw = read_csv(BRONZE_PATHS[2024])
df25_raw = read_csv(BRONZE_PATHS[2025])


# ---------------------------------------------------------------------------
# 2023: cabecalho vem como texto de tupla Python, ex: "('P1_a ', 'Idade')".
# Extraimos o codigo (primeiro elemento) para nomear as colunas, e guardamos
# o rotulo (segundo elemento) num dicionario para uso nos blocos de
# multipla escolha mais adiante.
# ---------------------------------------------------------------------------
TUPLE_PATTERN = re.compile(r"^\('([^']*)',\s*'(.*)'\)$")


def parse_2023_code(raw_name):
    m = TUPLE_PATTERN.match(raw_name)
    return m.group(1).strip() if m else raw_name


def parse_2023_label(raw_name):
    m = TUPLE_PATTERN.match(raw_name)
    return m.group(2).strip() if m else raw_name


LABELS_2023 = {parse_2023_code(c): parse_2023_label(c) for c in df23_raw.columns}

df23 = df23_raw.toDF(*[parse_2023_code(c) for c in df23_raw.columns])
df24 = df24_raw
df25 = df25_raw

# Remover duplicatas exatas usando a linha COMPLETA original, ANTES de
# reduzir para as colunas de negocio (evita falsos positivos de duplicata
# por coincidencia em poucas colunas - erro identificado na validacao).
df23 = df23.dropDuplicates()
df24 = df24.dropDuplicates()
df25 = df25.dropDuplicates()


# ---------------------------------------------------------------------------
# Dicionario de mapeamento: nome padronizado -> codigo de coluna por ano
# ---------------------------------------------------------------------------
MAPPING = {
    "id_original": {2023: "P0", 2024: "0.a_token", 2025: "0.a_token"},
    "idade": {2023: "P1_a", 2024: "1.a_idade", 2025: "1.a_idade"},
    "genero": {2023: "P1_b", 2024: "1.b_genero", 2025: "1.b_genero"},
    "raca_etnia": {2023: "P1_c", 2024: "1.c_cor/raca/etnia", 2025: "1.c_cor/raca/etnia"},
    "pcd": {2023: "P1_d", 2024: "1.d_pcd", 2025: "1.d_pcd"},
    "regiao": {2023: "P1_i_2", 2024: "1.i.2_regiao_onde_mora", 2025: "1.i.2_regiao_onde_mora"},
    "situacao_trabalho": {2023: "P2_a", 2024: "2.a_situação_de_trabalho", 2025: "2.a_situação_de_trabalho"},
    "setor": {2023: "P2_b", 2024: "2.b_setor", 2025: "2.b_setor"},
    "num_funcionarios": {2023: "P2_c", 2024: "2.c_numero_de_funcionarios", 2025: "2.c_numero_de_funcionarios"},
    "cargo": {2023: "P2_f", 2024: "2.f_cargo_atual", 2025: "2.f_cargo_atual"},
    "senioridade": {2023: "P2_g", 2024: "2.g_nivel", 2025: "2.g_nivel"},
    "faixa_salarial": {2023: "P2_h", 2024: "2.h_faixa_salarial", 2025: "2.h_faixa_salarial"},
    "satisfeito_atualmente": {2023: "P2_k", 2024: "2.k_satisfeito_atualmente", 2025: "2.k_satisfeito_atualmente"},
    "participou_entrevistas_6m": {
        2023: "P2_m",
        2024: "2.m_participou_de_entrevistas_ultimos_6m",
        2025: "2.m_participou_de_entrevistas_ultimos_6m",
    },
    "planeja_mudar_6m": {
        2023: "P2_n",
        2024: "2.n_planos_de_mudar_de_emprego_6m",
        2025: "2.n_planos_de_mudar_de_emprego_6m",
    },
    "modelo_trabalho": {2023: "P2_r", 2024: "2.r_modelo_de_trabalho_atual", 2025: "2.q_modelo_de_trabalho_atual"},
    "num_pessoas_dados": {2023: "P3_a", 2024: "3.a_numero_de_pessoas_em_dados", 2025: "3.a_numero_de_pessoas_em_dados"},
    "ia_prioridade": {
        2023: "P3_e",
        2024: "3.e_ai_generativa_e_llm_é_uma_prioridade?",
        2025: "3.e_ai_generativa_e_llm_é_uma_prioridade?",
    },
    "linguagem_preferida": {2023: "P4_f", 2024: "4.f_linguagem_preferida", 2025: "4.c_linguagem_preferida"},
    "bi_preferida": {2023: "P4_k", 2024: "4.k_ferramenta_de_bi_preferida", 2025: "4.h_ferramenta_de_bi_preferida"},
    "usa_ia_generativa": {
        2023: "P4_m",
        2024: "4.m_usa_chatgpt_ou_copilot_no_trabalho?",
        2025: "4.j_usa_chatgpt_ou_copilot_no_trabalho?",
    },
}

# Nulo estrutural: correlacionado com nao estar empregado -> "Nao aplicavel"
NULO_ESTRUTURAL = {"cargo", "senioridade", "faixa_salarial"}
# Nulo simples: ausencia de resposta comum -> "Nao informado"
NULO_SIMPLES = {
    "regiao", "setor", "num_funcionarios", "num_pessoas_dados", "modelo_trabalho",
    "linguagem_preferida", "bi_preferida", "ia_prioridade", "usa_ia_generativa",
    "participou_entrevistas_6m", "planeja_mudar_6m", "raca_etnia", "pcd",
}


def build_wide(df, ano):
    select_exprs = []
    for std_name, code_map in MAPPING.items():
        code = code_map[ano]
        if code in df.columns:
            select_exprs.append(F.col(f"`{code}`").alias(std_name))
        else:
            select_exprs.append(F.lit(None).alias(std_name))
    out = df.select(*select_exprs)
    out = out.withColumn("ano_pesquisa", F.lit(ano))
    out = out.withColumn(
        "id_respondente",
        F.concat(F.col("ano_pesquisa").cast("string"), F.lit("_"), F.col("id_original").cast("string")),
    )
    return out.drop("id_original")


w23 = build_wide(df23, 2023)
w24 = build_wide(df24, 2024)
w25 = build_wide(df25, 2025)

silver_wide = w23.unionByName(w24).unionByName(w25)

# Padronizar satisfeito_atualmente: bool (2024) vs float 0/1 (2023, 2025)
silver_wide = silver_wide.withColumn(
    "satisfeito_atualmente",
    F.when(F.col("satisfeito_atualmente").isNull(), "Não informado")
    .when(F.lower(F.col("satisfeito_atualmente").cast("string")).isin("true", "1", "1.0"), "Sim")
    .when(F.lower(F.col("satisfeito_atualmente").cast("string")).isin("false", "0", "0.0"), "Não")
    .otherwise("Não informado"),
)

# Tratamento de nulo: estrutural vs. simples
for col_name in NULO_ESTRUTURAL:
    silver_wide = silver_wide.withColumn(
        col_name, F.when(F.col(col_name).isNull(), "Não aplicável").otherwise(F.col(col_name))
    )
for col_name in NULO_SIMPLES:
    silver_wide = silver_wide.withColumn(
        col_name, F.when(F.col(col_name).isNull(), "Não informado").otherwise(F.col(col_name))
    )

silver_wide = silver_wide.withColumn("idade", F.col("idade").cast(IntegerType()))

(
    silver_wide.write.mode("overwrite")
    .partitionBy("ano_pesquisa")
    .parquet(SILVER_WIDE_PATH)
)


# ---------------------------------------------------------------------------
# Blocos de multipla escolha -> formato longo (uma linha por marcacao)
# ---------------------------------------------------------------------------
DESAFIO_GESTOR_2023_PARA_CANONICO = {
    "Contratar novos talentos": "Contratar talentos",
    "Convencer a empresa a aumentar os investimentos na área de dados": "Convencer a empresa a aumentar investimentos",
    "Gestão de projetos envolvendo áreas multidisciplinares da empresa": "Gestão de projetos envolvendo áreas multidisciplinares",
    "Organizar as informações e garantir a qualidade e confiabilidade": "Organizar as informações com qualidade e confiabilidade",
    "Conseguir processar e armazenar um alto volume de dados": "Processar e armazenar um alto volume de dados",
    "Conseguir gerar valor para as áreas de negócios através de estudos e experimentos": "Gerar valor para as áreas de negócios",
    "Gerenciar a expectativa das áreas de negócio em relação as entregas das equipes de dados": "Gerenciar a expectativa das áreas",
    "Garantir a manutenção dos projetos e modelos em produção, em meio ao crescimento da empresa": "Garantir a manutenção dos projetos e modelos em produção",
    "Conseguir levar inovação para a empresa através dos dados": "Conseguir levar inovação para a empresa",
    "Garantir retorno do investimento (ROI) em projetos de dados": "Garantir (ROI) em projetos de dados",
}

BLOCKS = {
    "cargos_time_dados": ("P3_b_", "3.b.", "3.b."),
    "cloud_usada": ("P4_h_", "4.h.", "4.e."),
    "banco_dados_usado": ("P4_g_", "4.g.", "4.d."),
    "motivo_nao_ia": ("P3_g_", "3.g.", "3.h."),
    "desafio_gestor": ("P3_d_", "3.d.", "3.d."),
}


def normalize_item(text):
    t = text.strip()
    t = re.sub(r"^[a-zA-Z0-9]{1,2}\s+", "", t)
    return t.rstrip(".").strip()


def get_block_cols(df, prefix, ano, harmonize_map=None):
    cols = [c for c in df.columns if c.startswith(prefix)]
    result = {}
    for c in cols:
        if ano == 2023:
            item = LABELS_2023.get(c, c)
        else:
            after = c[len(prefix):]
            item = after.split("_", 1)[1] if "_" in after else after
        item = normalize_item(item)
        if harmonize_map and item in harmonize_map:
            item = harmonize_map[item]
        result[c] = item
    return result


def melt_block(df, prefix, ano, bloco_nome, harmonize_map=None):
    """Achata todas as colunas binarias do bloco numa unica operacao via
    stack() do Spark SQL, em vez de uma uniao por coluna. Um teste local
    mostrou que a versao "uma uniao por coluna" gera um plano de execucao
    com ~200 estagios (33 colunas x 3 anos so no bloco de bancos de dados),
    o que estourou memoria do driver localmente e, no Glue real, geraria
    tarefas paralelas demais - mais tempo de execucao e mais DPU-hora
    consumida sem necessidade. Com stack(), cada (bloco, ano) vira uma
    unica operacao, reduzindo o total de uniões de ~200 para 15."""
    colmap = get_block_cols(df, prefix, ano, harmonize_map)
    n = len(colmap)
    if n == 0:
        return None
    stack_args = []
    for col_code, item_label in colmap.items():
        safe_label = item_label.replace("'", "\\'")
        stack_args.append(f"'{safe_label}'")
        stack_args.append(f"CAST(`{col_code}` AS INT)")
    stack_expr = f"stack({n}, {', '.join(stack_args)}) as (item, marcado)"
    result = (
        df.select(F.col("id_respondente"), F.expr(stack_expr))
        .filter(F.col("marcado") == 1)
        .select(
            F.col("id_respondente"),
            F.lit(ano).alias("ano_pesquisa"),
            F.lit(bloco_nome).alias("bloco"),
            F.col("item"),
        )
    )
    return result


# adiciona id_respondente aos dataframes originais (necessario para o melt)
df23 = df23.withColumn("id_respondente", F.concat(F.lit("2023_"), F.col("P0").cast("string")))
df24 = df24.withColumn("id_respondente", F.concat(F.lit("2024_"), F.col("`0.a_token`").cast("string")))
df25 = df25.withColumn("id_respondente", F.concat(F.lit("2025_"), F.col("`0.a_token`").cast("string")))

long_pieces = []
for bloco, (p23, p24, p25) in BLOCKS.items():
    hmap = DESAFIO_GESTOR_2023_PARA_CANONICO if bloco == "desafio_gestor" else None
    for df, prefix, ano in [(df23, p23, 2023), (df24, p24, 2024), (df25, p25, 2025)]:
        piece = melt_block(df, prefix, ano, bloco, hmap)
        if piece is not None:
            long_pieces.append(piece)

silver_long = long_pieces[0]
for p in long_pieces[1:]:
    silver_long = silver_long.unionByName(p)

(
    silver_long.write.mode("overwrite")
    .partitionBy("ano_pesquisa", "bloco")
    .parquet(SILVER_LONG_PATH)
)

job.commit()
