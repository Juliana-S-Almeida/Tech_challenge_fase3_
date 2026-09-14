# 📊 Mercado Brasileiro de Dados, Analytics e Inteligência Artificial

**Pipeline de Data Lake serverless na AWS + Análise Executiva do mercado de Dados no Brasil (2023–2025)**

> Tech Challenge Fase 3 — Pós-graduação em Data Analytics, FIAP POSTECH

[![AWS](https://img.shields.io/badge/AWS-Glue%20%7C%20S3%20%7C%20Athena-orange?logo=amazonaws)](.)
[![PySpark](https://img.shields.io/badge/PySpark-processing-red?logo=apachespark)](.)
[![Python](https://img.shields.io/badge/Python-pandas%20%7C%20matplotlib-blue?logo=python)](.)
[![SQL](https://img.shields.io/badge/SQL-Amazon%20Athena-lightgrey?logo=amazondynamodb)](.)

---

## 🎯 O problema de negócio

Uma instituição financeira de grande porte quer **expandir sua área de Dados, Analytics e IA**, mas precisa entender o mercado brasileiro antes de decidir como contratar, capacitar e investir em tecnologia.

Este projeto simula o trabalho de uma consultoria de dados: transforma três edições brutas da pesquisa **State of Data Brasil** (Data Hackers + Bain, 2023–2025) em um pipeline de dados na nuvem e em um relatório executivo com recomendações estratégicas.

## 🏗️ Arquitetura da solução

Data Lake serverless em camadas (**Bronze → Silver → Gold**), 100% AWS:

![Diagrama de arquitetura](./Desenho%20de%20arquitetura/Arquitetura_Tech_Challenge_Fase3.jpg)

| Camada | O que acontece | Serviço |
|---|---|---|
| **Bronze** | Ingestão dos 3 CSVs brutos do Kaggle, sem tratamento | Amazon S3 |
| **Silver** | Padronização de colunas, tratamento de nulos (estrutural vs. simples), harmonização de categorias entre as 3 edições | AWS Glue (PySpark) |
| **Gold** | 10 tabelas agregadas, uma por pergunta de negócio, prontas para consumo analítico | AWS Glue Notebook + Parquet particionado |
| **Consumo** | Consultas de validação e geração de gráficos executivos | Amazon Athena + Glue Notebook (pandas/matplotlib) |

Catalogação centralizada via **AWS Glue Data Catalog** (`bronze_db`, `silver_db`, `gold_db`), com Crawlers automatizando a descoberta de esquema.

## 📈 Principais achados

- **Mercado concentrado**: Finanças/Bancos e Tecnologia respondem por ~35% dos profissionais de dados; 41% trabalham em empresas com mais de 3.000 funcionários.
- **Teto de vidro real**: representatividade feminina cai de 28% (Júnior) para 20% (Especialista/Staff+); gap salarial de até 11 p.p. no nível Sênior.
- **Stack dominante**: Python (72%) e AWS (líder desde 2024) são o padrão de mercado; Power BI concentra 66% da preferência em ferramentas de BI.
- **IA já é mainstream no indivíduo, não na empresa**: uso pessoal de IA generativa é quase universal (98,9% em 2025), mas só 11% das empresas tratam IA como prioridade estratégica — um descompasso estratégico real.
- **Risco de retenção**: entre 19% e 20,5% dos profissionais estão simultaneamente insatisfeitos e buscando nova oportunidade.

📄 Relatório executivo completo com todas as análises: [`Relatório/`](./Relat%C3%B3rio)

## 🗂️ Estrutura do repositório

```
├── Arquivos brutos Data Hackers - Kaggle/   # CSVs originais das 3 edições da pesquisa
├── Relatório/                                # Relatório executivo (.pdf) com storytelling e recomendações
├── Desenho de arquitetura/                   # Diagrama da solução AWS (Draw.io)
└── Notebooks/
    ├── Tech_Challenge_Fase3_01_ETL_Bronze_Silver.py       # Glue Job PySpark: ingestão e tratamento
    ├── Tech_Challenge_Fase3_02_Modelagem_Silver_Gold.ipynb # Glue Notebook: construção das 10 tabelas Gold
    ├── Tech_Challenge_Fase3_03_Consultas_e_Graficos.ipynb  # Consultas analíticas + geração dos 11 gráficos executivos
    └── Tech_Challenge_Fase3_04_Consultas_Athena.sql        # Queries SQL de validação cruzada via Amazon Athena
```

## 🛠️ Competências técnicas demonstradas

- **Engenharia de Dados**: arquitetura de Data Lake em camadas, idempotência, particionamento, tratamento semântico de nulos (estrutural vs. ausência de resposta)
- **PySpark**: transformação distribuída, otimização de `stack()` para reduzir estágios de execução (~200 → 15), harmonização de schema entre fontes heterogêneas
- **AWS**: S3, Glue (Jobs, Crawlers, Data Catalog, Interactive Sessions), Athena — com gestão de custo em ambiente de laboratório com orçamento limitado
- **SQL analítico**: window functions, agregações condicionais, CTEs
- **Storytelling de dados**: tradução de indicadores técnicos em recomendações estratégicas para tomada de decisão executiva
- **Rigor de dados**: auditoria de rastreabilidade ponta a ponta — todo número do relatório executivo é reproduzível a partir do código-fonte



---

*Projeto acadêmico — FIAP POSTECH, Pós-graduação em Data Analytics. Fonte de dados: [State of Data Brasil](https://www.kaggle.com/datahackers/datasets), Data Hackers + Bain.*
