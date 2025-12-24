# 🗂️ Estrutura do Projeto – Pipeline Receita Federal

[Descrição]

---

## Estrutura Geral do Projeto

```
cnpj-data-pipeline/
├── docker-compose.yml
├── .env
├── README.md
│
├── airflow/
│   ├── Dockerfile
│   ├── dags/
│   │   ├── cnpj_pipeline.py
│   │   └── __init__.py
│   └── requirements.txt
│
├── downloader/
│   ├── Dockerfile
│   ├── downloader.py
│   ├── requirements.txt
│   └── README.md
│
├── dbt/
│   ├── Dockerfile
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── bronze/
│       │   └── sources.yml
│       ├── silver/
│       │   ├── empresas.sql
│       │   ├── estabelecimentos.sql
│       │   └── socios.sql
│       ├── gold/
│       │   ├── fato_empresas.sql
│       │   ├── dim_cnae.sql
│       │   ├── dim_municipio.sql
│       │   └── dim_tempo.sql
│       └── schema.yml
│
├── warehouse/
│   └── duckdb/
│       └── cnpj.duckdb
│
├── data/
│   ├── bronze/
│   │   ├── empresas/
│   │   │   └── ano=2024/mes=01/
│   │   ├── estabelecimentos/
│   │   └── socios/
│   │
│   ├── silver/
│   │   ├── empresas/
│   │   ├── estabelecimentos/
│   │   └── socios/
│   │
│   └── gold/
│       └── marts/
│
├── metabase/     
│   └── Dockerfile
│
└── scripts/
    └── init.sql
```