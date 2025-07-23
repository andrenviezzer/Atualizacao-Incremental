# Atualizacao-Incremental
Projeto de ingestão e transformação de dados usando PySpark e Delta Lake em três camadas: Bronze, Silver e Gold. O pipeline simula o fluxo completo de dados desde a ingestão de arquivos CSV até a estruturação e atualização incremental para consumo analítico.


#  Atualização Incremental com Databricks

Este repositório contém notebooks e scripts que implementam uma estratégia de **atualização incremental de dados** utilizando o **Databricks** e o formato **Delta Lake**. A arquitetura segue o padrão de camadas **Bronze, Silver e Gold**, garantindo controle, rastreabilidade e performance na transformação de dados.

---

##  Arquitetura

A pipeline é organizada nas seguintes camadas:

###  Bronze
- Ingestão bruta de dados a partir da origem (ex: arquivos, banco Oracle, APIs).
- Nenhuma transformação é aplicada.
- Utiliza o Autoloader para ingestão contínua e eficiente.

###  Silver
- Aplicação de regras de limpeza, deduplicação e padronização de dados.
- Adição de colunas técnicas (hashes, timestamps).
- Geração de tabelas intermediárias confiáveis.

###  Gold
- Aplicação de regras de negócio e métricas.
- Tabelas prontas para consumo analítico.
- Pode conter **views** ou tabelas otimizadas para dashboards e relatórios.

---

##  Estrutura do Projeto

Atualizacao-Incremental/
│
├── notebooks/
│ ├── bronze/
│ ├── silver/
│ └── gold/
│
├── utils/
│ └── api_connection.py
│
├── config/
│ └── parametros.yml
│
└── README.md


---

##  Tecnologias Utilizadas

- **Databricks**
- **PySpark**
- **Delta Lake**
- **Git & GitHub**
- **AWS S3 (para armazenamento de arquivos)**
- **Oracle Database (como fonte de dados)**

---

##  Executando Localmente

1. Clone o repositório:
   ```bash
   git clone https://github.com/andrenviezzer/Atualizacao-Incremental.git



