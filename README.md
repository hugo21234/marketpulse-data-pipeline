# MarketPulse Data Pipeline

Pipeline de engenharia de dados para coleta, processamento e análise de dados do mercado financeiro utilizando **Python**, **Apache Airflow**, **Alpha Vantage** e uma arquitetura analítica nativa da AWS.

O projeto foi desenhado como um laboratório prático de engenharia de dados, com foco em conceitos reais: orquestração, ingestão batch, idempotência, controle de cota, data lake, formatos colunares, catálogo de dados, modelagem dimensional, qualidade, observabilidade e operação de um warehouse serverless.

> **Escopo honesto:** o plano gratuito do Alpha Vantage permite até 25 requisições por dia e não fornece cotações de ações em tempo real gratuitamente. Por isso, o MVP será uma pipeline **batch diária**. Uma trilha de streaming poderá ser adicionada futuramente com outra fonte pública.

> **Decisão pedagógica:** o volume inicial poderia ser atendido por PostgreSQL ou Athena. O Amazon Redshift Serverless será adotado deliberadamente para estudar uma arquitetura analítica AWS completa, incluindo integração com S3, IAM, Glue, modelagem dimensional, cargas incrementais e operação de warehouse.

## Estado atual da implementação

O pipeline implementado atualmente executa diariamente o seguinte fluxo:

```text
Alpha Vantage -> S3 Bronze (JSON) -> S3 Silver (Parquet)
              -> S3 Gold (indicadores) -> AWS Glue Crawler
```

- o Airflow processa `AAPL`, `MSFT`, `GOOGL`, `AMZN` e `NVDA`;
- as chamadas Bronze são serializadas para respeitar o limite da API;
- a Silver tipa e valida datas, preços, volume, nulos e duplicidades;
- a Gold calcula retorno, variação, média móvel e volatilidade;
- o Glue Crawler atualiza o catálogo depois que todos os arquivos Gold existem;
- EventBridge Scheduler e Lambda ligam e desligam a EC2 que hospeda o Airflow.

Athena, Redshift Serverless, alertas e cargas incrementais descritos abaixo fazem parte do roadmap e ainda não estão implementados neste repositório.

## Objetivo

Construir uma plataforma capaz de responder:

> Quais ativos apresentaram movimentos relevantes de preço, volume e volatilidade no último pregão?

A primeira versão monitorará uma watchlist pequena de ativos, preservará as respostas brutas da API, transformará os dados em Parquet, disponibilizará consultas no Athena e carregará um modelo analítico no Redshift Serverless.

## Arquitetura

```text
Alpha Vantage
      |
      v
Apache Airflow em Amazon EC2
      |
      +--> Amazon S3 Bronze
      |      JSON bruto e imutável
      |
      +--> Transformações Python
      |
      +--> Amazon S3 Silver
      |      Parquet limpo, tipado e particionado
      |
      +--> AWS Glue Data Catalog
      |      Catálogo das tabelas externas
      |
      +--> Amazon Athena
      |      Exploração e validação direta no data lake
      |
      +--> Amazon Redshift Serverless
             staging -> analytics -> gold
```

Serviços transversais:

```text
IAM                  controle de acesso entre serviços
CloudWatch           logs, métricas e alertas
AWS Budgets          monitoramento de custos
Parameter Store      segredos e configurações sensíveis
```

## Responsabilidade de cada componente

| Componente | Responsabilidade |
|---|---|
| Alpha Vantage | Fonte de preços diários e fundamentos |
| Apache Airflow | Agendamento, dependências, retries, backfills e monitoramento |
| Amazon EC2 | Hospedagem do Airflow |
| Python | Extração, validação, transformação e carga |
| Amazon S3 Bronze | Preservação da resposta original da API |
| Amazon S3 Silver | Armazenamento colunar em Parquet |
| AWS Glue Data Catalog | Definição e descoberta das tabelas do data lake |
| Amazon Athena | Consultas SQL diretamente sobre os arquivos no S3 |
| Amazon Redshift Serverless | Warehouse OLAP para staging, modelo dimensional e camada Gold |
| IAM | Permissões entre EC2, S3, Glue, Athena e Redshift |
| CloudWatch | Logs, métricas e observabilidade operacional |
| AWS Budgets | Alertas e proteção contra custos inesperados |

## Por que OLAP neste projeto?

O MarketPulse é predominantemente analítico. As consultas esperadas envolvem séries históricas, janelas temporais, agregações, médias móveis, volatilidade e comparação entre ativos.

Exemplos:

```text
Qual foi o retorno acumulado por ativo nos últimos 30 dias?
Qual ativo apresentou maior volatilidade no período?
Como o volume negociado mudou em relação à média recente?
Quais movimentos fugiram do comportamento histórico?
```

Esse padrão de acesso é diferente de um sistema OLTP, que prioriza operações transacionais pequenas, como cadastrar usuários, atualizar pedidos ou consultar o estado atual de uma entidade.

## Camadas de dados

### Bronze — Amazon S3

Preserva a resposta original da API antes de qualquer transformação.

```text
s3://marketpulse-data-lake/bronze/daily_prices/
└── extraction_date=YYYY-MM-DD/
    ├── AAPL.json
    ├── MSFT.json
    └── NVDA.json
```

Objetivos:

- permitir auditoria;
- reprocessar sem consumir novamente a API;
- investigar mudanças de schema;
- preservar evidência da extração;
- desacoplar a fonte do restante da arquitetura.

### Silver — Amazon S3 + Parquet

Dados limpos, tipados, deduplicados e convertidos para formato colunar.

```text
s3://marketpulse-data-lake/silver/daily_prices/
└── symbol=AAPL/
    └── year=2026/
        └── month=07/
            └── data.parquet
```

Tabela lógica principal: `silver_daily_price`.

| Campo | Descrição |
|---|---|
| asset_id | Identificador do ativo |
| symbol | Código do ativo |
| reference_date | Data do pregão |
| open_price | Preço de abertura |
| high_price | Maior preço |
| low_price | Menor preço |
| close_price | Preço de fechamento |
| adjusted_close | Fechamento ajustado |
| volume | Volume negociado |
| source | Fonte do dado |
| extracted_at | Momento da extração |

Chave lógica:

```text
symbol + reference_date
```

Essa é a chave lógica pretendida. A carga atual ainda grava snapshots com timestamp; portanto, deduplicação entre execuções e idempotência completa permanecem como melhorias do roadmap.

### Catálogo — AWS Glue Data Catalog

O Glue Catalog registrará o schema e as partições dos arquivos Silver.

Responsabilidades:

- manter o catálogo das tabelas externas;
- disponibilizar metadados para Athena;
- facilitar descoberta e evolução de schema;
- centralizar a definição lógica dos dados no S3.

### Exploração — Amazon Athena

O Athena será utilizado para:

- validar arquivos Parquet;
- conferir partições;
- executar consultas exploratórias;
- investigar falhas de carga;
- comparar dados do lake com o warehouse.

Athena não será a principal camada de serving do projeto. Sua função será exploração, inspeção e validação do data lake.

### Warehouse — Amazon Redshift Serverless

O Redshift será organizado em três schemas:

```text
staging
analytics
gold
```

#### `staging`

Recebe dados carregados do S3 antes de merges e validações finais.

```text
staging.daily_price
staging.company_fundamentals
```

#### `analytics`

Contém o modelo dimensional principal.

```text
analytics.dim_asset
analytics.fact_daily_price
analytics.snapshot_company_fundamentals
```

#### `gold`

Contém métricas prontas para consumo analítico.

```text
gold.asset_daily_signal
```

## Modelo de dados

### `analytics.dim_asset`

```text
asset_id
symbol
company_name
exchange
sector
industry
currency
is_active
created_at
updated_at
```

### `analytics.fact_daily_price`

```text
asset_id
reference_date
open_price
high_price
low_price
close_price
adjusted_close
volume
source
extracted_at
```

### `analytics.snapshot_company_fundamentals`

```text
asset_id
reference_date
market_cap
pe_ratio
eps
profit_margin
revenue_ttm
dividend_yield
analyst_target_price
extracted_at
```

### `gold.asset_daily_signal`

```text
asset_id
reference_date
daily_return
volume_change
moving_average_7d
moving_average_30d
volatility_30d
price_anomaly_score
signal_classification
calculated_at
```

A classificação não representa recomendação de investimento. Ela apenas identifica movimentos estatisticamente relevantes para fins educacionais.

## Limites do Alpha Vantage

O plano gratuito permite até **25 requisições por dia**. O projeto adotará um orçamento operacional inferior ao limite para manter margem para testes e retries.

Exemplo com cinco ativos:

```text
5 requisições diárias para preços
5 requisições semanais para fundamentos
Margem restante para testes e reprocessamentos
```

Toda chamada deverá ser registrada para evitar consumo acidental da cota.

## Watchlist inicial

```text
AAPL
MSFT
NVDA
AMZN
GOOGL
```

A watchlist será configurável, mas permanecerá pequena durante o MVP.

## DAGs planejadas

### `daily_market_prices`

Executada após o encerramento do pregão.

```text
check_api_quota
      ↓
extract_daily_prices
      ↓
validate_api_response
      ↓
save_raw_to_s3_bronze
      ↓
normalize_prices
      ↓
write_parquet_to_s3_silver
      ↓
update_glue_partitions
      ↓
copy_to_redshift_staging
      ↓
merge_fact_and_dimensions
      ↓
run_quality_checks
```

### `weekly_company_fundamentals`

Executada semanalmente.

```text
check_api_quota
      ↓
extract_company_overview
      ↓
save_raw_to_s3_bronze
      ↓
normalize_fundamentals
      ↓
write_parquet_to_s3_silver
      ↓
copy_to_redshift_staging
      ↓
merge_fundamentals_snapshot
```

### `daily_asset_signals`

Não consome a API. Processa somente dados já persistidos.

```text
read_daily_prices
      ↓
calculate_returns
      ↓
calculate_moving_averages
      ↓
calculate_volatility
      ↓
classify_daily_signal
      ↓
upsert_gold_table
```

### `pipeline_data_quality`

Validações iniciais:

- `high_price >= low_price`;
- preços maiores que zero;
- volume não negativo;
- ausência de duplicatas na chave lógica;
- presença dos ativos esperados;
- resposta da API sem mensagem de limite excedido;
- quantidade de requisições dentro do orçamento diário;
- data do pregão não futura;
- arquivos Parquet disponíveis na partição esperada;
- quantidade de linhas reconciliada entre S3 e Redshift.

## Estrutura planejada do repositório

```text
marketpulse-data-pipeline/
├── airflow/
│   ├── dags/
│   │   ├── daily_market_prices.py
│   │   ├── weekly_company_fundamentals.py
│   │   ├── daily_asset_signals.py
│   │   └── pipeline_data_quality.py
│   ├── logs/
│   └── plugins/
├── src/
│   ├── extraction/
│   ├── transformation/
│   ├── loading/
│   ├── quality/
│   ├── storage/
│   └── domain/
├── infra/
│   ├── iam/
│   ├── redshift/
│   ├── s3/
│   └── monitoring/
├── sql/
│   ├── ddl/
│   ├── staging/
│   ├── analytics/
│   └── gold/
├── tests/
│   ├── unit/
│   ├── integration/
│   └── fixtures/
├── data/
│   └── samples/
├── docker-compose.yml
├── .env.example
├── .gitignore
├── pyproject.toml
└── README.md
```

A lógica de negócio permanecerá em `src/`. Os arquivos das DAGs serão responsáveis apenas pela orquestração.

## Princípios de engenharia

### Idempotência

Executar novamente uma DAG para a mesma data não poderá duplicar registros. As cargas utilizarão chaves lógicas, staging tables e operações de merge.

### Backfill

As DAGs deverão aceitar uma data de referência para permitir reprocessar períodos anteriores sem alterar o código.

### Controle de cota

O pipeline verificará o histórico de chamadas antes de cada requisição. Uma task não deverá consumir a API quando o orçamento diário estiver próximo do limite.

### Observabilidade

Cada execução deverá registrar:

```text
requisições realizadas
ativos processados
registros extraídos
arquivos gravados no S3
registros carregados no Redshift
registros rejeitados
duração da task
status da carga
data máxima disponível
```

Os logs do Airflow e das integrações AWS serão enviados ao CloudWatch quando a infraestrutura base estiver estável.

### Data quality

Respostas HTTP bem-sucedidas não serão consideradas automaticamente válidas. A API pode devolver mensagens de limite ou erro dentro de uma resposta tecnicamente aceita.

### Separação entre storage e serving

O S3 será a fonte histórica durável. O Redshift será a camada de serving analítico.

Essa separação permite:

- reprocessar dados sem consultar novamente a API;
- reconstruir tabelas analíticas;
- trocar o mecanismo de consulta futuramente;
- reduzir acoplamento entre ingestão e consumo.

## Segurança

- não versionar a chave do Alpha Vantage;
- não versionar credenciais AWS;
- utilizar IAM Roles em vez de chaves estáticas quando possível;
- armazenar segredos no Parameter Store ou Secrets Manager;
- manter `.env` fora do Git;
- disponibilizar somente `.env.example`;
- evitar exposição pública direta da interface do Airflow;
- acessar o Airflow por túnel SSH ou proxy autenticado;
- aplicar princípio do menor privilégio nas policies IAM;
- bloquear acesso público aos buckets S3;
- evitar deixar o Redshift publicamente acessível.

## Controle de custos

A arquitetura foi desenhada para aprendizado, mas nem todos os serviços são permanentemente gratuitos.

Medidas obrigatórias:

- criar um AWS Budget antes do provisionamento;
- configurar alertas de cobrança;
- acompanhar consumo de Redshift Processing Units;
- limitar consultas e cargas desnecessárias;
- particionar arquivos no S3 para reduzir leitura no Athena;
- encerrar recursos de laboratório quando não estiverem em uso;
- revisar diariamente o Billing Dashboard durante a fase inicial.

| Serviço | Uso planejado | Observação |
|---|---|---|
| Alpha Vantage | Até 25 requisições diárias | Plano gratuito limitado |
| Amazon EC2 | Airflow mínimo | Depende da elegibilidade da conta e do tipo de instância |
| Amazon S3 | Pequeno volume de JSON e Parquet | Baixo custo, mas não zero por definição |
| AWS Glue Data Catalog | Catálogo pequeno | Validar limites atuais |
| Amazon Athena | Consultas ocasionais | Cobrança por dados processados |
| Redshift Serverless | Laboratório analítico | Requer controle rigoroso de uso |
| CloudWatch | Logs essenciais | Retenção deve ser configurada |

## Roadmap

### Fase 1 — Fundação AWS

- [x] Criar repositório
- [x] Definir arquitetura inicial
- [x] Evoluir arquitetura para AWS analítica
- [ ] Criar AWS Budget e alertas
- [ ] Criar estrutura de pastas
- [ ] Configurar ambiente Python
- [ ] Configurar Airflow localmente
- [ ] Criar IAM Roles mínimas
- [ ] Criar bucket S3 com prefixos Bronze e Silver

### Fase 2 — Pipeline batch

- [ ] Implementar extração de preços diários
- [ ] Preservar respostas na camada Bronze
- [ ] Normalizar preços
- [ ] Gerar arquivos Parquet
- [ ] Criar DAG `daily_market_prices`
- [ ] Implementar controle de cota
- [ ] Garantir idempotência e backfill

### Fase 3 — Lakehouse básico

- [ ] Criar tabelas no Glue Catalog
- [ ] Registrar partições Silver
- [ ] Validar dados no Athena
- [ ] Criar consultas de reconciliação
- [ ] Adicionar testes de qualidade

### Fase 4 — Warehouse analítico

- [ ] Criar namespace e workgroup do Redshift Serverless
- [ ] Configurar acesso do Redshift ao S3
- [ ] Criar schemas `staging`, `analytics` e `gold`
- [ ] Implementar carga via `COPY`
- [ ] Criar dimensões e fatos
- [ ] Implementar merges incrementais
- [ ] Criar métricas Gold

### Fase 5 — Deploy e observabilidade

- [ ] Criar instância EC2
- [ ] Instalar Docker e Docker Compose
- [ ] Implantar Airflow mínimo
- [ ] Configurar volumes persistentes
- [ ] Configurar acesso seguro
- [ ] Integrar logs ao CloudWatch
- [ ] Validar execução agendada ponta a ponta

### Fase 6 — Evolução opcional

Após o batch estar estável, poderá ser adicionada uma fonte pública via WebSocket para estudar streaming e microbatch. O consumidor contínuo será um serviço separado; o Airflow processará apenas janelas concluídas.

## Referências oficiais

- [Alpha Vantage — documentação da API](https://www.alphavantage.co/documentation/)
- [Alpha Vantage — suporte e limites](https://www.alphavantage.co/support/)
- [Apache Airflow — documentação](https://airflow.apache.org/docs/)
- [Amazon EC2 — documentação](https://docs.aws.amazon.com/ec2/)
- [Amazon S3 — documentação](https://docs.aws.amazon.com/s3/)
- [AWS Glue Data Catalog — documentação](https://docs.aws.amazon.com/glue/)
- [Amazon Athena — documentação](https://docs.aws.amazon.com/athena/)
- [Amazon Redshift Serverless — documentação](https://docs.aws.amazon.com/redshift/latest/mgmt/working-with-serverless.html)
- [AWS IAM — documentação](https://docs.aws.amazon.com/iam/)
- [Amazon CloudWatch — documentação](https://docs.aws.amazon.com/cloudwatch/)
- [AWS Budgets — documentação](https://docs.aws.amazon.com/cost-management/latest/userguide/budgets-managing-costs.html)

## Aviso

Este projeto tem finalidade exclusivamente educacional. Os indicadores produzidos não constituem recomendação de investimento, análise financeira profissional ou aconselhamento sobre compra e venda de ativos.
