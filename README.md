# MarketPulse Data Pipeline

Pipeline de engenharia de dados para coleta, processamento e análise de dados do mercado financeiro utilizando **Python**, **Apache Airflow**, **Alpha Vantage**, **Supabase/PostgreSQL** e **Oracle Cloud**.

O projeto foi desenhado como um laboratório prático de engenharia de dados com infraestrutura gratuita, baixo custo operacional e foco em conceitos reais: orquestração, ingestão batch, idempotência, controle de cota, qualidade de dados, modelagem analítica e observabilidade.

> **Escopo honesto:** o plano gratuito do Alpha Vantage permite até 25 requisições por dia e não fornece cotações de ações em tempo real gratuitamente. Por isso, o MVP é uma pipeline **batch diária**. Uma trilha de streaming pode ser adicionada futuramente com outra fonte pública.

## Objetivo

Construir uma plataforma capaz de responder:

> Quais ativos apresentaram movimentos relevantes de preço, volume e volatilidade no último pregão?

A primeira versão monitorará uma watchlist pequena de ativos, preservará as respostas brutas da API, normalizará os dados e produzirá indicadores analíticos para consulta.

## Arquitetura

```text
Alpha Vantage
      |
      v
Apache Airflow na Oracle Cloud
      |
      +--> Bronze: JSON/CSV bruto em disco ou OCI Object Storage
      |
      +--> Transformações Python
      |
      v
Supabase / PostgreSQL
      |
      +--> Silver: dados normalizados
      |
      +--> Gold: métricas e sinais analíticos
```

### Responsabilidade de cada componente

| Componente | Responsabilidade |
|---|---|
| Alpha Vantage | Fonte de preços diários e dados financeiros |
| Apache Airflow | Agendamento, dependências, retries, backfills e monitoramento |
| Python | Extração, validação, transformação e carga |
| OCI Object Storage ou disco da VM | Preservação das respostas brutas |
| Supabase/PostgreSQL | Camada analítica e de consulta |
| Oracle Cloud VM | Hospedagem contínua do Airflow |

## Infraestrutura gratuita

A configuração recomendada para o Airflow é uma instância Oracle Cloud **VM.Standard.A1.Flex**, baseada em ARM.

Configuração sugerida:

```text
2 OCPUs
12 GB de RAM
Ubuntu ARM
50–80 GB de disco
```

A franquia Always Free da Oracle equivale a até 2 OCPUs e 12 GB de memória para instâncias A1 Flex. A antiga `VM.Standard.E2.1.Micro`, com 1 GB de RAM, não é adequada para uma instalação estável do Airflow em containers.

Para reduzir o consumo de recursos, o projeto utilizará uma implantação mínima:

```text
Airflow Scheduler
Airflow Webserver/API
LocalExecutor
PostgreSQL local para metadados do Airflow
Máximo de 1–2 tasks simultâneas
```

Não serão utilizados Celery, Redis, Kubernetes ou múltiplos workers no MVP.

## Limites do Alpha Vantage

O plano gratuito permite até **25 requisições por dia**. O projeto adotará um orçamento operacional inferior ao limite para manter margem para testes e retries.

Exemplo com cinco ativos:

```text
5 requisições diárias para preços
5 requisições semanais para fundamentos
Margem restante para testes e reprocessamentos
```

Toda chamada deverá ser registrada antes ou depois da execução para evitar consumo acidental da cota.

## Watchlist inicial

```text
AAPL
MSFT
NVDA
AMZN
GOOGL
```

A watchlist será configurável, mas permanecerá pequena durante o MVP.

## Camadas de dados

### Bronze

Preserva a resposta original da API antes de qualquer transformação.

```text
data/bronze/
└── daily_prices/
    └── extraction_date=YYYY-MM-DD/
        ├── AAPL.json
        ├── MSFT.json
        └── NVDA.json
```

Objetivos:

- permitir auditoria;
- reprocessar sem consumir novamente a API;
- investigar mudanças de schema;
- preservar evidência da extração.

### Silver

Dados limpos, tipados, deduplicados e normalizados no Supabase.

Tabela principal: `fact_daily_price`.

| Campo | Descrição |
|---|---|
| asset_id | Identificador do ativo |
| reference_date | Data do pregão |
| open_price | Preço de abertura |
| high_price | Maior preço |
| low_price | Menor preço |
| close_price | Preço de fechamento |
| volume | Volume negociado |
| extracted_at | Momento da extração |

Chave lógica:

```text
asset_id + reference_date
```

Essa restrição torna a carga idempotente e impede duplicações durante retries ou reprocessamentos.

### Gold

Camada de métricas derivadas para consumo analítico.

Tabela proposta: `gold_asset_daily_signal`.

| Campo | Descrição |
|---|---|
| asset_id | Identificador do ativo |
| reference_date | Data de referência |
| daily_return | Retorno diário |
| volume_change | Variação de volume |
| moving_average_7d | Média móvel de 7 dias |
| moving_average_30d | Média móvel de 30 dias |
| volatility_30d | Volatilidade histórica de 30 dias |
| price_anomaly_score | Indicador de desvio do comportamento recente |
| signal_classification | Classificação analítica |

A classificação não representa recomendação de investimento. Ela apenas identifica movimentos estatisticamente relevantes para fins educacionais.

## Modelo de dados

### `dim_asset`

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

### `fact_daily_price`

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

### `snapshot_company_fundamentals`

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

### `api_request_log`

```text
request_id
endpoint
symbol
requested_at
status_code
response_type
dag_id
task_id
attempt_number
```

### `gold_asset_daily_signal`

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
save_raw_response
      ↓
normalize_prices
      ↓
upsert_supabase
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
save_raw_response
      ↓
normalize_fundamentals
      ↓
upsert_supabase
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
- data do pregão não futura.

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
├── sql/
│   ├── ddl/
│   └── analytics/
├── tests/
│   ├── unit/
│   ├── integration/
│   └── fixtures/
├── data/
│   ├── bronze/
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

Executar novamente uma DAG para a mesma data não poderá duplicar registros. As cargas utilizarão chaves únicas e operações de upsert.

### Backfill

As DAGs deverão aceitar uma data de referência para permitir reprocessar períodos anteriores sem alterar o código.

### Controle de cota

O pipeline verificará o `api_request_log` antes de cada chamada. Uma task não deverá consumir a API quando o orçamento diário estiver próximo do limite.

### Observabilidade

Cada execução deverá registrar:

```text
requisições realizadas
ativos processados
registros extraídos
registros carregados
registros rejeitados
duração da task
status da carga
data máxima disponível
```

### Data quality

Respostas HTTP bem-sucedidas não serão consideradas automaticamente válidas. A API pode devolver mensagens de limite ou erro dentro de uma resposta tecnicamente aceita.

## Segurança

- não versionar a chave do Alpha Vantage;
- não versionar credenciais do Supabase;
- utilizar variáveis de ambiente ou Connections do Airflow;
- manter `.env` fora do Git;
- disponibilizar somente `.env.example`;
- evitar exposição pública direta da interface do Airflow;
- acessar o Airflow por túnel SSH ou proxy autenticado;
- manter o PostgreSQL de metadados do Airflow separado do banco analítico;
- preservar dados importantes fora do disco efêmero da VM.

## Roadmap

### Fase 1 — Fundação

- [x] Criar repositório
- [x] Definir arquitetura inicial
- [ ] Criar estrutura de pastas
- [ ] Configurar ambiente Python
- [ ] Criar projeto Supabase
- [ ] Definir tabelas e constraints
- [ ] Configurar Airflow localmente

### Fase 2 — Pipeline batch

- [ ] Implementar extração de preços diários
- [ ] Preservar respostas na camada bronze
- [ ] Normalizar preços
- [ ] Implementar carga idempotente no Supabase
- [ ] Criar DAG `daily_market_prices`
- [ ] Implementar controle de cota

### Fase 3 — Qualidade e analytics

- [ ] Criar validações de qualidade
- [ ] Calcular retornos e médias móveis
- [ ] Calcular volatilidade histórica
- [ ] Criar tabela gold
- [ ] Adicionar consultas analíticas
- [ ] Criar testes unitários e de integração

### Fase 4 — Deploy

- [ ] Criar VM Oracle A1 Flex
- [ ] Instalar Docker e Docker Compose
- [ ] Implantar Airflow mínimo
- [ ] Configurar volumes persistentes
- [ ] Configurar acesso seguro
- [ ] Validar execução agendada

### Fase 5 — Evolução opcional

Após o batch estar estável, poderá ser adicionada uma fonte pública via WebSocket para estudar streaming e microbatch. O consumidor contínuo será um serviço separado; o Airflow processará apenas janelas concluídas.

## Custos

O projeto foi planejado para operar dentro de planos gratuitos:

| Serviço | Uso planejado | Custo esperado |
|---|---|---:|
| Alpha Vantage | Até 25 requisições diárias | R$ 0 |
| Oracle Cloud A1 Flex | Dentro da franquia Always Free | R$ 0 |
| Supabase | Plano gratuito | R$ 0 |
| Apache Airflow | Código aberto | R$ 0 |
| Python | Código aberto | R$ 0 |

Os limites e condições dos provedores podem mudar. Antes do deploy, confirme a elegibilidade dos recursos e configure alertas de orçamento na Oracle Cloud.

## Referências oficiais

- [Alpha Vantage — documentação da API](https://www.alphavantage.co/documentation/)
- [Alpha Vantage — suporte e limites](https://www.alphavantage.co/support/)
- [Apache Airflow — documentação](https://airflow.apache.org/docs/)
- [Oracle Cloud — recursos Always Free](https://docs.oracle.com/en-us/iaas/Content/FreeTier/freetier_topic-Always_Free_Resources.htm)
- [Supabase — documentação](https://supabase.com/docs)

## Aviso

Este projeto tem finalidade exclusivamente educacional. Os indicadores produzidos não constituem recomendação de investimento, análise financeira profissional ou aconselhamento sobre compra e venda de ativos.
