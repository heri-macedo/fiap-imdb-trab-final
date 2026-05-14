# Radar Combustível — Pipeline MongoDB → Redis

Pipeline de dados em tempo quase real que captura eventos do MongoDB e disponibiliza consultas rápidas via Redis, com dashboard Streamlit.

## Arquitetura

```
┌──────────────────────────────────────────────────────────────────┐
│                        Docker Compose                            │
│                                                                  │
│  ┌─────────────┐    ┌──────────────────────┐    ┌────────────┐  │
│  │   MongoDB   │    │  pipeline_radar.py   │    │   Redis    │  │
│  │  (rs0)      │───▶│                      │───▶│  Stack     │  │
│  │             │    │  [1] Batch           │    │            │  │
│  │  postos     │    │  [2] Change Stream   │    │  HASH      │  │
│  │  eventos_   │◀───│      (tempo real)    │    │  ZSET      │  │
│  │  preco      │    └──────────────────────┘    │  GEO       │  │
│  │  buscas_    │                                │  TimeSeries│  │
│  │  usuarios   │                                └─────┬──────┘  │
│  │  avaliacoes │                                      │          │
│  │  localizacoes│                           ┌─────────▼──────┐  │
│  └─────────────┘                            │   Streamlit    │  │
│                                             │  :8501         │  │
│                                             └────────────────┘  │
└──────────────────────────────────────────────────────────────────┘
```

## Estruturas Redis

| Chave | Tipo | Conteúdo | Consulta atendida |
|---|---|---|---|
| `posto:{id}` | HASH | nome, bandeira, cidade, uf, ativo | Cadastro rápido de postos |
| `geo:postos` | GEO | coordenadas de todos os postos | Busca por proximidade |
| `ranking:preco:{comb}:{uf}` | ZSET | score = preço mais recente | Menor preço por combustível/estado |
| `ranking:buscas` | ZSET | score = contagem de buscas | Cidades com maior demanda |
| `ranking:variacao:{comb}` | ZSET | score = \|variação%\| | Postos com maior oscilação |
| `stats:preco_medio` | HASH | min/avg/max por combustível | Resumo de mercado |
| `ts:preco_avg:{comb}:{uf}` | TimeSeries | preço médio diário | Evolução temporal de preços |

## Pré-requisitos

- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- [MongoDB Compass](https://www.mongodb.com/products/compass) (opcional)
- [RedisInsight](https://redis.io/insight/) (opcional)

## Execução

### 1. Subir os containers

```bash
docker compose up -d
```

| Container | Porta | Descrição |
|---|---|---|
| `lab-mongo` | 27017 | MongoDB 7 com replica set `rs0` |
| `lab-redis` | 6379 / 8001 | Redis Stack (RedisInsight em :8001) |
| `lab-app` | — | Python com dependências instaladas |
| `lab-streamlit` | 8501 | Dashboard Streamlit |

### 2. Popular o MongoDB

```bash
docker compose exec app python seed_radar_combustivel.py
```

Insere ~100.000 documentos em cada uma das 5 coleções (500K docs no total).

### 3. Executar o pipeline

```bash
# Batch + Change Stream (fica rodando em tempo real)
docker compose exec app python pipeline/pipeline_radar.py

# Somente batch (encerra após completar)
docker compose exec app python pipeline/pipeline_radar.py --batch-only
```

O batch leva ~40s. Após concluir, o pipeline aguarda novos eventos via Change Stream e atualiza o Redis em tempo real.

### 4. Acessar o dashboard

**http://localhost:8501**

### Conexões opcionais

```
# MongoDB Compass
mongodb://localhost:27017/?directConnection=true

# RedisInsight
http://localhost:8001
```

## Estrutura do repositório

```
lab-streaming-mongo-redis/
├── docker-compose.yml              # Orquestração dos serviços
├── requirements.txt                # Dependências Python
├── seed_radar_combustivel.py       # Gerador de dados fake (MongoDB)
├── pipeline/
│   └── pipeline_radar.py          # Pipeline MongoDB → Redis
├── queries/
│   └── dashboard_radar.py         # Dashboard Streamlit (6 painéis)
├── .env.example                    # Template de variáveis de ambiente
└── .env.local                      # Configuração local (fora do Docker)
```

## Variáveis de ambiente

| Variável | Dentro do Docker | Fora do Docker |
|---|---|---|
| `MONGO_URI` | `mongodb://mongo:27017/?directConnection=true` | `mongodb://localhost:27017/?directConnection=true` |
| `MONGO_DB` | `radar_combustivel` | `radar_combustivel` |
| `REDIS_HOST` | `redis` | `localhost` |
| `REDIS_PORT` | `6379` | `6379` |

## Painéis do Dashboard

| Painel | Estrutura Redis | Conteúdo |
|---|---|---|
| Resumo Global | `stats:preco_medio` (HASH) | Preço mín/médio/máx por combustível |
| Rankings de Preço | `ranking:preco:*` (ZSET) | Top N postos mais baratos por combustível e estado |
| Volume de Buscas | `ranking:buscas` (ZSET) | Cidades com maior volume de buscas |
| Variação de Preço | `ranking:variacao:*` (ZSET) | Postos com maior oscilação recente |
| Série Temporal | `ts:preco_avg:*` (TimeSeries) | Evolução do preço médio diário |
| Busca Geográfica | `geo:postos` (GEO) | Postos próximos via GEOSEARCH + mapa |

## Consultas Redis de demonstração

```bash
# Abrir redis-cli
docker compose exec redis redis-cli

# Top 10 postos mais baratos (Gasolina Comum em SP)
ZRANGE ranking:preco:GASOLINA_COMUM:SP 0 9 WITHSCORES

# Top 10 cidades com mais buscas
ZREVRANGE ranking:buscas 0 9 WITHSCORES

# Preço médio global por combustível
HGETALL stats:preco_medio

# Postos em raio de 50 km de São Paulo
GEOSEARCH geo:postos FROMLONLAT -46.6333 -23.5505 BYRADIUS 50 km ASC COUNT 10

# Série temporal de preço médio (Gasolina Comum / SP)
TS.RANGE ts:preco_avg:GASOLINA_COMUM:SP - +
```
