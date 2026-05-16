N   ?= 100
RPS ?= 5

.PHONY: up down reset seed pipeline events logs-pipeline logs-seed redis-cli help

## Sobe todos os containers (seed + pipeline automáticos)
up:
	docker compose up -d

## Derruba todos os containers sem apagar volumes
down:
	docker compose down

## Derruba tudo, apaga volumes e sobe do zero
reset:
	docker compose down -v
	docker compose up -d

## Roda o seed manualmente (útil após reset sem aguardar container)
seed:
	docker compose exec pipeline python seed/seed_radar_combustivel.py

## Roda o pipeline em modo batch apenas
pipeline:
	docker compose exec pipeline python pipeline/pipeline_radar.py --batch-only

## Gera N eventos a RPS eventos/segundo para testar o Change Stream
## Uso: make events N=200 RPS=2
events:
	docker compose exec pipeline python seed/gerar_eventos.py --events $(N) --rps $(RPS)

## Acompanha logs do container do pipeline em tempo real
logs-pipeline:
	docker compose logs pipeline -f

## Acompanha logs do container do seed
logs-seed:
	docker compose logs seed -f

## Abre o redis-cli interativo
redis-cli:
	docker compose exec redis redis-cli

## Lista todos os targets disponíveis
help:
	@grep -E '^##' Makefile | sed 's/^## //'
