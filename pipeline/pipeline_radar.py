"""
Pipeline MongoDB → Redis — Radar Combustível
============================================
Fase batch: lê agregações do MongoDB e popula estruturas no Redis.
Fase stream: Change Stream em eventos_preco para atualização em tempo real.

Uso (dentro do container):
  docker compose exec app python pipeline/pipeline_radar.py
  docker compose exec app python pipeline/pipeline_radar.py --batch-only

Uso (fora do container):
  MONGO_URI=mongodb://localhost:27017/?directConnection=true REDIS_HOST=localhost python pipeline/pipeline_radar.py

Estruturas Redis geradas:
  HASH  posto:{id}                    cadastro resumido (nome, bandeira, cidade, uf)
  GEO   geo:postos                    coordenadas de todos os postos
  ZSET  ranking:preco:{comb}:{uf}     menor preço por combustível/estado (score=preço)
  ZSET  ranking:buscas                cidades com mais buscas (score=contagem)
  ZSET  ranking:variacao:{comb}       maior variação de preço absoluta (score=|var%|)
  HASH  stats:preco_medio             min/avg/max por combustível
  TS    ts:preco_avg:{comb}:{uf}      série temporal de preço médio diário
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any

from dotenv import load_dotenv
from pymongo import MongoClient
from redis import Redis
from redis.exceptions import ResponseError

load_dotenv(".env.local", override=False)
load_dotenv(override=False)

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017/?directConnection=true")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
DB_NAME = os.getenv("MONGO_DB", "radar_combustivel")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

COMBUSTIVEIS = (
    "GASOLINA_COMUM",
    "GASOLINA_ADITIVADA",
    "ETANOL",
    "DIESEL_S10",
    "DIESEL_COMUM",
    "GNV",
)


# ---------------------------------------------------------------------------
# Conexões
# ---------------------------------------------------------------------------


def get_mongo() -> MongoClient:
    return MongoClient(MONGO_URI, serverSelectionTimeoutMS=15_000)


def get_redis() -> Redis:
    return Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


# ---------------------------------------------------------------------------
# TimeSeries helper
# ---------------------------------------------------------------------------


def ts_add(
    r: Redis, key: str, ts_ms: int, value: float, labels: dict[str, str]
) -> None:
    try:
        r.execute_command("TS.ADD", key, ts_ms, value, "ON_DUPLICATE", "LAST")
    except ResponseError as exc:
        msg = str(exc).lower()
        if (
            "key does not exist" not in msg
            and "tsdb: the key does not exist" not in msg
        ):
            raise
        args = ["TS.CREATE", key, "RETENTION", 0, "DUPLICATE_POLICY", "LAST", "LABELS"]
        for k, v in labels.items():
            args += [k, v]
        r.execute_command(*args)
        r.execute_command("TS.ADD", key, ts_ms, value, "ON_DUPLICATE", "LAST")


# ---------------------------------------------------------------------------
# Fase batch — funções por estrutura
# ---------------------------------------------------------------------------


def batch_postos(db, r: Redis) -> int:
    """
    HASH posto:{id}  →  nome, bandeira, cidade, uf, ativo
    GEO  geo:postos  →  lng/lat de cada posto
    """
    r.delete("geo:postos")

    geo_batch: list = []
    n = 0
    for posto in db.postos.find(
        {},
        {
            "nome_fantasia": 1,
            "bandeira": 1,
            "endereco.cidade": 1,
            "endereco.estado": 1,
            "ativo": 1,
            "location": 1,
        },
    ):
        pid = str(posto["_id"])
        r.hset(
            f"posto:{pid}",
            mapping={
                "nome": posto.get("nome_fantasia", ""),
                "bandeira": posto.get("bandeira", ""),
                "cidade": posto.get("endereco", {}).get("cidade", ""),
                "uf": posto.get("endereco", {}).get("estado", ""),
                "ativo": int(bool(posto.get("ativo", True))),
            },
        )
        loc = posto.get("location", {})
        if loc.get("type") == "Point":
            lng, lat = loc["coordinates"]
            geo_batch += [lng, lat, pid]

        # Geoadd em lotes de 500 para não estourar memória
        if len(geo_batch) >= 1500:
            r.geoadd("geo:postos", geo_batch)
            geo_batch = []
        n += 1

    if geo_batch:
        r.geoadd("geo:postos", geo_batch)

    log.info("[batch_postos] %d postos → HASH posto:{id} + GEO geo:postos", n)
    return n


def batch_rankings_preco(db, r: Redis) -> None:
    """
    ZSET ranking:preco:{combustivel}:{uf}
    Score = preço mais recente; menor score = mais barato.
    Usa o hash posto:{id} (já populado) para resolver a UF.
    """
    # Apaga chaves antigas
    for key in r.scan_iter("ranking:preco:*"):
        r.delete(key)

    pipeline = [
        {"$sort": {"ocorrido_em": -1}},
        {
            "$group": {
                "_id": {"posto_id": "$posto_id", "combustivel": "$combustivel"},
                "preco": {"$first": "$preco_novo"},
            }
        },
    ]

    n = 0
    for row in db.eventos_preco.aggregate(pipeline, allowDiskUse=True):
        comb = row["_id"]["combustivel"]
        pid = str(row["_id"]["posto_id"])
        preco = float(row["preco"])
        uf = r.hget(f"posto:{pid}", "uf") or "XX"
        r.zadd(f"ranking:preco:{comb}:{uf}", {pid: preco})
        n += 1

    log.info("[batch_rankings_preco] %d entradas → ZSET ranking:preco:*", n)


def batch_rankings_buscas(db, r: Redis) -> None:
    """
    ZSET ranking:buscas  →  member="cidade|uf", score=contagem de buscas
    """
    r.delete("ranking:buscas")

    pipeline = [
        {
            "$group": {
                "_id": {"cidade": "$cidade", "estado": "$estado"},
                "count": {"$sum": 1},
            }
        }
    ]

    n = 0
    for row in db.buscas_usuarios.aggregate(pipeline):
        cidade = row["_id"].get("cidade", "")
        estado = row["_id"].get("estado", "")
        member = f"{cidade}|{estado}"
        r.zadd("ranking:buscas", {member: float(row["count"])})
        n += 1

    log.info("[batch_rankings_buscas] %d cidades → ZSET ranking:buscas", n)


def batch_rankings_variacao(db, r: Redis) -> None:
    """
    ZSET ranking:variacao:{combustivel}  →  score=|variacao_pct| mais recente
    """
    for key in r.scan_iter("ranking:variacao:*"):
        r.delete(key)

    pipeline = [
        {"$sort": {"ocorrido_em": -1}},
        {
            "$group": {
                "_id": {"posto_id": "$posto_id", "combustivel": "$combustivel"},
                "variacao_pct": {"$first": "$variacao_pct"},
            }
        },
        {
            "$project": {
                "posto_id": "$_id.posto_id",
                "combustivel": "$_id.combustivel",
                "variacao_abs": {"$abs": "$variacao_pct"},
            }
        },
    ]

    n = 0
    for row in db.eventos_preco.aggregate(pipeline, allowDiskUse=True):
        comb = row["combustivel"]
        pid = str(row["posto_id"])
        score = float(row.get("variacao_abs", 0))
        r.zadd(f"ranking:variacao:{comb}", {pid: score})
        n += 1

    log.info("[batch_rankings_variacao] %d entradas → ZSET ranking:variacao:*", n)


def batch_timeseries(db, r: Redis) -> None:
    """
    TS ts:preco_avg:{combustivel}:{uf}  →  preço médio diário por combustível/UF
    Agrega eventos_preco por dia+combustivel+uf e salva como série temporal.
    """
    for key in r.scan_iter("ts:preco_avg:*"):
        r.delete(key)

    pipeline = [
        {
            "$lookup": {
                "from": "postos",
                "localField": "posto_id",
                "foreignField": "_id",
                "as": "posto",
            }
        },
        {"$unwind": "$posto"},
        {
            "$group": {
                "_id": {
                    "data": {
                        "$dateToString": {"format": "%Y-%m-%d", "date": "$ocorrido_em"}
                    },
                    "combustivel": "$combustivel",
                    "uf": "$posto.endereco.estado",
                },
                "preco_avg": {"$avg": "$preco_novo"},
                "ts": {"$min": "$ocorrido_em"},
            }
        },
        {"$sort": {"_id.data": 1}},
    ]

    n = 0
    for row in db.eventos_preco.aggregate(pipeline, allowDiskUse=True):
        comb = row["_id"]["combustivel"]
        uf = row["_id"]["uf"]
        ts_dt = row.get("ts")
        if ts_dt is None or not hasattr(ts_dt, "timestamp"):
            continue
        ts_ms = int(ts_dt.timestamp() * 1000)
        preco_avg = float(row["preco_avg"])
        ts_add(
            r,
            f"ts:preco_avg:{comb}:{uf}",
            ts_ms,
            preco_avg,
            {"combustivel": comb, "uf": uf},
        )
        n += 1

    log.info("[batch_timeseries] %d pontos → TS ts:preco_avg:*", n)


def batch_stats_globais(db, r: Redis) -> None:
    """
    HASH stats:preco_medio  →  {combustivel}:min / avg / max
    """
    pipeline = [
        {
            "$group": {
                "_id": "$combustivel",
                "avg": {"$avg": "$preco_novo"},
                "min": {"$min": "$preco_novo"},
                "max": {"$max": "$preco_novo"},
                "count": {"$sum": 1},
            }
        }
    ]

    mapping: dict[str, str] = {}
    for row in db.eventos_preco.aggregate(pipeline):
        comb = row["_id"]
        mapping[f"{comb}:avg"] = str(round(float(row["avg"]), 3))
        mapping[f"{comb}:min"] = str(round(float(row["min"]), 3))
        mapping[f"{comb}:max"] = str(round(float(row["max"]), 3))
        mapping[f"{comb}:count"] = str(int(row["count"]))

    if mapping:
        r.hset("stats:preco_medio", mapping=mapping)

    log.info(
        "[batch_stats_globais] stats:preco_medio atualizado (%d combustiveis)",
        len(mapping) // 4,
    )


def run_batch(db, r: Redis) -> None:
    log.info("=" * 50)
    log.info("BATCH INICIO — MongoDB → Redis")
    log.info("=" * 50)
    t0 = time.time()
    batch_postos(db, r)
    batch_rankings_preco(db, r)
    batch_rankings_buscas(db, r)
    batch_rankings_variacao(db, r)
    batch_stats_globais(db, r)
    batch_timeseries(db, r)
    log.info("BATCH CONCLUIDO em %.1fs", time.time() - t0)
    log.info("=" * 50)


# ---------------------------------------------------------------------------
# Fase stream — Change Stream em eventos_preco
# ---------------------------------------------------------------------------


def handle_novo_evento(r: Redis, doc: dict[str, Any]) -> None:
    """Atualiza rankings e TimeSeries em tempo real para um novo evento de preço."""
    pid = str(doc.get("posto_id", ""))
    comb = str(doc.get("combustivel", ""))
    preco = float(doc.get("preco_novo", 0))
    variacao = float(doc.get("variacao_pct", 0))
    ocorrido = doc.get("ocorrido_em")

    uf = r.hget(f"posto:{pid}", "uf") or "XX"

    r.zadd(f"ranking:preco:{comb}:{uf}", {pid: preco})
    r.zadd(f"ranking:variacao:{comb}", {pid: abs(variacao)})

    if ocorrido and hasattr(ocorrido, "timestamp"):
        ts_ms = int(ocorrido.timestamp() * 1000)
        ts_add(
            r,
            f"ts:preco_avg:{comb}:{uf}",
            ts_ms,
            preco,
            {"combustivel": comb, "uf": uf},
        )

    log.info("[STREAM] %s | %s | R$ %.3f | var %.2f%%", pid[:8], comb, preco, variacao)


def run_change_stream(db, r: Redis) -> None:
    log.info("[STREAM] Aguardando eventos em eventos_preco (Change Stream)...")
    col = db.eventos_preco
    while True:
        try:
            with col.watch(
                [{"$match": {"operationType": "insert"}}],
                full_document="updateLookup",
            ) as stream:
                for change in stream:
                    handle_novo_evento(r, change["fullDocument"])
        except Exception as exc:
            log.warning("[STREAM] Reconectando após erro: %s", exc)
            time.sleep(3)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description="Pipeline MongoDB → Redis — Radar Combustível"
    )
    parser.add_argument(
        "--batch-only",
        action="store_true",
        help="Executa apenas o batch inicial sem Change Stream",
    )
    args = parser.parse_args()

    log.info("Conectando MongoDB: %s / %s", MONGO_URI, DB_NAME)
    log.info("Conectando Redis: %s:%s", REDIS_HOST, REDIS_PORT)

    mongo = get_mongo()
    mongo.admin.command("ping")
    db = mongo[DB_NAME]

    r = get_redis()
    r.ping()

    run_batch(db, r)

    if not args.batch_only:
        run_change_stream(db, r)


if __name__ == "__main__":
    main()
