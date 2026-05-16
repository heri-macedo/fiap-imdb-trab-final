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
    """Cria e retorna uma conexão com o MongoDB.

    Returns:
        MongoClient: Cliente MongoDB conectado ao URI configurado.
    """
    return MongoClient(MONGO_URI, serverSelectionTimeoutMS=15_000)


def get_redis() -> Redis:
    """Cria e retorna uma conexão com o Redis.

    Returns:
        Redis: Cliente Redis conectado ao host e porta configurados.
    """
    return Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


# ---------------------------------------------------------------------------
# TimeSeries helper
# ---------------------------------------------------------------------------


def ts_add(
    r: Redis, key: str, ts_ms: int, value: float, labels: dict[str, str]
) -> None:
    """Insere um ponto em uma série temporal Redis, criando a chave se não existir.

    Args:
        r (Redis): Cliente Redis ativo.
        key (str): Nome da chave TimeSeries (ex: ``ts:preco_avg:ETANOL:SP``).
        ts_ms (int): Timestamp em milissegundos.
        value (float): Valor a ser inserido.
        labels (dict[str, str]): Labels associadas à série (ex: ``{"combustivel": "ETANOL", "uf": "SP"}``).
    """
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
    """Popula os HASHes de cadastro de postos e o índice GEO no Redis.

    Para cada posto na coleção MongoDB, cria um HASH ``posto:{id}`` com dados
    resumidos e adiciona as coordenadas ao índice ``geo:postos`` em lotes de 500.

    Args:
        db: Banco de dados MongoDB (objeto ``Database`` do PyMongo).
        r (Redis): Cliente Redis ativo.

    Returns:
        int: Total de postos processados.
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
    """Popula os rankings de menor preço por combustível e UF no Redis.

    Agrega ``eventos_preco`` por posto e combustível, seleciona o preço mais
    recente e insere em ``ranking:preco:{combustivel}:{uf}`` (ZSET, score=preço).
    Usa ``HGET posto:{id} uf`` para resolver o estado sem novo acesso ao MongoDB.

    Args:
        db: Banco de dados MongoDB (objeto ``Database`` do PyMongo).
        r (Redis): Cliente Redis ativo.
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
    """Popula o ranking de cidades por volume de buscas no Redis.

    Agrega ``buscas_usuarios`` por cidade e estado e insere em ``ranking:buscas``
    (ZSET, member=``"cidade|uf"``, score=contagem de buscas).

    Args:
        db: Banco de dados MongoDB (objeto ``Database`` do PyMongo).
        r (Redis): Cliente Redis ativo.
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
    """Popula os rankings de variação de preço por combustível no Redis.

    Agrega ``eventos_preco`` por posto e combustível e insere em
    ``ranking:variacao:{combustivel}`` (ZSET, score=variação percentual absoluta).

    Args:
        db: Banco de dados MongoDB (objeto ``Database`` do PyMongo).
        r (Redis): Cliente Redis ativo.
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
    """Popula as séries temporais de preço médio diário no Redis.

    Agrega ``eventos_preco`` por dia, combustível e UF (via ``$lookup`` em postos)
    e insere cada ponto em ``ts:preco_avg:{combustivel}:{uf}`` usando RedisTimeSeries.

    Args:
        db: Banco de dados MongoDB (objeto ``Database`` do PyMongo).
        r (Redis): Cliente Redis ativo.
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
    """Popula as estatísticas globais de preço por combustível no Redis.

    Agrega ``eventos_preco`` calculando mínimo, média, máximo e contagem por
    combustível e armazena em ``stats:preco_medio`` (HASH, campos ``{comb}:min``,
    ``{comb}:avg``, ``{comb}:max``, ``{comb}:count``).

    Args:
        db: Banco de dados MongoDB (objeto ``Database`` do PyMongo).
        r (Redis): Cliente Redis ativo.
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
    """Executa todas as etapas do batch em sequência e registra o tempo total.

    Args:
        db: Banco de dados MongoDB (objeto ``Database`` do PyMongo).
        r (Redis): Cliente Redis ativo.
    """
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
    """Processa um novo evento de preço e atualiza as estruturas Redis correspondentes.

    Resolve a UF do posto via ``HGET`` (O(1)) e atualiza o ranking de preços,
    o ranking de variação e a série temporal sem acessar o MongoDB.

    Args:
        r (Redis): Cliente Redis ativo.
        doc (dict[str, Any]): Documento completo do evento (``fullDocument`` do Change Stream).
    """
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
    """Escuta o Change Stream de ``eventos_preco`` e processa eventos continuamente.

    Filtra apenas operações de ``insert`` e delega cada evento a
    :func:`handle_novo_evento`. Reconecta automaticamente com backoff de 3s
    em caso de falha de rede ou failover do replica set.

    Args:
        db: Banco de dados MongoDB (objeto ``Database`` do PyMongo).
        r (Redis): Cliente Redis ativo.
    """
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
    """Ponto de entrada do pipeline. Conecta ao MongoDB e Redis, executa o batch
    e opcionalmente inicia o Change Stream.

    Raises:
        SystemExit: Se a conexão com MongoDB ou Redis falhar.
    """
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
