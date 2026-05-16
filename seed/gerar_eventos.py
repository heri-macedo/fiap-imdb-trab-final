"""Gerador de eventos de preço para testar o Change Stream em tempo real.

Insere documentos em ``eventos_preco`` no MongoDB a uma taxa configurável,
permitindo observar o pipeline reagir em tempo real via Change Stream.

Uso:
    docker compose exec pipeline python seed/gerar_eventos.py
    docker compose exec pipeline python seed/gerar_eventos.py --events 200 --rps 2
"""

from __future__ import annotations

import argparse
import logging
import os
import random
import time
from datetime import timezone

from bson import ObjectId
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv(".env.local", override=False)
load_dotenv(override=False)

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017/?directConnection=true")
DB_NAME = os.getenv("MONGO_DB", "radar_combustivel")

COMBUSTIVEIS = (
    "GASOLINA_COMUM",
    "GASOLINA_ADITIVADA",
    "ETANOL",
    "DIESEL_S10",
    "DIESEL_COMUM",
    "GNV",
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def gerar_evento(posto_id: ObjectId) -> dict:
    """Gera um documento de evento de preço para um posto existente.

    Args:
        posto_id (ObjectId): ID de um posto existente na coleção ``postos``.

    Returns:
        dict: Documento pronto para inserção em ``eventos_preco``.
    """
    comb = random.choice(COMBUSTIVEIS)
    preco_novo = round(random.uniform(4.5, 8.9), 3)
    preco_ant = round(max(3.0, preco_novo + random.uniform(-0.8, 0.8)), 3)
    return {
        "_id": ObjectId(),
        "posto_id": posto_id,
        "combustivel": comb,
        "preco_anterior": preco_ant,
        "preco_novo": preco_novo,
        "variacao_pct": round((preco_novo - preco_ant) / preco_ant * 100, 4),
        "unidade": "BRL_L",
        "fonte": "simulador",
        "ocorrido_em": __import__("datetime").datetime.now(timezone.utc),
        "revisado": False,
    }


def main() -> None:
    """Ponto de entrada do gerador de eventos.

    Lê posto_ids existentes do MongoDB e insere eventos em ``eventos_preco``
    na taxa especificada, para acionar o Change Stream do pipeline.

    Raises:
        SystemExit: Se não houver postos na base ou a conexão falhar.
    """
    parser = argparse.ArgumentParser(
        description="Gera eventos de preço para testar o Change Stream"
    )
    parser.add_argument(
        "--events",
        "-n",
        type=int,
        default=100,
        help="Número de eventos a gerar (default: 100)",
    )
    parser.add_argument(
        "--rps", type=float, default=5.0, help="Eventos por segundo (default: 5.0)"
    )
    args = parser.parse_args()

    log.info("Conectando: %s / %s", MONGO_URI, DB_NAME)
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10_000)
    db = client[DB_NAME]

    log.info("Carregando posto_ids...")
    posto_ids = [doc["_id"] for doc in db.postos.find({}, {"_id": 1})]
    if not posto_ids:
        log.error("Nenhum posto encontrado. Execute o seed primeiro.")
        raise SystemExit(1)

    log.info("%d postos disponíveis.", len(posto_ids))
    log.info("Gerando %d eventos a %.1f ev/s...", args.events, args.rps)
    log.info("=" * 50)

    intervalo = 1.0 / args.rps
    col = db.eventos_preco

    for i in range(1, args.events + 1):
        evento = gerar_evento(random.choice(posto_ids))
        col.insert_one(evento)
        log.info(
            "[%d/%d] posto=%.8s | %s | R$ %.3f | var %.2f%%",
            i,
            args.events,
            str(evento["posto_id"]),
            evento["combustivel"],
            evento["preco_novo"],
            evento["variacao_pct"],
        )
        if i < args.events:
            time.sleep(intervalo)

    log.info("=" * 50)
    log.info("Concluído. %d eventos inseridos em eventos_preco.", args.events)
    client.close()


if __name__ == "__main__":
    main()
