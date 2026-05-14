import os
import time
from typing import List, Tuple

from dotenv import load_dotenv
from redis import Redis
from redis.commands.search.query import NumericFilter, Query

# Load .env.local first (for host development), fallback to .env (for Docker)
load_dotenv(".env.local")
load_dotenv()

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))


def top_restaurants(redis: Redis, n: int = 10) -> List[Tuple[str, float]]:
    return redis.zrevrange("ranking:restaurants:views", 0, n - 1, withscores=True)


def top_dishes(redis: Redis, n: int = 5) -> List[Tuple[str, float]]:
    return redis.zrevrange("ranking:dishes:searches", 0, n - 1, withscores=True)


def dish_name(redis: Redis, dish_id: str) -> str:
    name = redis.hget(f"dish:{dish_id}", "dish_name")
    return name or dish_id


def pizza_pinheiros(redis: Redis):
    query = (
        Query("@cuisine:{pizza} @neighborhood:{Pinheiros}")
        .add_filter(NumericFilter("stars", 4.5, 5))
        .sort_by("views", asc=False)
        .paging(0, 10)
    )
    return redis.ft("idx:restaurants").search(query)


def views_series(redis: Redis, restaurant_numeric_id: str = "245"):
    key = f"ts:resto:{restaurant_numeric_id}:views"
    return redis.execute_command("TS.RANGE", key, "-", "+", "AGGREGATION", "sum", "60000")


def print_block(title: str) -> None:
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def main() -> None:
    redis = Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    print("[READER] Consultas em tempo real iniciadas.")

    while True:
        print_block("Top 10 restaurantes mais visitados")
        for idx, (member, score) in enumerate(top_restaurants(redis), start=1):
            print(f"{idx:02d}. {member} -> {int(score)} views")

        print_block("Top 5 pratos mais buscados")
        for idx, (member, score) in enumerate(top_dishes(redis), start=1):
            print(f"{idx:02d}. {dish_name(redis, member)} ({member}) -> {int(score)} buscas")

        print_block("Pizza em Pinheiros com 4.5+ estrelas (RediSearch)")
        try:
            result = pizza_pinheiros(redis)
            if result.total == 0:
                print("Nenhum resultado para @cuisine:{pizza} @neighborhood:{Pinheiros}.")
            else:
                for doc in result.docs[:10]:
                    print(
                        f"{doc.id} | {getattr(doc, 'restaurant_name', '-')}"
                        f" | stars={getattr(doc, 'stars', '-')}"
                        f" | views={getattr(doc, 'views', '-')}"
                    )
        except Exception as exc:
            print(f"Falha na busca RediSearch: {exc}")

        print_block("Série temporal de views do restaurante 245 (agregação por minuto)")
        try:
            series = views_series(redis, "245")
            if not series:
                print("Sem dados de série temporal para ts:resto:245:views.")
            else:
                for point in series[-10:]:
                    ts, value = point
                    print(f"{ts} -> {value}")
        except Exception as exc:
            print(f"Falha na TimeSeries: {exc}")

        time.sleep(5)


if __name__ == "__main__":
    main()
