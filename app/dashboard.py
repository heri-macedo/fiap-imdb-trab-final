"""
Dashboard Streamlit — Radar Combustível
=======================================
Visualiza dados servidos pelo Redis, alimentados via pipeline MongoDB → Redis.

Uso (dentro do container):
  docker compose exec streamlit streamlit run queries/dashboard_radar.py

Uso (fora do container):
  REDIS_HOST=localhost streamlit run queries/dashboard_radar.py
"""

from __future__ import annotations

import os
import time
from datetime import datetime
from typing import Any

import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv
from redis import Redis

load_dotenv(".env.local", override=False)
load_dotenv(override=False)

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

COMBUSTIVEIS = [
    "GASOLINA_COMUM",
    "GASOLINA_ADITIVADA",
    "ETANOL",
    "DIESEL_S10",
    "DIESEL_COMUM",
    "GNV",
]

UFS = ["SP", "RJ", "MG", "PR", "RS", "BA", "PE", "CE", "DF", "GO"]

CIDADES_REFERENCIA = {
    "São Paulo, SP": (-46.6333, -23.5505),
    "Rio de Janeiro, RJ": (-43.1729, -22.9068),
    "Belo Horizonte, MG": (-43.9378, -19.9208),
    "Curitiba, PR": (-49.2733, -25.4290),
    "Porto Alegre, RS": (-51.2177, -30.0346),
    "Brasília, DF": (-47.9292, -15.7801),
    "Salvador, BA": (-38.5108, -12.9714),
    "Recife, PE": (-34.8813, -8.0578),
    "Fortaleza, CE": (-38.5434, -3.7172),
    "Goiânia, GO": (-49.2533, -16.6869),
}


@st.cache_resource
def get_redis() -> Redis:
    """Cria e retorna uma conexão com o Redis (cacheada pelo Streamlit).

    Returns:
        Redis: Cliente Redis conectado ao host e porta configurados.
    """
    return Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def posto_info(r: Redis, posto_id: str) -> dict[str, str]:
    """Retorna os dados cadastrais de um posto a partir do HASH Redis.

    Args:
        r (Redis): Cliente Redis ativo.
        posto_id (str): Identificador do posto (ObjectId como string).

    Returns:
        dict[str, str]: Dicionário com ``nome``, ``bandeira``, ``cidade`` e ``uf``.
            Retorna valores padrão caso o HASH não exista.
    """
    data = r.hgetall(f"posto:{posto_id}")
    return data or {
        "nome": posto_id[:12] + "...",
        "bandeira": "-",
        "cidade": "-",
        "uf": "-",
    }


# ---------------------------------------------------------------------------
# Páginas — sem @st.fragment aqui; o decorator run_every é aplicado no
# dispatch principal para que o intervalo seja configurável pelo sidebar.
# ---------------------------------------------------------------------------


def page_resumo(r: Redis) -> None:
    """Renderiza o painel de resumo global de preços por combustível.

    Lê ``stats:preco_medio`` (HASH) e exibe métricas de mínimo, médio e máximo
    para cada combustível, com gráfico de barras comparativo.

    Args:
        r (Redis): Cliente Redis ativo.
    """
    st.header("Resumo Global de Preços")

    mapping = r.hgetall("stats:preco_medio")
    if not mapping:
        st.warning("Sem dados em `stats:preco_medio`. Execute o pipeline primeiro.")
        st.code(
            "docker compose exec app python pipeline/pipeline_radar.py --batch-only"
        )
        return

    rows = []
    for comb in COMBUSTIVEIS:
        avg = mapping.get(f"{comb}:avg")
        mn = mapping.get(f"{comb}:min")
        mx = mapping.get(f"{comb}:max")
        count = mapping.get(f"{comb}:count", "0")
        if avg:
            rows.append(
                {
                    "Combustível": comb.replace("_", " ").title(),
                    "Mínimo (R$/L)": float(mn),
                    "Médio (R$/L)": float(avg),
                    "Máximo (R$/L)": float(mx),
                    "Eventos": int(count),
                }
            )

    if not rows:
        st.info("Dados ainda não disponíveis.")
        return

    df = pd.DataFrame(rows)

    col1, col2, col3 = st.columns(3)
    gaso = df[df["Combustível"] == "Gasolina Comum"]
    if not gaso.empty:
        col1.metric(
            "Gasolina Comum (média)", f"R$ {gaso['Médio (R$/L)'].values[0]:.3f}"
        )
    etanol = df[df["Combustível"] == "Etanol"]
    if not etanol.empty:
        col2.metric("Etanol (média)", f"R$ {etanol['Médio (R$/L)'].values[0]:.3f}")
    diesel = df[df["Combustível"] == "Diesel S10"]
    if not diesel.empty:
        col3.metric("Diesel S10 (média)", f"R$ {diesel['Médio (R$/L)'].values[0]:.3f}")

    st.divider()

    fig = px.bar(
        df.sort_values("Médio (R$/L)", ascending=False),
        x="Combustível",
        y="Médio (R$/L)",
        color="Combustível",
        error_y=df.sort_values("Médio (R$/L)", ascending=False)["Máximo (R$/L)"]
        - df.sort_values("Médio (R$/L)", ascending=False)["Médio (R$/L)"],
        error_y_minus=df.sort_values("Médio (R$/L)", ascending=False)["Médio (R$/L)"]
        - df.sort_values("Médio (R$/L)", ascending=False)["Mínimo (R$/L)"],
        title="Preço médio por combustível (com faixa mín/máx)",
    )
    fig.update_layout(showlegend=False)
    st.plotly_chart(fig, use_container_width=True)
    st.dataframe(df, use_container_width=True, hide_index=True)


def page_rankings(r: Redis) -> None:
    """Renderiza o painel de ranking de menores preços por combustível e estado.

    Lê ``ranking:preco:{combustivel}:{uf}`` (ZSET) e exibe os N postos mais
    baratos em gráfico de barras horizontais com filtros interativos.

    Args:
        r (Redis): Cliente Redis ativo.
    """
    st.header("Ranking de Preços por Combustível")

    col1, col2, col3 = st.columns(3)
    with col1:
        comb = st.selectbox("Combustível", COMBUSTIVEIS)
    with col2:
        uf = st.selectbox("Estado (UF)", UFS)
    with col3:
        top_n = st.slider("Quantidade de postos", 5, 30, 10)

    key = f"ranking:preco:{comb}:{uf}"
    raw = r.zrange(key, 0, top_n - 1, withscores=True)

    if not raw:
        st.info(f"Sem dados para `{key}`. Verifique se o pipeline foi executado.")
        return

    rows = []
    for posto_id, preco in raw:
        info = posto_info(r, posto_id)
        rows.append(
            {
                "Posto": info.get("nome", posto_id)[:35],
                "Bandeira": info.get("bandeira", "-"),
                "Cidade": info.get("cidade", "-"),
                "Preço (R$/L)": round(preco, 3),
            }
        )

    df = pd.DataFrame(rows)
    df.index = df.index + 1

    fig = px.bar(
        df,
        x="Preço (R$/L)",
        y="Posto",
        orientation="h",
        color="Bandeira",
        title=f"Top {top_n} menores preços — {comb.replace('_', ' ')} em {uf}",
    )
    fig.update_layout(yaxis={"categoryorder": "total ascending"})
    st.plotly_chart(fig, use_container_width=True)
    st.dataframe(
        df[["Posto", "Bandeira", "Cidade", "Preço (R$/L)"]], use_container_width=True
    )


def page_buscas(r: Redis) -> None:
    """Renderiza o painel de volume de buscas por cidade.

    Lê ``ranking:buscas`` (ZSET) e exibe as cidades com maior demanda
    em gráfico de barras horizontais coloridas por UF.

    Args:
        r (Redis): Cliente Redis ativo.
    """
    st.header("Cidades com Maior Volume de Buscas")

    top_n = st.slider("Quantidade de cidades", 5, 30, 15)
    raw = r.zrevrange("ranking:buscas", 0, top_n - 1, withscores=True)

    if not raw:
        st.info("Sem dados em `ranking:buscas`. Execute o pipeline primeiro.")
        return

    rows = []
    for member, score in raw:
        parts = member.split("|")
        cidade = parts[0] if parts else member
        uf = parts[1] if len(parts) > 1 else "-"
        rows.append({"Cidade": cidade, "UF": uf, "Buscas": int(score)})

    df = pd.DataFrame(rows)

    col1, col2 = st.columns([2, 1])
    with col1:
        fig = px.bar(
            df,
            x="Buscas",
            y="Cidade",
            orientation="h",
            color="UF",
            title=f"Top {top_n} cidades por volume de buscas",
        )
        fig.update_layout(yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig, use_container_width=True)
    with col2:
        st.metric("Total de cidades monitoradas", r.zcard("ranking:buscas"))
        st.metric("Buscas na cidade #1", int(raw[0][1]))
        st.dataframe(df, use_container_width=True, hide_index=True)


def page_variacao(r: Redis) -> None:
    """Renderiza o painel de postos com maior variação absoluta de preço.

    Lê ``ranking:variacao:{combustivel}`` (ZSET, ordem decrescente) e exibe
    os postos com maior oscilação para o combustível selecionado.

    Args:
        r (Redis): Cliente Redis ativo.
    """
    st.header("Postos com Maior Variação de Preço")

    col1, col2 = st.columns(2)
    with col1:
        comb = st.selectbox("Combustível", COMBUSTIVEIS)
    with col2:
        top_n = st.slider("Quantidade de postos", 5, 20, 10)

    key = f"ranking:variacao:{comb}"
    raw = r.zrevrange(key, 0, top_n - 1, withscores=True)

    if not raw:
        st.info(f"Sem dados para `{key}`.")
        return

    rows = []
    for posto_id, variacao_abs in raw:
        info = posto_info(r, posto_id)
        rows.append(
            {
                "Posto": info.get("nome", posto_id)[:35],
                "Bandeira": info.get("bandeira", "-"),
                "Cidade": info.get("cidade", "-"),
                "UF": info.get("uf", "-"),
                "Variação |%|": round(variacao_abs, 2),
            }
        )

    df = pd.DataFrame(rows)

    fig = px.bar(
        df,
        x="Variação |%|",
        y="Posto",
        orientation="h",
        color="Bandeira",
        title=f"Top {top_n} maior variação de preço — {comb.replace('_', ' ')}",
    )
    fig.update_layout(yaxis={"categoryorder": "total ascending"})
    st.plotly_chart(fig, use_container_width=True)
    st.dataframe(df, use_container_width=True, hide_index=True)


def page_timeseries(r: Redis) -> None:
    """Renderiza o painel de evolução temporal do preço médio diário.

    Executa ``TS.RANGE`` em ``ts:preco_avg:{combustivel}:{uf}`` e plota a série
    histórica em gráfico de linha com métricas de variação no período.

    Args:
        r (Redis): Cliente Redis ativo.
    """
    st.header("Evolução do Preço Médio ao Longo do Tempo")

    col1, col2 = st.columns(2)
    with col1:
        comb = st.selectbox("Combustível", COMBUSTIVEIS)
    with col2:
        uf = st.selectbox("Estado", UFS)

    key = f"ts:preco_avg:{comb}:{uf}"

    try:
        raw = r.execute_command("TS.RANGE", key, "-", "+")
    except Exception as exc:
        st.error(f"Erro ao consultar TimeSeries `{key}`: {exc}")
        return

    if not raw:
        st.info(f"Sem dados de série temporal para `{key}`.")
        return

    df = pd.DataFrame(raw, columns=["ts_ms", "preco_avg"])
    df["preco_avg"] = df["preco_avg"].astype(float).round(3)
    df["Data"] = df["ts_ms"].apply(lambda v: datetime.fromtimestamp(int(v) / 1000))

    variacao_total = df["preco_avg"].iloc[-1] - df["preco_avg"].iloc[0]
    col_a, col_b, col_c = st.columns(3)
    col_a.metric("Pontos na série", len(df))
    col_b.metric("Preço mais recente", f"R$ {df['preco_avg'].iloc[-1]:.3f}")
    col_c.metric("Variação no período", f"{variacao_total:+.3f} R$/L")

    fig = px.line(
        df,
        x="Data",
        y="preco_avg",
        markers=True,
        title=f"Preço médio diário — {comb.replace('_', ' ')} | {uf}",
        labels={"preco_avg": "Preço Médio (R$/L)", "Data": ""},
    )
    fig.update_traces(line_color="#FF6B35")
    st.plotly_chart(fig, use_container_width=True)
    st.dataframe(
        df[["Data", "preco_avg"]]
        .rename(columns={"preco_avg": "Preço Médio (R$/L)"})
        .tail(30),
        use_container_width=True,
        hide_index=True,
    )


def page_geo(r: Redis) -> None:
    """Renderiza o painel de busca geográfica de postos por raio.

    Executa ``GEOSEARCH`` em ``geo:postos`` a partir de uma cidade de referência
    e exibe os resultados em mapa nativo do Streamlit e tabela com distância em km.

    Args:
        r (Redis): Cliente Redis ativo.
    """
    st.header("Busca Geográfica de Postos (Redis GEO)")

    col1, col2 = st.columns(2)
    with col1:
        cidade_ref = st.selectbox(
            "Cidade de referência", list(CIDADES_REFERENCIA.keys())
        )
        lng, lat = CIDADES_REFERENCIA[cidade_ref]
    with col2:
        raio_km = st.slider("Raio de busca (km)", 5, 200, 50)
        top_n = st.number_input("Máximo de postos", 10, 100, 30)

    try:
        resultados = r.geosearch(
            "geo:postos",
            longitude=lng,
            latitude=lat,
            radius=raio_km,
            unit="km",
            sort="ASC",
            count=int(top_n),
            withcoord=True,
            withdist=True,
        )
    except Exception as exc:
        st.error(f"Erro na consulta GEO: {exc}")
        return

    if not resultados:
        st.info(f"Nenhum posto encontrado em {raio_km} km de {cidade_ref}.")
        return

    rows = []
    for item in resultados:
        pid = item[0]
        distancia = round(float(item[1]), 2)
        coord = item[2]
        info = posto_info(r, pid)
        rows.append(
            {
                "Posto": info.get("nome", pid)[:35],
                "Bandeira": info.get("bandeira", "-"),
                "Cidade": info.get("cidade", "-"),
                "UF": info.get("uf", "-"),
                "Distância (km)": distancia,
                "lat": float(coord[1]),
                "lon": float(coord[0]),
            }
        )

    df = pd.DataFrame(rows)
    st.metric(f"Postos encontrados em {raio_km} km de {cidade_ref}", len(df))

    st.map(df[["lat", "lon"]], zoom=9)
    st.dataframe(
        df[["Posto", "Bandeira", "Cidade", "UF", "Distância (km)"]],
        use_container_width=True,
        hide_index=True,
    )


# ---------------------------------------------------------------------------
# Layout principal
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Radar Combustível",
    page_icon="⛽",
    layout="wide",
)

st.title("⛽ Radar Combustível — Dashboard")
st.caption(
    "Dados servidos pela camada **Redis**, alimentados via pipeline **MongoDB → Redis**."
)

auto_refresh = st.sidebar.toggle("Auto-refresh", value=True)
refresh_s = st.sidebar.number_input("Intervalo (s)", min_value=3, max_value=60, value=5)

page = st.sidebar.radio(
    "Painel",
    [
        "📊 Resumo Global",
        "⛽ Rankings de Preço",
        "🔍 Volume de Buscas",
        "📈 Variação de Preço",
        "🕐 Série Temporal",
        "🗺️ Busca Geográfica",
    ],
)

try:
    r = get_redis()
    r.ping()
except Exception as exc:
    st.error(f"Não foi possível conectar ao Redis ({REDIS_HOST}:{REDIS_PORT}): {exc}")
    st.stop()

# run_every=None desativa o auto-refresh sem alterar a estrutura do fragmento.
run_every = int(refresh_s) if auto_refresh else None

PAGE_MAP = {
    "📊 Resumo Global": page_resumo,
    "⛽ Rankings de Preço": page_rankings,
    "🔍 Volume de Buscas": page_buscas,
    "📈 Variação de Preço": page_variacao,
    "🕐 Série Temporal": page_timeseries,
    "🗺️ Busca Geográfica": page_geo,
}

if page in PAGE_MAP:
    st.fragment(run_every=run_every)(PAGE_MAP[page])(r)
