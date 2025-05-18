import os
import json
import streamlit as st
import duckdb
import pandas as pd
from datetime import datetime
import plotly.express as px
from typing import List
from tmdb_utils import buscar_filme_por_titulo, detalhes_completos_filme

st.set_page_config(page_title="📊 Dashboard ANCINE", layout="wide")

@st.cache_resource
def get_connection():
    return duckdb.connect(
        database=os.path.join("ancine_data", "ancine.duckdb"),
        read_only=True
    )

@st.cache_data
def run_query(sql: str) -> pd.DataFrame:
    return get_connection().execute(sql).df()

@st.cache_data
def get_titles() -> List[str]:
    cache_file = "titles_cache.csv"
    try:
        df = pd.read_csv(cache_file)
        return df['TITULO_BRASIL'].tolist()
    except FileNotFoundError:
        df = run_query(
            "SELECT DISTINCT TITULO_BRASIL "
            "FROM read_parquet('ancine_data/*.parquet') "
            "ORDER BY TITULO_BRASIL"
        )
        try:
            df.to_csv(cache_file, index=False)
        except Exception:
            pass
        return df['TITULO_BRASIL'].tolist()

st.sidebar.title("Filtros")
titles = get_titles()
selection = st.sidebar.selectbox(
    "🔍 Buscar (título):",
    options=["(limpar pesquisa)"] + titles,
    index=0,
    help="Comece a digitar para autocompletar"
)
query = "" if selection == "(limpar pesquisa)" else selection
BASE = "read_parquet('ancine_data/*.parquet')"

data_bounds = run_query(
    f"SELECT MIN(DT_INICIO_EXIBICAO)::DATE AS min_date, "
    f"MAX(DT_INICIO_EXIBICAO)::DATE AS max_date "
    f"FROM {BASE}"
).iloc[0]
min_date = data_bounds['min_date'].date() if hasattr(data_bounds['min_date'], 'date') else data_bounds['min_date']
max_date = data_bounds['max_date'].date() if hasattr(data_bounds['max_date'], 'date') else data_bounds['max_date']
today = datetime.today().date()
default_end = min(today, max_date)
default_start = default_end.replace(day=1)

date_range = st.sidebar.date_input(
    "📅 Período:",
    value=(default_start, default_end),
    min_value=min_date,
    max_value=max_date,
    format="DD/MM/YYYY",
    key="date"
)
if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
    start_dt, end_dt = date_range
else:
    start_dt = end_dt = date_range if not isinstance(date_range, (list, tuple)) else date_range[0]
start_date = start_dt.strftime("%Y-%m-%d")
end_date = end_dt.strftime("%Y-%m-%d")

states_df = run_query(
    f"SELECT DISTINCT UF_SALA_COMPLEXO AS estado FROM {BASE} ORDER BY estado"
)
estados = st.sidebar.multiselect("📍 Estado:", options=states_df['estado'].tolist())

if estados:
    state_list = ",".join(f"'{s}'" for s in estados)
    cities_df = run_query(
        f"SELECT DISTINCT MUNICIPIO_SALA_COMPLEXO AS municipio FROM {BASE} "
        f"WHERE UF_SALA_COMPLEXO IN ({state_list}) ORDER BY municipio"
    )
    cidades_options = cities_df['municipio'].tolist()
else:
    cidades_options = []
cidades = st.sidebar.multiselect("🏘️ Município:", options=cidades_options)

conds = [
    f"DT_INICIO_EXIBICAO::DATE BETWEEN '{start_date}' AND '{end_date}'"
]
if query:
    sq = query.replace("'", "''")
    conds.append(f"TITULO_BRASIL = '{sq}'")
if estados:
    conds.append(f"UF_SALA_COMPLEXO IN ({state_list})")
if cidades:
    c_list = ",".join(f"'{c}'" for c in cidades)
    conds.append(f"MUNICIPIO_SALA_COMPLEXO IN ({c_list})")
where = "WHERE " + " AND ".join(conds)

if query:
    sq = query.replace("'", "''")
    fb = run_query(
        f"SELECT MIN(DT_INICIO_EXIBICAO)::DATE AS min_date, "
        f"MAX(DT_INICIO_EXIBICAO)::DATE AS max_date "
        f"FROM {BASE} WHERE TITULO_BRASIL = '{sq}'"
    ).iloc[0]
    st.sidebar.caption(
        f"⏰ Período de exibição: {fb['min_date']:%d/%m/%Y} - {fb['max_date']:%d/%m/%Y}"
    )

if query:
    start_date = fb['min_date'].strftime("%Y-%m-%d")
    end_date = fb['max_date'].strftime("%Y-%m-%d")
    tmdb_results = buscar_filme_por_titulo(query)
    if tmdb_results:
        mv = tmdb_results[0]
        movie_id = mv['id'] if isinstance(mv, dict) else getattr(mv, 'id', None)
        details = detalhes_completos_filme(movie_id) or {}
        poster = details.get('poster_path')
        cols = st.columns([1, 3])
        with cols[0]:
            if poster:
                st.image(f"https://image.tmdb.org/t/p/w300{poster}")
            else:
                st.write("Sem pôster TMDB")
        with cols[1]:
            title = details.get('title', query)
            year = details.get('release_date', '')[:4]
            st.markdown(f"### {title} ({year})")
            st.write(details.get('overview', 'Sem sinopse disponível.'))
            for k, v in {
                'Nota Média': details.get('vote_average', 'N/A'),
                'Votos': details.get('vote_count', 'N/A'),
                'Duração': f"{details.get('runtime', 0)} min"
            }.items():
                st.write(f"**{k}:** {v}")
    df_film = run_query(
        f"SELECT DT_INICIO_EXIBICAO::DATE AS data, "
        f"SUM(PUBLICO::BIGINT) AS publico, COUNT(*) AS sessoes FROM {BASE} "
        + where + " GROUP BY data ORDER BY data"
    )
    df_film['data'] = pd.to_datetime(df_film['data']).dt.strftime('%d/%m/%Y')
    st.subheader("Evolução de Público por Data")
    if not df_film.empty:
        fig = px.line(df_film, x='data', y='publico', markers=True)
        fig.update_xaxes(showticklabels=False)
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(df_film)
        st.subheader("🌍 Distribuição por Estado e Município")
        cs, cc = st.columns(2)
        df_s = run_query(
            f"SELECT UF_SALA_COMPLEXO AS estado, SUM(PUBLICO::BIGINT) AS publico FROM {BASE} " + where +
            " GROUP BY estado ORDER BY publico DESC LIMIT 7"
        )
        with cs:
            fig_s = px.pie(df_s, names='estado', values='publico', hole=0.4)
            fig_s.update_traces(textinfo='percent')
            st.plotly_chart(fig_s, use_container_width=True)
        df_c = run_query(
            f"SELECT MUNICIPIO_SALA_COMPLEXO AS municipio, SUM(PUBLICO::BIGINT) AS publico FROM {BASE} " + where +
            " GROUP BY municipio ORDER BY publico DESC LIMIT 7"
        )
        with cc:
            fig_c = px.pie(df_c, names='municipio', values='publico', hole=0.4)
            fig_c.update_traces(textinfo='percent')
            st.plotly_chart(fig_c, use_container_width=True)
    else:
        st.write("Nenhuma exibição encontrada para este filme no período.")
    st.stop()

st.title("📽️ Painel de Bilheteria - ANCINE")
st.markdown("---")
c1, c2 = st.columns(2)
with c1:
    st.subheader("💰 Top 10 Bilheteria")
    df1 = run_query(
        f"SELECT TITULO_BRASIL AS filme, SUM(PUBLICO::BIGINT) AS bilheteria FROM {BASE} " + where +
        " GROUP BY filme ORDER BY bilheteria DESC LIMIT 10"
    )
    fig1 = px.bar(df1, x='filme', y='bilheteria', text_auto=True)
    fig1.update_layout(yaxis_tickformat=',.0f')
    st.plotly_chart(fig1, use_container_width=True)
with c2:
    st.subheader("🎟️ Top 10 Sessões")
    df2 = run_query(
        f"SELECT TITULO_BRASIL AS filme, COUNT(*) AS sessoes FROM {BASE} " + where +
        " GROUP BY filme ORDER BY sessoes DESC LIMIT 10"
    )
    fig2 = px.bar(df2, x='filme', y='sessoes', text_auto=True)
    st.plotly_chart(fig2, use_container_width=True)

c3, c4 = st.columns(2)
with c3:
    st.subheader("📍 Público por Estado")
    df_e = run_query(
        f"SELECT UF_SALA_COMPLEXO AS uf, SUM(PUBLICO::BIGINT) AS publico FROM {BASE} " + where +
        " GROUP BY uf"
    )
    with open('brazil_states.geojson', 'r', encoding='utf-8') as f:
        gj = json.load(f)
    fig3 = px.choropleth(df_e, geojson=gj, featureidkey='properties.sigla',
                         locations='uf', color='publico',
                         range_color=(df_e['publico'].min(), df_e['publico'].quantile(0.9)))
    fig3.update_geos(fitbounds='locations', visible=False)
    st.plotly_chart(fig3, use_container_width=True)
with c4:
    st.subheader("📋 Média por Sessão (Top 10)")
    df_m = run_query(
        f"SELECT TITULO_BRASIL AS filme, SUM(PUBLICO::BIGINT)/COUNT(*) AS media_sessao FROM {BASE} " + where +
        " GROUP BY filme ORDER BY media_sessao DESC LIMIT 10"
    )
    df_m['media_sessao'] = df_m['media_sessao'].round(2)
    fig4 = px.bar(df_m, x='filme', y='media_sessao', text_auto=True)
    st.plotly_chart(fig4, use_container_width=True)

df_time = run_query(
    f"SELECT DT_INICIO_EXIBICAO::DATE AS date, SUM(PUBLICO::BIGINT) AS publico_diario FROM {BASE} " + where +
    " GROUP BY date ORDER BY date"
)
df_time['mov_avg7'] = df_time['publico_diario'].rolling(7).mean()
c5, c6 = st.columns(2)
with c5:
    st.subheader("📅 Evolução Diária de Público")
    fig5 = px.line(df_time, x='date', y='publico_diario')
    fig5.update_layout(xaxis_tickformat='%d/%m/%Y')
    st.plotly_chart(fig5, use_container_width=True)
with c6:
    st.subheader("📈 Média Móvel 7 dias")
    fig6 = px.line(df_time, x='date', y='mov_avg7')
    fig6.update_layout(xaxis_tickformat='%d/%m/%Y')
    valid = df_time['mov_avg7'].dropna()
    if not valid.empty:
        low = valid.min(); high = valid.max()
        li, hi = valid.idxmin(), valid.idxmax()
        ld, hd = df_time.loc[li, 'date'], df_time.loc[hi, 'date']
        fig6.add_scatter(x=[ld], y=[low], mode='markers+text', text=[f'Mín {low:.0f}'], textposition='bottom right')
        fig6.add_scatter(x=[hd], y=[high], mode='markers+text', text=[f'Máx {high:.0f}'], textposition='top right')
    st.plotly_chart(fig6, use_container_width=True)

c7, c8 = st.columns(2)
with c7:
    st.subheader('📉 Top 5 Menos Vistos (Média/Sessão)')
    df_l = run_query(
        f"SELECT TITULO_BRASIL AS filme, COUNT(*) AS sessoes, SUM(PUBLICO::BIGINT)/COUNT(*) AS media_sessao FROM {BASE} " + where +
        " GROUP BY filme ORDER BY media_sessao ASC LIMIT 5"
    )
    fig7 = px.scatter(df_l, x='sessoes', y='media_sessao', text='filme')
    fig7.update_traces(textposition='top center')
    st.plotly_chart(fig7, use_container_width=True)
with c8:
    st.subheader('🌆 Top 5 Municípios por Público')
    df_c5 = run_query(
        f"SELECT MUNICIPIO_SALA_COMPLEXO AS municipio, SUM(PUBLICO::BIGINT) AS total FROM {BASE} " + where +
        " GROUP BY municipio ORDER BY total DESC LIMIT 5"
    )
    fig8 = px.pie(df_c5, names='municipio', values='total', hole=0.4)
    fig8.update_traces(textposition='inside', textinfo='percent+label')
    st.plotly_chart(fig8, use_container_width=True)