"""App Streamlit para formatar/analisar quadros horários.

Versão Python do Shiny ``1.2 app_format_qh.R``. Execute com:

    python -m streamlit run src/app_format_qh.py
"""

from __future__ import annotations

import hashlib

import pandas as pd
import streamlit as st

from src.qh_format import (
    bands_from_departures,
    comparison_table,
    filter_trips,
    get_departure_seconds,
    get_time_color,
    load_gtfs_zip,
    parse_partidas,
    qh_dataframe,
    route_choices,
    route_id_from_label,
    sanitize_filename,
    schedule_display,
    summary_table,
    trip_short_name_from_label,
    unique_nonempty,
)


@st.cache_data(show_spinner="Lendo GTFS...")
def _carregar_gtfs(conteudo: bytes) -> dict[str, pd.DataFrame]:
    return load_gtfs_zip(conteudo)


def _formatar_intervalo(valor, casas: int):
    if isinstance(valor, str):
        return valor
    if pd.isna(valor):
        return "-"
    return f"{float(valor):.{casas}f}"


def _estilo_hora(hora) -> str:
    return f"background-color: {get_time_color(hora)}; color: #1F1F1F;"


def _estilizar_programacao(df: pd.DataFrame, coluna_hora: str = "_hora"):
    visao = df.drop(columns=[c for c in df.columns if c.startswith("_")]).copy()
    if visao.empty:
        return visao
    if "Intervalo" in visao.columns:
        visao["Intervalo"] = visao["Intervalo"].map(lambda v: _formatar_intervalo(v, 2))

    def _cores(serie: pd.Series):
        horas = df.loc[serie.index, coluna_hora]
        return [_estilo_hora(h) for h in horas]

    return visao.style.apply(_cores, subset=["Horário"])


def _estilizar_comparacao(df: pd.DataFrame):
    visao = df.drop(columns=[c for c in df.columns if c.startswith("_")]).copy()
    if visao.empty:
        return visao
    for coluna in ("Intervalo GTFS", "Intervalo Usuário"):
        if coluna in visao.columns:
            visao[coluna] = visao[coluna].map(lambda v: _formatar_intervalo(v, 1))

    def _cores_gtfs(serie: pd.Series):
        horas = df.loc[serie.index, "_hora_gtfs"]
        return [_estilo_hora(h) for h in horas]

    def _cores_usuario(serie: pd.Series):
        horas = df.loc[serie.index, "_hora_user"]
        return [_estilo_hora(h) for h in horas]

    return (
        visao.style
        .apply(_cores_gtfs, subset=["Horário GTFS"])
        .apply(_cores_usuario, subset=["Horário Usuário"])
    )


def _estilizar_resumo(df: pd.DataFrame):
    visao = df.drop(columns=[c for c in df.columns if c.startswith("_")])
    if visao.empty:
        return visao

    def _cores(serie: pd.Series):
        horas = df.loc[serie.index, "_hora"]
        return [_estilo_hora(h) for h in horas]

    return visao.style.apply(_cores, subset=["Faixa"])


def _mostrar_tabela(df, estilizado, vazio: str):
    if df is None or df.empty:
        st.info(vazio)
    else:
        st.dataframe(estilizado, width="stretch", hide_index=True)


def main():
    st.set_page_config(
        page_title="Formatar QH - Análise da Operação",
        layout="wide",
    )

    st.title("Análise da Operação de Linhas de Ônibus")

    st.sidebar.header("Filtros")
    uploaded = st.sidebar.file_uploader(
        "Carregar arquivo GTFS (ZIP)",
        type=["zip"],
        key="fmt_gtfs_zip",
    )
    if uploaded is None:
        st.info("Carregue um arquivo GTFS (.zip) na barra lateral para começar.")
        st.stop()

    conteudo = uploaded.getvalue()
    chave = hashlib.md5(conteudo).hexdigest()

    try:
        gtfs = _carregar_gtfs(conteudo)
    except Exception as erro:
        st.error(f"Não foi possível ler o GTFS: {erro}")
        st.stop()

    routes = gtfs["routes"]
    trips = gtfs["trips"]

    opcoes_linha = route_choices(routes)
    if not opcoes_linha:
        st.sidebar.error("O GTFS não possui linhas válidas em routes.txt.")
        st.stop()

    linha = st.sidebar.selectbox(
        "Selecione uma linha:",
        opcoes_linha,
        key=f"fmt_route::{chave}",
    )

    route_id = route_id_from_label(linha)
    trips_linha = filter_trips(trips, route_id)

    sentidos = unique_nonempty(trips_linha.get("direction_id", []))
    if not sentidos:
        st.sidebar.warning("Esta linha não possui sentidos em trips.txt.")
        st.stop()
    direction_id = st.sidebar.selectbox(
        "Selecione o sentido:",
        sentidos,
        key=f"fmt_direction::{chave}::{linha}",
    )

    trips_sentido = filter_trips(trips, route_id, direction_id=direction_id)
    servicos = unique_nonempty(trips_sentido.get("service_id", []))
    if not servicos:
        st.sidebar.warning("Este sentido não possui calendários em trips.txt.")
        st.stop()
    service_id = st.sidebar.selectbox(
        "Selecione um calendário:",
        servicos,
        key=f"fmt_service::{chave}::{linha}::{direction_id}",
    )

    headsigns = unique_nonempty(trips_sentido.get("trip_headsign", []))
    if not headsigns:
        st.sidebar.warning("Este sentido não possui destinos em trips.txt.")
        st.stop()
    trip_headsign = st.sidebar.selectbox(
        "Selecione o destino:",
        headsigns,
        key=f"fmt_headsign::{chave}::{linha}::{direction_id}",
    )

    trips_selecionadas = filter_trips(
        trips,
        route_id,
        direction_id=direction_id,
        service_id=service_id,
        trip_headsign=trip_headsign,
    )
    gtfs_segundos = get_departure_seconds(gtfs, trips_selecionadas)

    st.subheader("trip_headsign")
    st.code(trip_headsign, language=None)

    col_programacao, col_gtfs, col_proposta, col_comparacao = st.columns(4)

    with col_programacao:
        st.markdown("### Colar a Programação Prevista")
        st.caption("Um horário por linha, no formato HH:MM:SS.")
        if st.button("Limpar", key="fmt_clear"):
            st.session_state["fmt_partidas"] = ""
            st.rerun()
        partidas_texto = st.text_area(
            "Programação prevista",
            key="fmt_partidas",
            height=620,
            label_visibility="collapsed",
            placeholder="Exemplo:\n03:40:00\n04:00:00\n04:20:00\n...",
        )

    tabela_gtfs = schedule_display(gtfs_segundos)
    with col_gtfs:
        st.markdown("### Tabela do GTFS")
        if not gtfs_segundos:
            st.warning("GTFS sem frequencies para esta linha/sentido/calendário/destino.")
        else:
            _mostrar_tabela(
                tabela_gtfs,
                _estilizar_programacao(tabela_gtfs),
                "Nenhuma partida encontrada no GTFS.",
            )

    erro_partidas = None
    user_segundos: list[int] = []
    if partidas_texto.strip():
        try:
            user_segundos = parse_partidas(partidas_texto)
        except ValueError as erro:
            erro_partidas = str(erro)
    else:
        erro_partidas = "Nenhum horário informado."

    tabela_proposta = schedule_display(user_segundos) if user_segundos else pd.DataFrame()
    with col_proposta:
        st.markdown("### Tabela Proposta")
        if erro_partidas:
            st.error(erro_partidas)
        else:
            _mostrar_tabela(
                tabela_proposta,
                _estilizar_programacao(tabela_proposta),
                "Nenhuma partida proposta.",
            )

    comparacao = pd.DataFrame()
    if gtfs_segundos and user_segundos:
        comparacao = comparison_table(gtfs_segundos, user_segundos)

    with col_comparacao:
        st.markdown("### Comparação")
        if not gtfs_segundos:
            st.info("A comparação exige partidas no GTFS.")
        elif not user_segundos:
            st.info("A comparação exige uma programação proposta válida.")
        else:
            _mostrar_tabela(
                comparacao,
                _estilizar_comparacao(comparacao),
                "Nenhuma comparação disponível.",
            )

    st.divider()
    resumo = pd.DataFrame()
    if gtfs_segundos and user_segundos:
        resumo = summary_table(gtfs_segundos, user_segundos)

    st.markdown("### Partidas por Faixa Horária")
    if resumo.empty:
        st.info("Informe uma programação proposta válida e carregue um GTFS com frequencies.")
    else:
        st.dataframe(_estilizar_resumo(resumo), width="stretch", hide_index=True)

    if user_segundos:
        try:
            bandas = bands_from_departures(user_segundos)
        except ValueError as erro:
            st.sidebar.warning(str(erro))
        else:
            qh = qh_dataframe(
                bandas,
                trip_short_name=trip_short_name_from_label(linha),
                trip_headsign=trip_headsign,
            )
            nome_arquivo = (
                f"horarios_{sanitize_filename(trip_short_name_from_label(linha))}"
                f"_{sanitize_filename(trip_headsign)}"
                f"_{sanitize_filename(service_id)}.csv"
            )
            st.sidebar.download_button(
                "Baixar QH em CSV",
                data=qh.to_csv(index=False).encode("utf-8"),
                file_name=nome_arquivo,
                mime="text/csv",
                key="fmt_download",
            )
    else:
        st.sidebar.caption("Informe uma programação válida para liberar o download do QH.")


if __name__ == "__main__":
    main()
