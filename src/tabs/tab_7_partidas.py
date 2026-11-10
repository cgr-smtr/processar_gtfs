import streamlit as st
import os
from src.runner import run_script


def _script_path(nome):
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "codigos_py", nome))


def render():
    st.header("7. Lista Partidas")
    st.markdown("Lista e combina partidas por linha e tipo de dia.")

    modo = st.radio(
        "Selecione o processamento",
        ["7", "7.1", "7.2"],
        key="t7_modo",
        horizontal=True,
    )

    base_dados = st.session_state.BASE_DADOS
    base_resultados_default = base_dados.replace("dados", "resultados")

    if modo == "7":
        st.subheader("Script 7 — Lista de partidas")
        st.markdown("Lista as partidas por linha baseando-se nos horários da primeira parada (stop_times).")

        default_gtfs = f"{base_dados}/gtfs/2026/gtfs_combi_2026-10-02Q_RIO.zip"

        col1, col2 = st.columns(2)
        with col1:
            endereco_gtfs = st.text_input(
                "GTFS Entrada (endereco_gtfs)",
                value=default_gtfs,
                key="t7_endereco_gtfs",
            )
            base_resultados = st.text_input(
                "Pasta de Resultados (BASE_RESULTADOS)",
                value=base_resultados_default,
                key="t7_base_resultados",
            )
        with col2:
            tipos_dia = st.multiselect(
                "Tipos de Dia (tipos_dia)",
                ["du", "sab", "dom", "pf"],
                default=["du", "sab", "dom", "pf"],
                help="Quais dias processar (pf = ponto facultativo, service_id F_REG)",
                key="t7_tipos_dia",
            )
            linhas_excluir = st.text_area(
                "Linhas a Excluir (linhas_excluir)",
                value="",
                help="Insira os trip_short_name separados por vírgula (ou um por linha)",
                key="t7_linhas_excluir",
            )

        if st.button("▶ Executar Script 7", key="btn_script_7"):
            linhas = [x.strip() for x in linhas_excluir.split(',') if x.strip()] if ',' in linhas_excluir else [x.strip() for x in linhas_excluir.split('\n') if x.strip()]

            config = {
                "BASE_DADOS": st.session_state.BASE_DADOS,
                "BASE_RESULTADOS": base_resultados,
                "ano_gtfs": st.session_state.ano_gtfs,
                "endereco_gtfs": endereco_gtfs,
                "tipos_dia": tipos_dia,
                "linhas_excluir": linhas,
            }
            run_script(_script_path("7_lista_partidas.py"), config)

    elif modo == "7.1":
        st.subheader("Script 7.1 — Resumo OS domingo + dia útil")
        st.markdown("Gera a tabela resumo de domingo importando o bloco de dia útil dentro da janela informada.")

        col1, col2 = st.columns(2)
        with col1:
            caminho_entrada = st.text_input(
                "Arquivo de Partidas (caminho_entrada)",
                value=r"C:\R_SMTR\resultados\OS_historico\2026_SET_1ºESTUDO_RETIFICAÇÃO\partidas\partidas_2026-09-01Q.parquet",
                help="Aceita .parquet ou .csv.",
                key="t7_caminho_entrada_71",
            )
            pasta_saida = st.text_input(
                "Pasta de Saída (pasta_saida)",
                value=r"C:\R_SMTR\resultados\OS_historico\2026_SET_1ºESTUDO_RETIFICAÇÃO\arquivos_os",
                key="t7_pasta_saida_71",
            )
        with col2:
            nome_base_saida = st.text_input(
                "Nome Base de Saída (nome_base_saida)",
                value="tabela_domingo_ajustada",
                help="Gera <nome>.xlsx e <nome>.csv.",
                key="t7_nome_base_saida_71",
            )
            hora_inicio = st.text_input(
                "Hora de Início (hora_inicio)",
                value="06:00:00",
                key="t7_hora_inicio_71",
            )
            hora_fim = st.text_input(
                "Hora de Fim (hora_fim, exclusivo)",
                value="20:00:00",
                key="t7_hora_fim_71",
            )

        if st.button("▶ Executar Script 7.1", key="btn_script_71"):
            config = {
                "caminho_entrada": caminho_entrada,
                "pasta_saida": pasta_saida,
                "nome_base_saida": nome_base_saida,
                "hora_inicio": hora_inicio,
                "hora_fim": hora_fim,
            }
            run_script(_script_path("7.1_resumo_os_domingo_du.py"), config)

    elif modo == "7.2":
        st.subheader("Script 7.2 — Combinador de partidas")
        st.markdown("Combina as partidas de domingo com o bloco de dia útil entre 06:00 e 20:00.")

        col1, col2 = st.columns(2)
        with col1:
            caminho_du = st.text_input(
                "Partidas de Dia Útil (caminho_du)",
                value="quadros_du.csv",
                key="t7_caminho_du_72",
            )
            caminho_dom = st.text_input(
                "Partidas de Domingo (caminho_dom)",
                value="quadros_dom.csv",
                key="t7_caminho_dom_72",
            )
        with col2:
            caminho_saida = st.text_input(
                "Arquivo de Saída (caminho_saida)",
                value="quadros_dom_04-10.csv",
                key="t7_caminho_saida_72",
            )

        if st.button("▶ Executar Script 7.2", key="btn_script_72"):
            config = {
                "caminho_du": caminho_du,
                "caminho_dom": caminho_dom,
                "caminho_saida": caminho_saida,
            }
            run_script(_script_path("7.2_combinador_partidas.py"), config)
