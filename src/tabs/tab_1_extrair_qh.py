import streamlit as st
from pathlib import Path
import os
from src.runner import run_script


def _script_path(nome):
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "codigos_py", nome))


def _opcional(config, chave, valor):
    valor = str(valor).strip()
    if valor:
        config[chave] = valor


def render():
    st.header("1. Extrair QH")
    st.markdown("Extrai e combina quadros de horários a partir do GTFS ou de tabelas de partidas.")

    modo = st.radio(
        "Selecione o processamento",
        ["1", "1.1", "1.2", "1.3", "1.3.1", "1.4"],
        key="t1_modo",
        horizontal=True,
    )

    base_dados = st.session_state.BASE_DADOS
    base_resultados_default = base_dados.replace("dados", "resultados")
    ano = st.session_state.ano_gtfs
    mes = st.session_state.mes_gtfs
    estudo = st.session_state.estudo_gtfs
    default_gtfs = f"{base_dados}/gtfs/{ano}/gtfs_combi_{ano}-{mes}-{estudo}Q.zip"

    if modo == "1":
        st.subheader("Script 1 — QH por faixas de frequência")
        st.markdown("Extrai quadros de horários para linhas e serviços específicos a partir do GTFS Combinado.")

        col1, col2 = st.columns(2)
        with col1:
            end_gtfs = st.text_input(
                "Caminho GTFS Combinado (end_gtfs)",
                value=default_gtfs,
                help="Caminho do GTFS combi",
                key="t1_end_gtfs",
            )
            base_resultados = st.text_input(
                "Pasta de Resultados (BASE_RESULTADOS)",
                value=base_resultados_default,
                help="Pasta base para a saída (será subpasta quadro_horario_extraido/...)",
                key="t1_base_resultados",
            )
        with col2:
            linhas_str = st.text_input(
                "Linhas a Rodar (separadas por vírgula)",
                value="371, 624, SN624, SV624",
                help="Lista de 'trip_short_name' para extrair. Ex: 371, 624",
                key="t1_linhas_str",
            )
            servicos = st.multiselect(
                "Calendários Alvo (services_to_run)",
                ["U_REG", "S_REG", "D_REG", "EXCEP", "U", "S", "D"],
                default=["U_REG", "S_REG", "D_REG"],
                help="Selecione os 'service_id' a serem extraídos",
                key="t1_servicos",
            )

        if st.button("▶ Executar Script 1", key="btn_script_1"):
            config = {
                "BASE_DADOS": st.session_state.BASE_DADOS,
                "BASE_RESULTADOS": base_resultados,
                "ano_gtfs": st.session_state.ano_gtfs,
                "mes_gtfs": st.session_state.mes_gtfs,
                "estudo_gtfs": st.session_state.estudo_gtfs,
                "end_gtfs": end_gtfs,
                "linhas_rodar": linhas_str,
                "services_to_run": servicos,
            }
            run_script(_script_path("1_extrair_qh_especificado_no_gtfs.py"), config)

    elif modo == "1.1":
        st.subheader("Script 1.1 — QH com partidas detalhadas")
        st.markdown("Extrai a grade de partidas detalhadas a partir das frequências e dos horários do GTFS.")

        col1, col2 = st.columns(2)
        with col1:
            end_gtfs = st.text_input(
                "Caminho GTFS Combinado (end_gtfs)",
                value=default_gtfs,
                key="t1_end_gtfs_11",
            )
            base_resultados = st.text_input(
                "Pasta de Resultados (BASE_RESULTADOS)",
                value=base_resultados_default,
                key="t1_base_resultados_11",
            )
            pasta_saida = st.text_input(
                "Pasta de Saída (pasta_saida)",
                value=f"{base_resultados_default}/quadro_horario_extraido/{ano}/{mes}/qh_partidas_por_linha/{estudo}Q",
                key="t1_pasta_saida_11",
            )
        with col2:
            linhas_str = st.text_input(
                "Linhas a Rodar (linhas_rodar)",
                value="804, 807, 809, 825, 840, 842, 849, 868, 870, 892, 898, SN825, SN840, SN898, 808, 821, 822, 833, 841, 869, 893, SN821, SN893",
                help="Separe por vírgula; use 'todas' para processar o GTFS inteiro.",
                key="t1_linhas_str_11",
            )
            servicos = st.multiselect(
                "Calendários Alvo (services_to_run)",
                ["U_REG", "S_REG", "D_REG", "EXCEP", "U", "S", "D"],
                default=["U_REG", "S_REG", "D_REG"],
                key="t1_servicos_11",
            )

        if st.button("▶ Executar Script 1.1", key="btn_script_11"):
            config = {
                "BASE_DADOS": st.session_state.BASE_DADOS,
                "BASE_RESULTADOS": base_resultados,
                "ano_gtfs": st.session_state.ano_gtfs,
                "mes_gtfs": st.session_state.mes_gtfs,
                "estudo_gtfs": st.session_state.estudo_gtfs,
                "end_gtfs": end_gtfs,
                "linhas_rodar": linhas_str,
                "services_to_run": servicos,
            }
            _opcional(config, "pasta_saida", pasta_saida)
            run_script(_script_path("1.1_extrair_qh_partidas.py"), config)

    elif modo == "1.2":
        st.subheader("Script 1.2 — Combinar quadros por horário")
        st.markdown("Combina faixas entre serviços/calendários, importando o bloco de horário do serviço de origem.")

        col1, col2 = st.columns(2)
        with col1:
            base_resultados = st.text_input(
                "Pasta de Resultados (BASE_RESULTADOS)",
                value=base_resultados_default,
                key="t1_base_resultados_12",
            )
            pasta_entrada = st.text_input(
                "Pasta de Entrada (pasta_entrada)",
                value=f"{base_resultados_default}/quadro_horario_extraido/{ano}/{mes}/qh_por_linha/{estudo}Q",
                key="t1_pasta_entrada_12",
            )
            pasta_saida = st.text_input(
                "Pasta de Saída (pasta_saida)",
                value=f"{base_resultados_default}/quadro_horario_extraido/{ano}/{mes}/qh_por_linha_combinado/{estudo}Q",
                key="t1_pasta_saida_12",
            )
        with col2:
            servico_origem = st.text_input(
                "Serviço de Origem (servico_origem)",
                value="U_REG",
                key="t1_servico_origem_12",
            )
            servico_destino = st.text_input(
                "Serviço de Destino (servico_destino)",
                value="D_REG",
                key="t1_servico_destino_12",
            )
            hora_inicio = st.text_input(
                "Hora de Início (hora_inicio)",
                value="06:00:00",
                key="t1_hora_inicio_12",
            )
            hora_fim = st.text_input(
                "Hora de Fim (hora_fim, exclusivo)",
                value="21:00:00",
                key="t1_hora_fim_12",
            )
            linhas_str = st.text_input(
                "Linhas a Rodar (linhas_rodar)",
                value="",
                help="Separe por vírgula; vazio processa todas as linhas encontradas.",
                key="t1_linhas_str_12",
            )

        if st.button("▶ Executar Script 1.2", key="btn_script_12"):
            config = {
                "BASE_DADOS": st.session_state.BASE_DADOS,
                "BASE_RESULTADOS": base_resultados,
                "ano_gtfs": st.session_state.ano_gtfs,
                "mes_gtfs": st.session_state.mes_gtfs,
                "estudo_gtfs": st.session_state.estudo_gtfs,
                "servico_origem": servico_origem,
                "servico_destino": servico_destino,
                "hora_inicio": hora_inicio,
                "hora_fim": hora_fim,
                "linhas_rodar": linhas_str,
            }
            _opcional(config, "pasta_entrada", pasta_entrada)
            _opcional(config, "pasta_saida", pasta_saida)
            run_script(_script_path("1.2_combinar_quadros_horarios.py"), config)

    elif modo == "1.3":
        st.subheader("Script 1.3 — QH a partir de partidas")
        st.markdown("Gera quadros factíveis no formato do Script 1 a partir de contagens por linha, sentido e faixa.")

        col1, col2 = st.columns(2)
        with col1:
            base_resultados = st.text_input(
                "Pasta de Resultados (BASE_RESULTADOS)",
                value=base_resultados_default,
                key="t1_base_resultados_13",
            )
            caminho_partidas = st.text_input(
                "Tabela de Contagens (caminho_partidas)",
                value="partidas_pf_sppo_como_du.csv",
                help="CSV no formato wide (uma coluna por faixa) ou longo (Faixa + Partidas).",
                key="t1_caminho_partidas_13",
            )
            pasta_saida = st.text_input(
                "Pasta de Saída (pasta_saida)",
                value=f"{base_resultados_default}/quadro_horario_extraido/{ano}/{mes}/qh_por_linha_partidas/{estudo}Q",
                key="t1_pasta_saida_13",
            )
            linhas_str = st.text_input(
                "Linhas a Rodar (linhas_rodar)",
                value="",
                help="Separe por vírgula; vazio processa todas as linhas.",
                key="t1_linhas_str_13",
            )
            service_id = st.text_input(
                "service_id",
                value="F_REG",
                key="t1_service_id_13",
            )
        with col2:
            granularidade_headway = st.number_input(
                "Granularidade do headway (segundos)",
                min_value=1,
                step=300,
                value=300,
                key="t1_granularidade_13",
            )
            traco_ausente = st.selectbox(
                "Tratamento de '-' (traco_ausente)",
                ["zerar", "erro"],
                index=0,
                key="t1_traco_ausente_13",
            )
            gerar_relatorio = st.checkbox(
                "Gerar relatório",
                value=True,
                key="t1_gerar_relatorio_13",
            )
            gerar_excel = st.checkbox(
                "Gerar Excel",
                value=True,
                key="t1_gerar_excel_13",
            )
            nome_excel = st.text_input(
                "Nome do Excel (nome_excel)",
                value="",
                help="Vazio usa quadros_partidas_<service_id>.xlsx.",
                key="t1_nome_excel_13",
            )
            tipo_dia = st.text_input(
                "Rótulo do tipo de dia (tipo_dia)",
                value="",
                help="Vazio usa o próprio service_id.",
                key="t1_tipo_dia_13",
            )

        if st.button("▶ Executar Script 1.3", key="btn_script_13"):
            config = {
                "BASE_RESULTADOS": base_resultados,
                "ano_gtfs": st.session_state.ano_gtfs,
                "mes_gtfs": st.session_state.mes_gtfs,
                "estudo_gtfs": st.session_state.estudo_gtfs,
                "caminho_partidas": caminho_partidas,
                "service_id": service_id,
                "granularidade_headway": int(granularidade_headway),
                "traco_ausente": traco_ausente,
                "linhas_rodar": linhas_str,
                "gerar_relatorio": gerar_relatorio,
                "gerar_excel": gerar_excel,
            }
            _opcional(config, "pasta_saida", pasta_saida)
            _opcional(config, "nome_excel", nome_excel)
            _opcional(config, "tipo_dia", tipo_dia)
            run_script(_script_path("1.3_qh_a_partir_de_partidas.py"), config)

    elif modo == "1.3.1":
        st.subheader("Script 1.3.1 — Conferir partidas geradas")
        st.markdown("Confere de forma independente os quadros gerados pelo Script 1.3 contra a tabela de entrada.")

        col1, col2 = st.columns(2)
        with col1:
            base_resultados = st.text_input(
                "Pasta de Resultados (BASE_RESULTADOS)",
                value=base_resultados_default,
                key="t1_base_resultados_131",
            )
            caminho_partidas = st.text_input(
                "Tabela de Contagens (caminho_partidas)",
                value="partidas_pf_sppo_como_du.csv",
                key="t1_caminho_partidas_131",
            )
            pasta_saida = st.text_input(
                "Pasta Conferida (pasta_saida)",
                value=f"{base_resultados_default}/quadro_horario_extraido/{ano}/{mes}/qh_por_linha_partidas/{estudo}Q",
                key="t1_pasta_saida_131",
            )
        with col2:
            service_id = st.text_input(
                "service_id",
                value="F_REG",
                key="t1_service_id_131",
            )
            traco_ausente = st.selectbox(
                "Tratamento de '-' (traco_ausente)",
                ["zerar", "erro"],
                index=0,
                key="t1_traco_ausente_131",
            )
            linhas_str = st.text_input(
                "Linhas a Rodar (linhas_rodar)",
                value="",
                help="Deve refletir o filtro usado na geração; vazio processa todas.",
                key="t1_linhas_str_131",
            )
            verificar_excel = st.checkbox(
                "Conferir Excel",
                value=True,
                key="t1_verificar_excel_131",
            )
            nome_excel = st.text_input(
                "Nome do Excel (nome_excel)",
                value="",
                help="Vazio usa quadros_partidas_<service_id>.xlsx.",
                key="t1_nome_excel_131",
            )

        if st.button("▶ Executar Script 1.3.1", key="btn_script_131"):
            config = {
                "BASE_RESULTADOS": base_resultados,
                "ano_gtfs": st.session_state.ano_gtfs,
                "mes_gtfs": st.session_state.mes_gtfs,
                "estudo_gtfs": st.session_state.estudo_gtfs,
                "caminho_partidas": caminho_partidas,
                "service_id": service_id,
                "traco_ausente": traco_ausente,
                "linhas_rodar": linhas_str,
                "verificar_excel": verificar_excel,
            }
            _opcional(config, "pasta_saida", pasta_saida)
            _opcional(config, "nome_excel", nome_excel)
            run_script(_script_path("1.3.1_conferir_partidas_geradas.py"), config)

    elif modo == "1.4":
        st.subheader("Script 1.4 — QH de ponto facultativo")
        st.markdown("Gera quadros F_REG a partir das frequências do GTFS, excluindo a MOBI-Rio.")

        repo_raiz = Path(__file__).resolve().parents[2]
        col1, col2 = st.columns(2)
        with col1:
            base_resultados = st.text_input(
                "Pasta de Resultados (BASE_RESULTADOS)",
                value=base_resultados_default,
                key="t1_base_resultados_14",
            )
            end_gtfs = st.text_input(
                "GTFS de Entrada (end_gtfs)",
                value=str(repo_raiz / "0171_gtfs_out_26_1E_ret.zip"),
                key="t1_end_gtfs_14",
            )
            pasta_saida = st.text_input(
                "Pasta de Saída (pasta_saida)",
                value=f"{base_resultados_default}/quadro_horario_extraido/{ano}/{mes}/qh_por_linha_pf_gtfs/{estudo}Q",
                key="t1_pasta_saida_14",
            )
            linhas_str = st.text_input(
                "Linhas a Rodar (linhas_rodar)",
                value="",
                help="Separe por vírgula; vazio processa todas as linhas.",
                key="t1_linhas_str_14",
            )
        with col2:
            service_id = st.text_input(
                "service_id",
                value="F_REG",
                key="t1_service_id_14",
            )
            agencias_excluidas = st.text_input(
                "Agências excluídas (agencias_excluidas)",
                value="20001",
                help="Separe por vírgula.",
                key="t1_agencias_excluidas_14",
            )
            route_types_permitidos = st.text_input(
                "Route types permitidos",
                value="700",
                help="Separe por vírgula.",
                key="t1_route_types_14",
            )
            gerar_relatorio = st.checkbox(
                "Gerar relatório",
                value=True,
                key="t1_gerar_relatorio_14",
            )
            gerar_excel = st.checkbox(
                "Gerar Excel",
                value=True,
                key="t1_gerar_excel_14",
            )
            nome_excel = st.text_input(
                "Nome do Excel (nome_excel)",
                value="",
                help="Vazio usa quadros_partidas_<service_id>.xlsx.",
                key="t1_nome_excel_14",
            )
            tipo_dia = st.text_input(
                "Rótulo do tipo de dia (tipo_dia)",
                value="",
                help="Vazio usa o próprio service_id.",
                key="t1_tipo_dia_14",
            )

        if st.button("▶ Executar Script 1.4", key="btn_script_14"):
            config = {
                "BASE_RESULTADOS": base_resultados,
                "ano_gtfs": st.session_state.ano_gtfs,
                "mes_gtfs": st.session_state.mes_gtfs,
                "estudo_gtfs": st.session_state.estudo_gtfs,
                "end_gtfs": end_gtfs,
                "service_id": service_id,
                "agencias_excluidas": agencias_excluidas,
                "route_types_permitidos": route_types_permitidos,
                "linhas_rodar": linhas_str,
                "gerar_relatorio": gerar_relatorio,
                "gerar_excel": gerar_excel,
            }
            _opcional(config, "pasta_saida", pasta_saida)
            _opcional(config, "nome_excel", nome_excel)
            _opcional(config, "tipo_dia", tipo_dia)
            run_script(_script_path("1.4_qh_pf_a_partir_do_gtfs.py"), config)
