import streamlit as st
from pathlib import Path
import os
from src.runner import run_script

def render():
    st.header("5. Juntar GTFS")
    st.markdown("Opções para juntar múltiplos GTFS dependendo do caso de uso.")
    
    modo = st.radio(
        "Selecione o Script de Junção / Limpeza", 
        [
            "5. Juntar SPPO + BRT", 
            "5.1 Juntar Único (apenas SPPO ou BRT)", 
            "5.2 Concatenar Simples", 
            "5.3 Juntar c/ Substituição",
            "5.4 Eliminar Duplicatas GTFS"
        ],
        key="t5_modo"
    )
    
    st.subheader("Parâmetros Específicos")
    
    base_dados = st.session_state.BASE_DADOS
    ano = st.session_state.ano_gtfs
    mes = st.session_state.mes_gtfs
    estudo = st.session_state.estudo_gtfs
    processar = st.session_state.gtfs_processar
    sufixo = f"{ano}-{mes}-{estudo}Q"
    
    if "5. Juntar SPPO" in modo:
        col1, col2 = st.columns(2)
        with col1:
            etapa_gtfs_rio = st.text_input(
                "Etapa GTFS Rio (etapa_gtfs_rio)",
                value="ETAPA_01",
                help="Etapa(s) do GTFS Rio. Pode ser múltipla ex: ETAPA_01,ETAPA_02",
                key="t5_etapa_gtfs_rio_1"
            )
            linhas_excluir = st.text_area(
                "Linhas a Excluir",
                value="",
                help="Lista de trip_short_name separados por vírgula",
                key="t5_linhas_excluir_1"
            )
        with col2:
            endereco_sppo = st.text_input("Endereço SPPO", value=f"{base_dados}/gtfs/{ano}/sppo_{sufixo}_PROC.zip", key="t5_endereco_sppo_1")
            endereco_brt = st.text_input("Endereço BRT", value=f"{base_dados}/gtfs/{ano}/brt_{sufixo}_PROC.zip", key="t5_endereco_brt_1")
            endereco_gtfs_combi = st.text_input("Endereço Saída (Combi)", value=f"{base_dados}/gtfs/{ano}/gtfs_combi_{sufixo}.zip", key="t5_endereco_combi_1")
            
        if st.button("▶ Executar Script 5", key="btn_5"):
            linhas = [x.strip() for x in linhas_excluir.split(',')] if linhas_excluir else []
            config = {
                "BASE_DADOS": base_dados,
                "ano_gtfs": ano,
                "mes_gtfs": mes,
                "estudo_gtfs": estudo,
                "gtfs_processar": processar,
                "etapa_gtfs_rio": etapa_gtfs_rio,
                "endereco_sppo": endereco_sppo,
                "endereco_brt": endereco_brt,
                "endereco_gtfs_combi": endereco_gtfs_combi,
                "linhas_excluir": linhas
            }
            script_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "codigos_py", "5_juntar_gtfs.py"))
            run_script(script_path, config)
            
    elif "5.1" in modo:
        col1, col2 = st.columns(2)
        with col1:
            gtfs_processar = st.selectbox(
                "GTFS a Processar",
                ["rio", "brt", "sppo"],
                index=0,
                key="t5_gtfs_processar_2"
            )
            etapa_gtfs_rio = st.text_input(
                "Etapa GTFS Rio",
                value="ETAPA_01",
                key="t5_etapa_gtfs_rio_2"
            )
        with col2:
            endereco_sppo = st.text_input("Endereço SPPO", value=f"{base_dados}/gtfs/{ano}/sppo_{sufixo}_PROC.zip", key="t5_endereco_sppo_2")
            endereco_brt = st.text_input("Endereço BRT", value=f"{base_dados}/gtfs/{ano}/brt_{sufixo}_PROC.zip", key="t5_endereco_brt_2")
            endereco_gtfs_combi = st.text_input("Endereço Saída", value=f"{base_dados}/gtfs/{ano}/gtfs_combi_{sufixo}.zip", key="t5_endereco_combi_2")
            
        if st.button("▶ Executar Script 5.1", key="btn_51"):
            config = {
                "BASE_DADOS": base_dados,
                "ano_gtfs": ano,
                "mes_gtfs": mes,
                "estudo_gtfs": estudo,
                "gtfs_processar": gtfs_processar,
                "etapa_gtfs_rio": etapa_gtfs_rio,
                "endereco_sppo": endereco_sppo,
                "endereco_brt": endereco_brt,
                "endereco_gtfs_combi": endereco_gtfs_combi
            }
            script_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "codigos_py", "5.1_juntar_gtfs_sppo.py"))
            run_script(script_path, config)
            
    elif "5.2" in modo:
        st.markdown("Concatena múltiplos GTFS numa única saída.")
        
        uploaded_files = st.file_uploader(
            "Upload de GTFS (Opcional)",
            type=['zip'],
            accept_multiple_files=True,
            key="t5_uploaded_files"
        )
        
        input_zips = st.text_area(
            "Lista de Arquivos ZIP (Um por linha)",
            value=f"{base_dados}/gtfs/{ano}/GTFS_Filtrado_141.zip\n{base_dados}/gtfs/{ano}/GTFS_Filtrado_143.zip",
            help="Ou informe os caminhos manualmente",
            key="t5_input_zips"
        )
        output_zip = st.text_input(
            "Arquivo ZIP de Saída",
            value=f"{base_dados}/gtfs/{ano}/gtfs_combined.zip",
            key="t5_output_zip_3"
        )
        
        if st.button("▶ Executar Script 5.2", key="btn_52"):
            # Lidar com temp files
            final_inputs = []
            temp_files = []
            if uploaded_files:
                import tempfile
                for uf in uploaded_files:
                    with tempfile.NamedTemporaryFile(delete=False, suffix='.zip') as f:
                        f.write(uf.getbuffer())
                        temp_files.append(f.name)
                        final_inputs.append(f.name)
                st.info(f"Usando {len(uploaded_files)} arquivos do upload.")
            else:
                final_inputs = [x.strip() for x in input_zips.split('\n') if x.strip()]
                
            config = {
                "INPUT_ZIPS": final_inputs,
                "OUTPUT_ZIP": output_zip
            }
            script_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "codigos_py", "5.2_juntar_gtfs_simples.py"))
            
            try:
                run_script(script_path, config)
            finally:
                for tf in temp_files:
                    try:
                        os.remove(tf)
                    except:
                        pass

    elif "5.3" in modo:
        st.markdown("Junta dois GTFS com o detalhe de que as rotas do GTFS 2 substituem as rotas equivalentes no GTFS 1.")
        
        col1, col2 = st.columns(2)
        with col1:
            gtfs_1_path = st.text_input(
                "GTFS 1 Base (Substituído)",
                value=f"{base_dados}/gtfs/{ano}/0143_gtfs_ago_26_1E_ret1.zip",
                key="t5_gtfs_1_path"
            )
            gtfs_2_path = st.text_input(
                "GTFS 2 Modificador (Substitui)",
                value=f"{base_dados}/gtfs/{ano}/GTFS_Filtrado_141.zip",
                key="t5_gtfs_2_path"
            )
        with col2:
            output_zip = st.text_input(
                "GTFS de Saída",
                value=f"{base_dados}/gtfs/{ano}/gtfs_combined2.zip",
                key="t5_output_zip_4"
            )
            
        if st.button("▶ Executar Script 5.3", key="btn_53"):
            config = {
                "GTFS_1_PATH": gtfs_1_path,
                "GTFS_2_PATH": gtfs_2_path,
                "OUTPUT_ZIP": output_zip
            }
            script_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "codigos_py", "5.3_juntar_gtfs_substituicao.py"))
            run_script(script_path, config)

    elif "5.4" in modo:
        st.markdown("Elimina duplicatas em linhas regulares (mesmo serviço, sentido e partidas), **preservando 100% das viagens de excepcionalidades (`EXCEP` e desvios)**.")
        
        col1, col2 = st.columns(2)
        with col1:
            endereco_gtfs = st.text_input(
                "Caminho do Arquivo GTFS (Entrada)",
                value=f"{base_dados}/gtfs/{ano}/gtfs_combi_{sufixo}.zip",
                help="Informe o caminho completo do arquivo GTFS ZIP a ser deduplicado (ex: C:/R_SMTR/dados/GTFS/2027/gtfs_combi_2026-07-02Q.zip)",
                key="t5_endereco_gtfs_54"
            )
        with col2:
            caminho_saida = st.text_input(
                "Caminho do GTFS de Saída (Opcional)",
                value="",
                help="Deixe em branco para sobrescrever o arquivo de entrada com o GTFS limpo, ou informe um novo caminho.",
                key="t5_caminho_saida_54"
            )
            
        if st.button("▶ Executar Script 5.4", key="btn_54"):
            config = {
                "endereco_gtfs": endereco_gtfs,
                "caminho_saida": caminho_saida
            }
            script_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "codigos_py", "5.4_eliminar_duplicatas_gtfs.py"))
            run_script(script_path, config)
