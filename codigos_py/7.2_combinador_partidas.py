import os
import json
import argparse
import pandas as pd

# ==============================================================================
# LEITURA DE CONFIGURAÇÃO (Linha de Comando / Streamlit / JSON)
# ==============================================================================
_parser = argparse.ArgumentParser(
    description="Combina partidas de domingo com o bloco de dia útil (06h-20h)."
)
_parser.add_argument('--config', type=str, default='', help='Caminho para arquivo JSON de configuração')
_args, _ = _parser.parse_known_args()
_config = {}
if _args.config:
    if os.path.isfile(_args.config):
        with open(_args.config, 'r', encoding='utf-8') as _f:
            _config = json.load(_f)
    else:
        try:
            _config = json.loads(_args.config)
        except Exception:
            with open(_args.config, 'r', encoding='utf-8') as _f:
                _config = json.load(_f)

caminho_du = str(_config.get("caminho_du", "quadros_du.csv"))
caminho_dom = str(_config.get("caminho_dom", "quadros_dom.csv"))
caminho_saida = str(_config.get("caminho_saida", "quadros_dom_04-10.csv"))


def parse_time_to_seconds(t_str):
    """Converte horário no formato HH:MM:SS para segundos para permitir cálculos de intervalo."""
    parts = list(map(int, str(t_str).strip().split(':')))
    if len(parts) == 3:
        return parts[0] * 3600 + parts[1] * 60 + parts[2]
    elif len(parts) == 2:
        return parts[0] * 3600 + parts[1] * 60
    return 0

def seconds_to_time_format(seconds):
    """Converte segundos de volta para o formato HH:MM:SS."""
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    return f"{h:02d}:{m:02d}:{s:02d}"

def processar_combinador_quadros(caminho_du='quadros_du.csv', caminho_dom='quadros_dom.csv', caminho_saida='quadros_dom_04-10.csv'):
    # 1. Leitura dos arquivos de entrada
    df_du = pd.read_csv(caminho_du)
    df_dom = pd.read_csv(caminho_dom)

    # 2. Restrição: Manter apenas os serviços (trip_short_name) presentes na tabela quadros_dom
    servicos_dom_validos = df_dom['trip_short_name'].unique()
    df_du_filtrado = df_du[df_du['trip_short_name'].isin(servicos_dom_validos)].copy()
    df_dom_filtrado = df_dom[df_dom['trip_short_name'].isin(servicos_dom_validos)].copy()

    # Conversão do tempo para segundos auxiliares para ordenação e cálculo exato
    df_du_filtrado['sec_temp'] = df_du_filtrado['departure_time'].apply(parse_time_to_seconds)
    df_dom_filtrado['sec_temp'] = df_dom_filtrado['departure_time'].apply(parse_time_to_seconds)

    sec_06h = parse_time_to_seconds("06:00:00")
    sec_20h = parse_time_to_seconds("20:00:00")

    group_cols = ['trip_short_name', 'direction_id'] if 'direction_id' in df_dom.columns else ['trip_short_name']
    resultados = []

    # 3. Processamento por serviço e sentido
    for keys, group_dom in df_dom_filtrado.groupby(group_cols):
        # Selecionar o correspondente em dia útil
        if len(group_cols) > 1:
            cond = (df_du_filtrado['trip_short_name'] == keys[0]) & (df_du_filtrado['direction_id'] == keys[1])
            group_du = df_du_filtrado[cond].copy()
        else:
            group_du = df_du_filtrado[df_du_filtrado['trip_short_name'] == keys].copy()

        # Separação das faixas horárias
        # Faixa 1: Domingo (00:00:00 até 05:59:59)
        dom_madrugada = group_dom[group_dom['sec_temp'] < sec_06h].sort_values('sec_temp').copy()

        # Faixa 2: Dia Útil (06:00:00 até 19:59:59)
        du_dia = group_du[(group_du['sec_temp'] >= sec_06h) & (group_du['sec_temp'] < sec_20h)].sort_values('sec_temp').copy()

        # Faixa 3: Domingo (20:00:00 em diante)
        dom_noite = group_dom[group_dom['sec_temp'] >= sec_20h].sort_values('sec_temp').copy()

        # 4. Compatibilização de intervalo - Transição 05:59 -> 06:00
        if not dom_madrugada.empty and not du_dia.empty:
            ult_dom_madrugada_sec = dom_madrugada.iloc[-1]['sec_temp']
            pri_du_dia_sec = du_dia.iloc[0]['sec_temp']
            
            diff_sec = pri_du_dia_sec - ult_dom_madrugada_sec
            idx_primeira_du = du_dia.index[0]
            du_dia.loc[idx_primeira_du, 'intervalo'] = seconds_to_time_format(diff_sec)

        # 5. Compatibilização de intervalo - Transição 19:59 -> 20:00
        if not du_dia.empty and not dom_noite.empty:
            ult_du_dia_sec = du_dia.iloc[-1]['sec_temp']
            pri_dom_noite_sec = dom_noite.iloc[0]['sec_temp']
            
            diff_sec = pri_dom_noite_sec - ult_du_dia_sec
            idx_primeira_dom = dom_noite.index[0]
            dom_noite.loc[idx_primeira_dom, 'intervalo'] = seconds_to_time_format(diff_sec)

        # Concatena os três blocos do serviço
        servico_completo = pd.concat([dom_madrugada, du_dia, dom_noite], ignore_index=True)
        resultados.append(servico_completo)

    # Unificação final de todos os serviços
    df_final = pd.concat(resultados, ignore_index=True)
    df_final = df_final.drop(columns=['sec_temp'])

    # 6. Exportação para arquivo CSV
    df_final.to_csv(caminho_saida, index=False)
    print(f"Arquivo gerado com sucesso: {caminho_saida}")

# Executar o script
if __name__ == "__main__":
    processar_combinador_quadros(caminho_du, caminho_dom, caminho_saida)