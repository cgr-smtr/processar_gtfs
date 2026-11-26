import pandas as pd
import numpy as np
import zipfile
import os
import time
from pathlib import Path
import warnings
import sys
import argparse
import json

# Ensure UTF-8 output on Windows
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')
warnings.filterwarnings('ignore', category=pd.errors.DtypeWarning)

# ==============================================================================
# LEITURA DE CONFIGURAÇÃO (Streamlit / CLI)
# ==============================================================================
_parser = argparse.ArgumentParser()
_parser.add_argument('--config', type=str, default='', help='Caminho para arquivo JSON de configuração')
_args, _ = _parser.parse_known_args()
_config = {}
if _args.config:
    with open(_args.config, 'r', encoding='utf-8') as _f:
        _config = json.load(_f)

# ==============================================================================
# CONFIGURAÇÕES
# ==============================================================================
BASE_DADOS = Path(_config.get("BASE_DADOS", "C:/R_SMTR/dados"))

ano_gtfs      = _config.get("ano_gtfs", "2028")
mes_gtfs      = _config.get("mes_gtfs", "11")
estudo_gtfs   = _config.get("estudo_gtfs", "05")  # ESTUDO, NÃO CONSIDERAR MAIS QUINZENA!!!!
sufixo        = f"{ano_gtfs}-{mes_gtfs}-{estudo_gtfs}Q"

# Tipo de GTFS de entrada para compor o nome padrão quando endereco_gtfs não é informado
gtfs_processar = _config.get("gtfs_processar", "sppo")

endereco_gtfs = Path(_config.get(
    "endereco_gtfs",
    BASE_DADOS / f"gtfs/{ano_gtfs}/{gtfs_processar}_{sufixo}_PROC.zip"
))
endereco_gtfs_combi = Path(_config.get(
    "endereco_gtfs_combi",
    BASE_DADOS / f"gtfs/{ano_gtfs}/gtfs_combi_{sufixo}.zip"
))
caminho_gtfs_pub = Path(_config.get(
    "caminho_gtfs_pub",
    BASE_DADOS / f"gtfs/{ano_gtfs}/gtfs_rio-de-janeiro_pub.zip"
))
caminho_cores = Path(_config.get(
    "caminho_cores",
    BASE_DADOS / "insumos/gtfs_cores.csv"
))

# Filtro opcional de linhas a excluir (usar trip_short_name)
linhas_excluir = _config.get("linhas_excluir", [])
if isinstance(linhas_excluir, str):
    linhas_excluir = [x.strip() for x in linhas_excluir.split(',') if x.strip()]

# ==============================================================================
# FUNÇÕES AUXILIARES
# ==============================================================================
def log_msg(msg):
    t = time.strftime("%H:%M:%S")
    print(f"[{t}] {msg}")


def read_gtfs(zip_path):
    log_msg(f"Lendo GTFS: {zip_path}")
    gtfs_data = {}
    with zipfile.ZipFile(zip_path, 'r') as z:
        for fname in z.namelist():
            if fname.endswith('.txt'):
                key = os.path.basename(fname).rsplit('.', 1)[0]
                with z.open(fname) as f:
                    try:
                        gtfs_data[key] = pd.read_csv(f, dtype=str)
                    except pd.errors.EmptyDataError:
                        gtfs_data[key] = pd.DataFrame()
    return gtfs_data


def write_gtfs(gtfs_dict, zip_path):
    log_msg(f"Salvando GTFS em: {zip_path}")
    os.makedirs(os.path.dirname(zip_path), exist_ok=True)
    with zipfile.ZipFile(zip_path, 'w', compression=zipfile.ZIP_DEFLATED) as zout:
        for key, df in gtfs_dict.items():
            if not df.empty:
                csv_str = df.to_csv(index=False)
                zout.writestr(f"{key}.txt", csv_str)


def ajustar_service_id(df, col="service_id"):
    if col in df.columns:
        mask = df[col].isin(["U", "S", "D"])
        df.loc[mask, col] = df.loc[mask, col] + "_REG"
    return df


def remover_colunas(df, cols_to_remove):
    cols = [c for c in cols_to_remove if c in df.columns]
    if cols:
        df.drop(columns=cols, inplace=True)
    return df


def classificar_route_types(df_routes):
    """
    Preserva route_type de BRT ('702') e Executivos ('200') já definidos no GTFS unificado
    e normaliza as demais rotas (SPPO/RIO -> '700', ou '200' para > 1000 / LECD124 / LECD125).
    """
    if df_routes.empty:
        return df_routes

    df_rs = df_routes.copy()
    if 'route_type' not in df_rs.columns:
        df_rs['route_type'] = ''
    df_rs['route_type'] = (
        df_rs['route_type']
        .fillna('')
        .astype(str)
        .str.strip()
        .replace({'nan': '', 'None': ''})
    )

    mask_preservar = df_rs['route_type'].isin(['702', '200'])
    mask_classificar = ~mask_preservar

    if mask_classificar.any():
        numeros = (
            pd.to_numeric(
                df_rs.loc[mask_classificar, 'route_short_name'].astype(str).str.extract(r'([0-9]+)', expand=False),
                errors='coerce'
            )
            .fillna(0)
            .astype(int)
        )
        df_rs.loc[mask_classificar, 'route_type'] = '700'

        eh_executivo = (numeros > 1000) | df_rs.loc[mask_classificar, 'route_short_name'].isin(["LECD124", "LECD125"])
        idx_exec = eh_executivo[eh_executivo].index
        df_rs.loc[idx_exec, 'route_type'] = '200'

    return df_rs


def atualizar_cores_gtfs(gtfs_dict, caminho_cores_csv):
    if not os.path.exists(caminho_cores_csv):
        raise FileNotFoundError(f"Arquivo de cores não encontrado: {caminho_cores_csv}")

    cores = pd.read_csv(caminho_cores_csv, dtype=str)

    needed = ['route_short_name', 'route_color', 'route_text_color']
    for c in needed:
        if c not in cores.columns:
            raise KeyError(f"O arquivo de cores deve conter as colunas: {needed}")

    cores = cores.drop_duplicates(subset=['route_short_name'], keep='first')

    df_routes = gtfs_dict.get('routes', pd.DataFrame()).copy()
    if df_routes.empty:
        return gtfs_dict

    for col in ('route_color', 'route_text_color'):
        if col not in df_routes.columns:
            df_routes[col] = np.nan

    # Preserva cores originais do BRT (route_type == '702')
    cores_702 = df_routes[df_routes['route_type'] == '702'][
        ['route_id', 'route_short_name', 'route_color', 'route_text_color']
    ].copy()

    # Reseta cores para não-702 antes do merge
    mask_not_702 = df_routes['route_type'] != '702'
    df_routes.loc[mask_not_702, ['route_color', 'route_text_color']] = np.nan

    df_merged = df_routes.merge(cores, on='route_short_name', how='left', suffixes=('', '_csv'))

    mask_has_csv = df_merged['route_color_csv'].notna()
    df_merged.loc[mask_has_csv, 'route_color'] = df_merged.loc[mask_has_csv, 'route_color_csv']
    df_merged.loc[mask_has_csv, 'route_text_color'] = df_merged.loc[mask_has_csv, 'route_text_color_csv']

    df_merged.drop(columns=['route_color_csv', 'route_text_color_csv'], inplace=True, errors='ignore')

    # Restaura cores do BRT (702)
    if not cores_702.empty:
        df_merged.set_index('route_id', inplace=True)
        cores_702.set_index('route_id', inplace=True)
        df_merged.update(cores_702[['route_color', 'route_text_color']])
        df_merged.reset_index(inplace=True)

    # Padroniza cores de Executivos (200)
    mask_200 = df_merged['route_type'] == '200'
    df_merged.loc[mask_200, 'route_color'] = "030478"
    df_merged.loc[mask_200, 'route_text_color'] = "FFFFFF"

    gtfs_dict['routes'] = df_merged
    return gtfs_dict


def clean_gtfs(gtfs_dict):
    """Filters all associated tables based on the currently existing trips, mimicking gtfstools::filter_by_trip_id"""
    if 'trips' not in gtfs_dict or gtfs_dict['trips'].empty:
        return gtfs_dict

    valid_trips = set(gtfs_dict['trips']['trip_id'])

    if 'stop_times' in gtfs_dict and not gtfs_dict['stop_times'].empty:
        gtfs_dict['stop_times'] = gtfs_dict['stop_times'][gtfs_dict['stop_times']['trip_id'].isin(valid_trips)]

    if 'frequencies' in gtfs_dict and not gtfs_dict['frequencies'].empty:
        gtfs_dict['frequencies'] = gtfs_dict['frequencies'][gtfs_dict['frequencies']['trip_id'].isin(valid_trips)]

    valid_routes = set(gtfs_dict['trips']['route_id'])
    if 'routes' in gtfs_dict and not gtfs_dict['routes'].empty:
        gtfs_dict['routes'] = gtfs_dict['routes'][gtfs_dict['routes']['route_id'].isin(valid_routes)]

    if 'shapes' in gtfs_dict and not gtfs_dict['shapes'].empty and 'shape_id' in gtfs_dict['trips'].columns:
        valid_shapes = set(gtfs_dict['trips']['shape_id'].dropna())
        gtfs_dict['shapes'] = gtfs_dict['shapes'][gtfs_dict['shapes']['shape_id'].isin(valid_shapes)]

    if 'stop_times' in gtfs_dict and not gtfs_dict['stop_times'].empty:
        valid_stops = set(gtfs_dict['stop_times']['stop_id'])
        if 'stops' in gtfs_dict and not gtfs_dict['stops'].empty:
            df_stops = gtfs_dict['stops']
            child_stops = df_stops[df_stops['stop_id'].isin(valid_stops)]
            if 'parent_station' in child_stops.columns:
                parents = set(child_stops['parent_station'].replace("", np.nan).dropna())
                valid_stops.update(parents)
            gtfs_dict['stops'] = df_stops[df_stops['stop_id'].isin(valid_stops)]

    valid_services = set(gtfs_dict['trips']['service_id'].dropna())
    if 'calendar' in gtfs_dict and not gtfs_dict['calendar'].empty:
        gtfs_dict['calendar'] = gtfs_dict['calendar'][gtfs_dict['calendar']['service_id'].isin(valid_services)]

    if 'calendar_dates' in gtfs_dict and not gtfs_dict['calendar_dates'].empty:
        gtfs_dict['calendar_dates'] = gtfs_dict['calendar_dates'][gtfs_dict['calendar_dates']['service_id'].isin(valid_services)]

    valid_agencies = set(gtfs_dict['routes']['agency_id'].dropna()) if 'agency_id' in gtfs_dict['routes'].columns else set()
    if valid_agencies and 'agency' in gtfs_dict and not gtfs_dict['agency'].empty and 'agency_id' in gtfs_dict['agency'].columns:
        gtfs_dict['agency'] = gtfs_dict['agency'][gtfs_dict['agency']['agency_id'].isin(valid_agencies)]

    return gtfs_dict


def main():
    print("\n╔════════════════════════════════════════════════════════════════════════════╗")
    print("║        PROCESSAMENTO DE GTFS UNIFICADO (SPPO + RIO + BRT + EXEC)           ║")
    print("╚════════════════════════════════════════════════════════════════════════════╝\n")

    tempo_inicio = time.time()

    # ----------------- 1. CARREGAR E PROCESSAR GTFS UNIFICADO -----------------
    if not os.path.exists(endereco_gtfs):
        raise FileNotFoundError(f"Arquivo GTFS não encontrado: {endereco_gtfs}")

    gtfs_input = read_gtfs(endereco_gtfs)
    log_msg("Processando GTFS unificado (preservando SPPO, RIO, BRT e Executivos)...")

    # Classificar/preservar route_type por modal
    gtfs_input['routes'] = classificar_route_types(gtfs_input['routes'])

    # Ajustar service_id (U/S/D -> U_REG/S_REG/D_REG)
    gtfs_input['trips'] = ajustar_service_id(gtfs_input['trips'])
    if 'calendar' in gtfs_input:
        gtfs_input['calendar'] = ajustar_service_id(gtfs_input['calendar'])

    # Verificar horários vazios em stop_times
    df_st_input = gtfs_input['stop_times']
    mask_vazios = (
        df_st_input['arrival_time'].isna() | (df_st_input['arrival_time'] == "") |
        df_st_input['departure_time'].isna() | (df_st_input['departure_time'] == "")
    )
    stops_vazios = df_st_input[mask_vazios]

    if not stops_vazios.empty:
        log_msg(f"⚠️  GTFS tem {len(stops_vazios)} stops com horários vazios!")
        raise ValueError("⛔ GTFS processado está com horários vazios.")

    trips_com_st = df_st_input['trip_id'].unique()
    trips_excep = gtfs_input['trips'][gtfs_input['trips']['service_id'] == "EXCEP"]['trip_id'].unique()
    trips_manter = np.unique(np.concatenate([trips_com_st, trips_excep]))

    path_fantasmas = BASE_DADOS / "insumos/trip_id_fantasma.txt"
    if os.path.exists(path_fantasmas):
        trips_fantasma = pd.read_csv(path_fantasmas, header=None).iloc[:, 0].tolist()
    else:
        trips_fantasma = []

    trips_final_input = set(trips_manter) - set(trips_fantasma)

    gtfs_input['trips'] = gtfs_input['trips'][gtfs_input['trips']['trip_id'].isin(trips_final_input)]

    if linhas_excluir and 'trip_short_name' in gtfs_input['trips'].columns:
        gtfs_input['trips'] = gtfs_input['trips'][~gtfs_input['trips']['trip_short_name'].isin(linhas_excluir)]

    gtfs_input = clean_gtfs(gtfs_input)

    contagem_tipos = gtfs_input['routes']['route_type'].value_counts().to_dict() if 'route_type' in gtfs_input['routes'].columns else {}
    log_msg(f"✓ GTFS processado — {len(gtfs_input['routes'])} rotas {contagem_tipos}, {len(gtfs_input['trips'])} trips")

    # ----------------- 2. PREPARAR BASE COMBINADA -----------------
    log_msg("Preparando GTFS unificado como base combinada...")
    gtfs_combi = {k: v.copy() for k, v in gtfs_input.items()}

    # ----------------- 3. LIMPEZA E AJUSTES DO GTFS -----------------
    log_msg("Aplicando limpezas e ajustes...")

    pontos_apagar = []
    if 'stops' in gtfs_combi:
        df_stops = gtfs_combi['stops']
        pontos_apagar = df_stops[df_stops['stop_name'] == "APAGAR"]["stop_id"].tolist()
        df_stops = df_stops.drop_duplicates(subset=['stop_id'])
        df_stops = df_stops[~df_stops['stop_id'].isin(pontos_apagar)]
        gtfs_combi['stops'] = df_stops

    if 'agency' in gtfs_combi:
        gtfs_combi['agency'] = remover_colunas(gtfs_combi['agency'], ["agency_phone", "agency_fare_url", "agency_email", "agency_branding_url"])

    if 'feed_info' in gtfs_combi and not gtfs_combi['feed_info'].empty:
        gtfs_combi['feed_info'] = remover_colunas(gtfs_combi['feed_info'], ["default_lang", "feed_contact_url", "feed_id"])
        gtfs_combi['feed_info'] = gtfs_combi['feed_info'].head(1)

    if 'routes' in gtfs_combi:
        gtfs_combi['routes'] = remover_colunas(gtfs_combi['routes'], ["route_sort_order", "continuous_pickup", "route_branding_url", "continuous_drop_off", "route_url"])

    if 'trips' in gtfs_combi:
        gtfs_combi['trips'] = remover_colunas(gtfs_combi['trips'], ["block_id", "wheelchair_accessible", "bikes_allowed"])
        gtfs_combi['trips'].loc[gtfs_combi['trips']['trip_headsign'].isna() | (gtfs_combi['trips']['trip_headsign'] == ""), 'trip_headsign'] = "Circular"

    if 'calendar' in gtfs_combi:
        gtfs_combi['calendar'].drop_duplicates(inplace=True)
    if 'calendar_dates' in gtfs_combi:
        gtfs_combi['calendar_dates'].drop_duplicates(inplace=True)

    if 'stop_times' in gtfs_combi:
        df_st = gtfs_combi['stop_times']
        df_st = remover_colunas(df_st, ["pickup_type", "drop_off_type", "continuous_pickup", "continuous_drop_off"])
        df_st['timepoint'] = '0'

        if 'shape_dist_traveled' in df_st.columns:
            df_st['shape_dist_traveled'] = pd.to_numeric(df_st['shape_dist_traveled'], errors='coerce').round(2)

        if pontos_apagar:
            df_st = df_st[~df_st['stop_id'].isin(pontos_apagar)]
        gtfs_combi['stop_times'] = df_st

    if 'fare_attributes' in gtfs_combi and not gtfs_combi['fare_attributes'].empty:
        gtfs_combi['fare_attributes']['currency_type'] = "BRL"

    log_msg("✓ Limpezas aplicadas")

    log_msg("Verificando e ordenando shapes...")
    if 'shapes' in gtfs_combi and not gtfs_combi['shapes'].empty:
        df_sh = gtfs_combi['shapes']
        df_sh['shape_pt_sequence'] = pd.to_numeric(df_sh['shape_pt_sequence'], errors='coerce').fillna(0).astype(int)
        df_sh.sort_values(by=['shape_id', 'shape_pt_sequence'], inplace=True)

        shape_counts = df_sh['shape_id'].value_counts()
        shapes_invalidos = shape_counts[shape_counts < 2].index.tolist()
        if shapes_invalidos:
            log_msg(f"⚠️  Encontrados {len(shapes_invalidos)} shapes com menos de 2 pontos.")
        else:
            log_msg("✓ Todos os shapes possuem pelo menos 2 pontos e estão ordenados.")
        gtfs_combi['shapes'] = df_sh

    log_msg("Validando horários no GTFS...")
    df_st_final = gtfs_combi.get('stop_times', pd.DataFrame())
    if not df_st_final.empty:
        mask = (
            df_st_final['arrival_time'].isna() | (df_st_final['arrival_time'] == "") |
            df_st_final['departure_time'].isna() | (df_st_final['departure_time'] == "")
        )
        if mask.any():
            raise ValueError("⛔ ERRO: GTFS tem stops com horários vazios!")
    log_msg(f"✓ Todos os {len(df_st_final)} stops têm horários válidos")

    gtfs_combi = clean_gtfs(gtfs_combi)

    # ==============================================================================
    # 4. SALVAMENTO GTFS COMBI (SEM SUBSTITUIÇÃO DE ARQUIVOS EM DISCO)
    # ==============================================================================
    log_msg("═══════════════════════════════════════════════════════════════")
    log_msg("ESTATÍSTICAS DO GTFS PROCESSADO:")
    for k, v in gtfs_combi.items():
        if k == 'shapes':
            log_msg(f"  ├─ Shapes únicos:   {v['shape_id'].nunique()}")
        else:
            log_msg(f"  ├─ {k}:           {len(v)}")
    log_msg("═══════════════════════════════════════════════════════════════")

    write_gtfs(gtfs_combi, endereco_gtfs_combi)
    log_msg("✓ GTFS combinado salvo (arquivos originais do feed preservados, sem substituição em disco)")

    # ==============================================================================
    # 5. FILTRAGEM FINAL E APLICAÇÃO DE CORES (GTFS PÚBLICO)
    # ==============================================================================
    gtfs_pub = {k: v.copy() for k, v in gtfs_combi.items()}

    log_msg("Removendo viagens com service_id EXCEP...")
    if 'trips' in gtfs_pub:
        trips_pub = gtfs_pub['trips']
        gtfs_pub['trips'] = trips_pub[trips_pub['service_id'] != "EXCEP"]
        gtfs_pub = clean_gtfs(gtfs_pub)

    log_msg("Aplicando cores personalizadas às rotas...")
    gtfs_pub = atualizar_cores_gtfs(gtfs_pub, caminho_cores)

    log_msg("Salvando GTFS público final...")
    write_gtfs(gtfs_pub, caminho_gtfs_pub)
    log_msg("✓ GTFS público salvo (arquivos originais do feed preservados, sem substituição em disco)")

    # ==============================================================================
    # FINALIZAÇÃO
    # ==============================================================================
    tempo_total = time.time() - tempo_inicio
    print("\n╔════════════════════════════════════════════════════════════════════════════╗")
    print("║                       PROCESSAMENTO FINALIZADO!                            ║")
    print(f"║                    Tempo total: {tempo_total:.1f} segundos                             ║")
    print("╚════════════════════════════════════════════════════════════════════════════╝\n")


if __name__ == "__main__":
    main()
