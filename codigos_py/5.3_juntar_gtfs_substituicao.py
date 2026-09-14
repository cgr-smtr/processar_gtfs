import pandas as pd
import zipfile
import os
import time
import numpy as np
from pathlib import Path
import warnings

warnings.filterwarnings('ignore', category=pd.errors.DtypeWarning)

import argparse
import json

# ==============================================================================
# LEITURA DE CONFIGURAÇÃO (Streamlit)
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
# Caminhos dos arquivos GTFS de entrada e saída
GTFS_1_PATH = Path(_config.get("GTFS_1_PATH", r"C:/R_SMTR/dados/gtfs/2027/0152_gtfs_ago_26_2E_ret2.zip"))
GTFS_2_PATH = Path(_config.get("GTFS_2_PATH", r"C:/R_SMTR/dados/gtfs/2027/GTFS_Filtrado.zip"))
OUTPUT_ZIP = Path(_config.get("OUTPUT_ZIP", r"C:/R_SMTR/dados/gtfs/2026/gtfs_combined_868_CORRIGIDA.zip"))

# ==============================================================================
# FUNÇÕES AUXILIARES
# ==============================================================================
def log_msg(msg):
    t = time.strftime("%H:%M:%S")
    print(f"[{t}] {msg}")

def read_gtfs(zip_path):
    log_msg(f"Lendo: {zip_path}")
    gtfs_data = {}
    with zipfile.ZipFile(zip_path, 'r') as z:
        for fname in z.namelist():
            if fname.endswith('.txt'):
                with z.open(fname) as f:
                    try:
                        gtfs_data[fname.split('.')[0]] = pd.read_csv(f, dtype=str)
                    except pd.errors.EmptyDataError:
                        gtfs_data[fname.split('.')[0]] = pd.DataFrame()
    return gtfs_data

def write_gtfs(gtfs_dict, zip_path):
    log_msg(f"Salvando em: {zip_path}")
    os.makedirs(os.path.dirname(zip_path), exist_ok=True)
    with zipfile.ZipFile(zip_path, 'w', compression=zipfile.ZIP_STORED) as zout:
        for key, df in gtfs_dict.items():
            if not df.empty:
                csv_bytes = df.to_csv(index=False).encode('utf-8')
                zout.writestr(f"{key}.txt", csv_bytes)

def clean_gtfs(gtfs_dict):
    """Filtra todas as tabelas associadas com base nas trips existentes."""
    if 'trips' not in gtfs_dict or gtfs_dict['trips'].empty:
        return gtfs_dict

    valid_trips = set(gtfs_dict['trips']['trip_id'])

    if 'stop_times' in gtfs_dict and not gtfs_dict['stop_times'].empty:
        gtfs_dict['stop_times'] = gtfs_dict['stop_times'][
            gtfs_dict['stop_times']['trip_id'].isin(valid_trips)
        ]

    if 'frequencies' in gtfs_dict and not gtfs_dict['frequencies'].empty:
        gtfs_dict['frequencies'] = gtfs_dict['frequencies'][
            gtfs_dict['frequencies']['trip_id'].isin(valid_trips)
        ]

    valid_routes = set(gtfs_dict['trips']['route_id'])
    if 'routes' in gtfs_dict and not gtfs_dict['routes'].empty:
        gtfs_dict['routes'] = gtfs_dict['routes'][
            gtfs_dict['routes']['route_id'].isin(valid_routes)
        ]

    if 'shapes' in gtfs_dict and not gtfs_dict['shapes'].empty \
            and 'shape_id' in gtfs_dict['trips'].columns:
        valid_shapes = set(gtfs_dict['trips']['shape_id'].dropna())
        gtfs_dict['shapes'] = gtfs_dict['shapes'][
            gtfs_dict['shapes']['shape_id'].isin(valid_shapes)
        ]

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
        gtfs_dict['calendar'] = gtfs_dict['calendar'][
            gtfs_dict['calendar']['service_id'].isin(valid_services)
        ]
    if 'calendar_dates' in gtfs_dict and not gtfs_dict['calendar_dates'].empty:
        gtfs_dict['calendar_dates'] = gtfs_dict['calendar_dates'][
            gtfs_dict['calendar_dates']['service_id'].isin(valid_services)
        ]

    if 'routes' in gtfs_dict and not gtfs_dict['routes'].empty:
        valid_agencies = (
            set(gtfs_dict['routes']['agency_id'].dropna())
            if 'agency_id' in gtfs_dict['routes'].columns else set()
        )
        if valid_agencies and 'agency' in gtfs_dict \
                and not gtfs_dict['agency'].empty \
                and 'agency_id' in gtfs_dict['agency'].columns:
            gtfs_dict['agency'] = gtfs_dict['agency'][
                gtfs_dict['agency']['agency_id'].isin(valid_agencies)
            ]

    return gtfs_dict

# ==============================================================================
# PROCESSAMENTO PRINCIPAL
# ==============================================================================
print("\n================================================================================")
print("             JUNÇÃO DE DOIS GTFS COM SUBSTITUIÇÃO DE LINHAS (GTFS 2 > GTFS 1)")
print("================================================================================\n")

tempo_inicio = time.time()

# 1. Carregar os dois GTFSs
if not os.path.exists(GTFS_1_PATH):
    raise FileNotFoundError(f"Arquivo GTFS 1 não encontrado: {GTFS_1_PATH}")
if not os.path.exists(GTFS_2_PATH):
    raise FileNotFoundError(f"Arquivo GTFS 2 não encontrado: {GTFS_2_PATH}")

gtfs_1 = read_gtfs(GTFS_1_PATH)
gtfs_2 = read_gtfs(GTFS_2_PATH)

# 2. Identificar linhas do GTFS 2 a serem substituídas
if 'routes' in gtfs_2 and not gtfs_2['routes'].empty:
    linhas_substituir = set(gtfs_2['routes']['route_short_name'].dropna())
    log_msg(f"Linhas identificadas no GTFS 2 para substituição: {linhas_substituir}")
else:
    linhas_substituir = set()
    log_msg("Nenhuma rota encontrada no GTFS 2.")

# 3. Remover essas linhas do GTFS 1
if linhas_substituir and 'routes' in gtfs_1 and not gtfs_1['routes'].empty:
    # Achar os route_ids das rotas a serem substituídas no GTFS 1
    df_routes_1 = gtfs_1['routes']
    rotas_remover = df_routes_1[df_routes_1['route_short_name'].isin(linhas_substituir)]
    route_ids_remover = set(rotas_remover['route_id'].dropna())
    
    if route_ids_remover:
        log_msg(f"Removendo {len(route_ids_remover)} rotas correspondentes no GTFS 1...")
        gtfs_1['routes'] = df_routes_1[~df_routes_1['route_id'].isin(route_ids_remover)]
        
        # Filtrar as trips dessas rotas do GTFS 1
        if 'trips' in gtfs_1 and not gtfs_1['trips'].empty:
            df_trips_1 = gtfs_1['trips']
            gtfs_1['trips'] = df_trips_1[~df_trips_1['route_id'].isin(route_ids_remover)]
        
        # Limpar registros órfãos no GTFS 1
        log_msg("Limpando registros órfãos nas demais tabelas do GTFS 1...")
        gtfs_1 = clean_gtfs(gtfs_1)

# 4. Concatenar GTFS 1 filtrado com GTFS 2
log_msg("Concatenando tabelas do GTFS 1 e GTFS 2...")
gtfs_combined = {}
todas_tabelas = set(list(gtfs_1.keys()) + list(gtfs_2.keys()))

for tab in todas_tabelas:
    df1 = gtfs_1.get(tab, pd.DataFrame())
    df2 = gtfs_2.get(tab, pd.DataFrame())
    
    if df1.empty and df2.empty:
        gtfs_combined[tab] = pd.DataFrame()
    elif df1.empty:
        gtfs_combined[tab] = df2
    elif df2.empty:
        gtfs_combined[tab] = df1
    else:
        # Garantir mesmo alinhamento de colunas
        todas_colunas = list(dict.fromkeys(list(df1.columns) + list(df2.columns)))
        df1_aligned = df1.reindex(columns=todas_colunas)
        df2_aligned = df2.reindex(columns=todas_colunas)
        gtfs_combined[tab] = pd.concat([df1_aligned, df2_aligned], ignore_index=True)

# 5. Remover duplicatas exatas nas tabelas
log_msg("Removendo duplicatas exatas por tabela...")
for key, df in gtfs_combined.items():
    if not df.empty:
        before = len(df)
        gtfs_combined[key] = df.drop_duplicates()
        after = len(gtfs_combined[key])
        if before != after:
            log_msg(f"  {key}: {before} -> {after} linhas ({before - after} duplicatas removidas)")

# 6. Salvar o GTFS final
write_gtfs(gtfs_combined, OUTPUT_ZIP)

# Estatísticas
log_msg("================================================================================")
log_msg("ESTATISTICAS DO GTFS COMBINADO:")
for k, v in gtfs_combined.items():
    if v.empty:
        continue
    if k == 'shapes':
        log_msg(f"  shapes únicos: {v['shape_id'].nunique() if 'shape_id' in v.columns else 'N/A'}")
    else:
        log_msg(f"  {k}: {len(v)}")
log_msg("================================================================================")

tempo_total = time.time() - tempo_inicio
print(f"\n[OK] Concluido em {tempo_total:.1f} segundos")
print(f"[OK] Arquivo salvo em: {OUTPUT_ZIP}")
