import pandas as pd
import numpy as np
import zipfile
import os
import time
from pathlib import Path
import warnings
import sys

sys.stdout.reconfigure(encoding='utf-8')
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
# Caminho do GTFS de entrada no PC (exemplo com ou sem .zip)
endereco_gtfs_raw = _config.get("endereco_gtfs", r"C:/R_SMTR/dados/GTFS/2027/gtfs_combi_2026-07-02Q.zip")
endereco_gtfs = Path(endereco_gtfs_raw)
if endereco_gtfs.suffix.lower() != '.zip':
    endereco_gtfs = endereco_gtfs.with_suffix('.zip')

# Caminho de saída (se não informado, sobrescreve o arquivo de entrada)
caminho_saida_raw = _config.get("caminho_saida", "")
if caminho_saida_raw:
    caminho_saida = Path(caminho_saida_raw)
    if caminho_saida.suffix.lower() != '.zip':
        caminho_saida = caminho_saida.with_suffix('.zip')
else:
    caminho_saida = endereco_gtfs

# Suporte a múltiplos arquivos alvo opcionais via config
arquivos_alvo = _config.get("arquivos_alvo", [])
if not arquivos_alvo:
    arquivos_alvo = [str(endereco_gtfs)]
elif isinstance(arquivos_alvo, str):
    arquivos_alvo = [x.strip() for x in arquivos_alvo.split(',') if x.strip()]

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
                with z.open(fname) as f:
                    try:
                        gtfs_data[fname.split('.')[0]] = pd.read_csv(f, dtype=str)
                    except pd.errors.EmptyDataError:
                        gtfs_data[fname.split('.')[0]] = pd.DataFrame()
    return gtfs_data


def write_gtfs(gtfs_dict, zip_path):
    log_msg(f"Salvando GTFS em: {zip_path}")
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

    valid_trips = set(gtfs_dict['trips']['trip_id'].dropna())

    if 'stop_times' in gtfs_dict and not gtfs_dict['stop_times'].empty:
        gtfs_dict['stop_times'] = gtfs_dict['stop_times'][
            gtfs_dict['stop_times']['trip_id'].isin(valid_trips)
        ]

    if 'frequencies' in gtfs_dict and not gtfs_dict['frequencies'].empty:
        gtfs_dict['frequencies'] = gtfs_dict['frequencies'][
            gtfs_dict['frequencies']['trip_id'].isin(valid_trips)
        ]

    valid_routes = set(gtfs_dict['trips']['route_id'].dropna())
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
        valid_stops = set(gtfs_dict['stop_times']['stop_id'].dropna())
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


def is_trip_excep(df_trips):
    """
    Identifica viagens de excepcionalidades/desvios.
    Trips EXCEP têm service_id contendo 'EXCEP' ou '[' no trip_headsign.
    """
    mask_service = df_trips['service_id'].notna() & df_trips['service_id'].astype(str).str.contains('EXCEP', case=False)
    mask_headsign = df_trips['trip_headsign'].notna() & df_trips['trip_headsign'].astype(str).str.contains(r'\[')
    return mask_service | mask_headsign


def deduplicar_gtfs(gtfs_dict):
    """
    Elimina duplicatas em linhas regulares (sem atuar nas viagens EXCEP).
    
    Critérios:
    1. Separa viagens regulares das viagens EXCEP.
    2. Identifica e resolve duplicatas de routes para linhas regulares mantendo o primeiro route_id.
    3. Remove trips regulares redundantes (mesmo route_short_name, direction_id, service_id, horários/itinerários).
    4. Preserva 100% das viagens EXCEP.
    5. Executa clean_gtfs para garantir integridade referencial.
    """
    if 'trips' not in gtfs_dict or gtfs_dict['trips'].empty:
        log_msg("⚠️ Tabela trips não encontrada ou vazia.")
        return gtfs_dict

    df_trips = gtfs_dict['trips'].copy()
    df_routes = gtfs_dict.get('routes', pd.DataFrame()).copy()
    df_st = gtfs_dict.get('stop_times', pd.DataFrame())
    df_freq = gtfs_dict.get('frequencies', pd.DataFrame())

    total_trips_ini = len(df_trips)
    total_routes_ini = len(df_routes)

    # 1. Separar EXCEP vs Regulares
    mask_excep = is_trip_excep(df_trips)
    trips_excep = df_trips[mask_excep].copy()
    trips_reg = df_trips[~mask_excep].copy()

    log_msg(f"Total de trips: {total_trips_ini} ({len(trips_excep)} EXCEP preservadas, {len(trips_reg)} regulares a avaliar)")

    if trips_reg.empty:
        log_msg("Nenhuma viagem regular para deduplicar.")
        return gtfs_dict

    # Enriquecer trips regulares com dados de rotas
    if not df_routes.empty:
        cols_routes = [c for c in ['route_id', 'route_short_name', 'agency_id'] if c in df_routes.columns]
        trips_reg = trips_reg.merge(df_routes[cols_routes], on='route_id', how='left')
    else:
        if 'route_short_name' not in trips_reg.columns:
            trips_reg['route_short_name'] = trips_reg['trip_short_name']

    # Se trip_short_name estiver vazio, usa route_short_name e vice-versa
    if 'trip_short_name' in trips_reg.columns:
        trips_reg['servico'] = trips_reg['trip_short_name'].fillna(trips_reg.get('route_short_name', ''))
    else:
        trips_reg['servico'] = trips_reg.get('route_short_name', '')

    # 2. Obter assinatura de partida para trips regulares (frequencies ou stop_times)
    # a) De frequencies:
    dict_freq_start = {}
    if not df_freq.empty and 'trip_id' in df_freq.columns:
        for _, row in df_freq.iterrows():
            tid = str(row['trip_id'])
            st_time = str(row.get('start_time', ''))
            ed_time = str(row.get('end_time', ''))
            hw = str(row.get('headway_secs', ''))
            dict_freq_start[tid] = f"FREQ_{st_time}_{ed_time}_{hw}"

    # b) De stop_times (primeiro ponto):
    dict_st_first = {}
    if not df_st.empty and 'trip_id' in df_st.columns:
        # Pega a menor sequência de cada trip_id
        df_st_first = df_st.sort_values(by=['trip_id', 'stop_sequence']).drop_duplicates(subset=['trip_id'], keep='first')
        for _, row in df_st_first.iterrows():
            tid = str(row['trip_id'])
            dep = str(row.get('departure_time', ''))
            sid = str(row.get('stop_id', ''))
            dict_st_first[tid] = f"ST_{dep}_{sid}"

    def get_time_signature(row):
        tid = str(row['trip_id'])
        if tid in dict_freq_start:
            return dict_freq_start[tid]
        if tid in dict_st_first:
            return dict_st_first[tid]
        return f"SHAPE_{row.get('shape_id', '')}"

    trips_reg['time_signature'] = trips_reg.apply(get_time_signature, axis=1)

    # 3. Deduplicar trips regulares
    # Chave de unicidade para viagem regular: (serviço/linha, direction_id, service_id, time_signature)
    cols_dedup = ['servico', 'direction_id', 'service_id', 'time_signature']
    
    # Preserva apenas as colunas originais de trips após o drop_duplicates
    cols_trips_orig = [c for c in df_trips.columns]
    
    trips_reg_dedup = trips_reg.drop_duplicates(subset=cols_dedup, keep='first')
    dups_removidas = len(trips_reg) - len(trips_reg_dedup)

    if dups_removidas > 0:
        # Identificar quais linhas tiveram duplicatas removidas
        linhas_afetadas = trips_reg[trips_reg.duplicated(subset=cols_dedup, keep=False)]['servico'].unique()
        linhas_str = ", ".join([str(x) for x in sorted(linhas_afetadas) if pd.notna(x)])
        log_msg(f"  ✓ {dups_removidas} trips regulares duplicadas removidas.")
        log_msg(f"  ✓ Linhas afetadas ({len(linhas_afetadas)}): {linhas_str}")
    else:
        log_msg("  ✓ Nenhuma trip regular duplicada encontrada.")

    # Recombinar trips regulares deduplicadas com trips EXCEP
    trips_final = pd.concat([trips_reg_dedup[cols_trips_orig], trips_excep[cols_trips_orig]], ignore_index=True)
    gtfs_dict['trips'] = trips_final

    # 4. Deduplicar rotas regulares se houver múltiplos route_id para o mesmo route_short_name
    if not df_routes.empty and 'route_short_name' in df_routes.columns:
        # Verifica rotas usadas nas trips finais
        routes_ativas = set(trips_final['route_id'].dropna())
        df_routes_ativas = df_routes[df_routes['route_id'].isin(routes_ativas)].copy()
        
        # Rotas com route_type != 200 (não frescão) com mesmo route_short_name
        # Mantém a primeira ocorrência
        df_routes_dedup = df_routes_ativas.drop_duplicates(subset=['route_short_name', 'route_type'], keep='first')
        routes_removidas = len(df_routes_ativas) - len(df_routes_dedup)
        
        if routes_removidas > 0:
            routes_mantidas_ids = set(df_routes_dedup['route_id'].dropna())
            log_msg(f"  ✓ {routes_removidas} route_ids redundantes eliminados em routes.txt.")
            # Atualiza trips que usavam os route_ids redundantes para o route_id mantido correspondente
            mapa_route_subst = {}
            for _, r in df_routes_dedup.iterrows():
                sn = r['route_short_name']
                rt = r['route_type']
                r_id_correto = r['route_id']
                outros = df_routes_ativas[(df_routes_ativas['route_short_name'] == sn) & (df_routes_ativas['route_type'] == rt)]['route_id']
                for oid in outros:
                    if oid != r_id_correto:
                        mapa_route_subst[oid] = r_id_correto
            
            if mapa_route_subst:
                gtfs_dict['trips']['route_id'] = gtfs_dict['trips']['route_id'].replace(mapa_route_subst)
            
            gtfs_dict['routes'] = df_routes_dedup
        else:
            gtfs_dict['routes'] = df_routes_ativas

    # 5. Limpeza em cascata para remover registros órfãos
    log_msg("Aplicando limpeza em cascata (clean_gtfs)...")
    gtfs_dict = clean_gtfs(gtfs_dict)

    # Estatísticas de resumo
    log_msg(f"Trips finais: {len(gtfs_dict['trips'])} (antes: {total_trips_ini}) | Rotas finais: {len(gtfs_dict['routes'])} (antes: {total_routes_ini})")
    return gtfs_dict


# ==============================================================================
# PROCESSAMENTO PRINCIPAL
# ==============================================================================
def main():
    print("\n╔════════════════════════════════════════════════════════════════════════════╗")
    print("║          ELIMINAÇÃO DE DUPLICATAS EM LINHAS REGULARES NO GTFS             ║")
    print("╚════════════════════════════════════════════════════════════════════════════╝\n")

    tempo_inicio = time.time()
    arquivos_processados = 0

    for arq in arquivos_alvo:
        caminho_in = Path(arq)
        if not caminho_in.exists():
            log_msg(f"⚠️ Arquivo não encontrado (ignorado): {caminho_in}")
            continue

        print(f"\n--------------------------------------------------------------------------------")
        log_msg(f"Processando arquivo: {caminho_in.name}")
        print(f"--------------------------------------------------------------------------------")

        gtfs = read_gtfs(caminho_in)
        gtfs_dedup = deduplicar_gtfs(gtfs)

        if len(arquivos_alvo) == 1 and caminho_saida != caminho_in:
            caminho_out = caminho_saida
        else:
            caminho_out = caminho_in

        write_gtfs(gtfs_dedup, caminho_out)
        arquivos_processados += 1

    tempo_total = time.time() - tempo_inicio
    print("\n╔════════════════════════════════════════════════════════════════════════════╗")
    print(f"║ CONCLUÍDO: {arquivos_processados} arquivo(s) processado(s) em {tempo_total:.1f}s")
    print("╚════════════════════════════════════════════════════════════════════════════╝\n")


if __name__ == '__main__':
    main()
