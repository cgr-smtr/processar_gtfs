import pandas as pd
import numpy as np
import zipfile
import io
import os
import json
import math
import time
from pathlib import Path
from datetime import datetime
import warnings

warnings.filterwarnings('ignore', category=pd.errors.DtypeWarning)

# ==============================================================================
# CONFIGURAÇÕES
# ==============================================================================
# Base directory for data - adjust this to your environment
BASE_DADOS = Path("C:/R_SMTR/dados")

ano_velocidade = '2025'
mes_velocidade = '10'

ano_gtfs = '2026'
mes_gtfs = '03'
quinzena_gtfs = '05'

gtfs_processar = 'sppo'  # "brt" ou "sppo"

endereco_gtfs = BASE_DADOS / f"gtfs/{ano_gtfs}/{gtfs_processar}_{ano_gtfs}-{mes_gtfs}-{quinzena_gtfs}Q.zip"
velocidade_padrao_kmh = 15.0

# ==============================================================================
# FUNÇÕES AUXILIARES OTIMIZADAS
# ==============================================================================
def horario_para_segundos(series):
    # Parses strings like "HH:MM:SS" into total seconds, handling HH > 23
    def parse_time(x):
        if pd.isna(x) or x == "":
            return np.nan
        parts = str(x).split(':')
        if len(parts) >= 3:
            return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
        return np.nan
    return series.apply(parse_time)

def segundos_para_horario(series):
    # Formats seconds into "HH:MM:SS", handling HH > 23
    def format_time(x):
        if pd.isna(x):
            return np.nan
        s = int(round(x))
        return f"{s // 3600:02d}:{(s % 3600) // 60:02d}:{s % 60:02d}"
    return series.apply(format_time)

def corrigir_por_distancia_inplace(df, trip_id, vel_padrao_kmh, horario_inicial):
    # Obtains the initial time in seconds
    h_inicio_seg = horario_para_segundos(pd.Series([horario_inicial])).iloc[0]
    
    idx = df['trip_id'] == trip_id
    if not idx.any(): return
    
    # Needs to be sorted by stop_sequence
    sorted_idx = df[idx].sort_values('stop_sequence').index
    primeira_dist = df.loc[sorted_idx[0], 'shape_dist_traveled_num']
    
    distancias = df.loc[idx, 'shape_dist_traveled_num']
    tempo_decorrido = ((distancias - primeira_dist) / 1000.0) / vel_padrao_kmh * 3600.0
    horarios_seg = h_inicio_seg + tempo_decorrido
    novos_horarios = segundos_para_horario(horarios_seg)
    
    mask_arrival = df['arrival_time'].isna() | (df['arrival_time'] == "")
    mask_departure = df['departure_time'].isna() | (df['departure_time'] == "")
    
    df.loc[idx & mask_arrival, 'arrival_time'] = novos_horarios[idx & mask_arrival]
    df.loc[idx & mask_departure, 'departure_time'] = novos_horarios[idx & mask_departure]

def ajustar_shape_dist_traveled(df):
    print("\n==============================================================================")
    print("AJUSTANDO SHAPE_DIST_TRAVELED")
    print("==============================================================================\n")
    df['shape_dist_num'] = pd.to_numeric(df['shape_dist_traveled'], errors='coerce')
    
    # find first dist per trip
    first_dist = df.sort_values('stop_sequence').groupby('trip_id')['shape_dist_num'].first().reset_index()
    trips_para_ajustar = first_dist[first_dist['shape_dist_num'].notna() & (first_dist['shape_dist_num'] != 0)]
    
    print("DIAGNÓSTICO:")
    print(f"├─ Total de trips no GTFS: {df['trip_id'].nunique()}")
    print(f"├─ Trips que não começam em 0: {len(trips_para_ajustar)}")
    print(f"└─ Trips que já estão corretas: {df['trip_id'].nunique() - len(trips_para_ajustar)}\n")
    
    if len(trips_para_ajustar) == 0:
        print("✓ Todas as trips já começam com shape_dist_traveled = 0!\n")
        df.drop(columns=['shape_dist_num'], inplace=True)
        return
        
    print("PROCESSANDO AJUSTES...\n")
    # subtract initial dist
    mapping = trips_para_ajustar.set_index('trip_id')['shape_dist_num']
    df_adjusted = df['shape_dist_num'] - df['trip_id'].map(mapping).fillna(0)
    df['shape_dist_num'] = df_adjusted

    mask_notna = df['shape_dist_num'].notna()
    df.loc[mask_notna, 'shape_dist_traveled'] = df.loc[mask_notna, 'shape_dist_num'].apply(lambda x: f"{x:.2f}")
    df.drop(columns=['shape_dist_num'], inplace=True)
    print("✓ Trips ajustadas.\n")

def corrigir_horarios_faltantes(df, frequencies_df=None, vel_padrao_kmh=15.0):
    print("\n==============================================================================")
    print("CORRIGINDO HORÁRIOS FALTANTES (PRÉ-PROCESSAMENTO GPS)")
    print("==============================================================================\n")
    df['stop_sequence'] = pd.to_numeric(df['stop_sequence'], errors='coerce')
    
    df['sem_arrival'] = df['arrival_time'].isna() | (df['arrival_time'] == "")
    df['sem_departure'] = df['departure_time'].isna() | (df['departure_time'] == "")
    
    probs = df.groupby('trip_id')[['sem_arrival', 'sem_departure']].sum()
    trips_prob = probs[(probs['sem_arrival'] > 0) | (probs['sem_departure'] > 0)]
    
    print("DIAGNÓSTICO:")
    print(f"├─ Total de trips no GTFS: {df['trip_id'].nunique()}")
    print(f"├─ Trips com horários faltantes: {len(trips_prob)}")
    print(f"└─ Total de paradas afetadas: {probs['sem_arrival'].sum()}\n")
    
    df.drop(columns=['sem_arrival', 'sem_departure'], inplace=True)
    
    if len(trips_prob) == 0:
        print("✓ Nenhum horário faltante! Pulando correção.\n")
        return
        
    freq_lookup = {}
    if frequencies_df is not None and not frequencies_df.empty:
        # First start_time per trip
        temp = frequencies_df.groupby('trip_id')['start_time'].first()
        freq_lookup = temp.to_dict()
        print("✓ Usando frequencies.txt para horários iniciais\n")
        
    df['shape_dist_traveled_num'] = pd.to_numeric(df['shape_dist_traveled'], errors='coerce')
    
    trips_problematicas = trips_prob.index.tolist()
    
    print("PROCESSANDO CORREÇÕES... (otimizado via pandas vectorization)")
    
    # Create mask for problematic rows
    mask_prob = df['trip_id'].isin(trips_problematicas)
    df_prob = df[mask_prob].copy()
    
    # Filter trips with missing shape_dist
    invalid_shape = df_prob.groupby('trip_id')['shape_dist_traveled_num'].apply(lambda x: x.isna().all())
    trips_impossivel = invalid_shape[invalid_shape].index.tolist()
    valid_mask = ~df_prob['trip_id'].isin(trips_impossivel)
    df_prob_valid = df_prob[valid_mask].copy()

    # Calculate initial time for each valid problematic trip
    df_prob_valid['h_inicial_str'] = df_prob_valid['trip_id'].map(lambda t: freq_lookup.get(t, "00:00:00"))
    h_inicio_seg = horario_para_segundos(df_prob_valid['h_inicial_str'])
    
    # Calculate initial distance for each valid problematic trip
    primeiras_dists = df_prob_valid.sort_values(['trip_id', 'stop_sequence']).groupby('trip_id')['shape_dist_traveled_num'].transform('first')
    
    # Calculate new times
    tempo_decorrido = ((df_prob_valid['shape_dist_traveled_num'] - primeiras_dists) / 1000.0) / vel_padrao_kmh * 3600.0
    horarios_seg = h_inicio_seg + tempo_decorrido
    novos_horarios = segundos_para_horario(horarios_seg)
    
    # Assign back to original DF
    mask_arrival = df['arrival_time'].isna() | (df['arrival_time'] == "")
    mask_departure = df['departure_time'].isna() | (df['departure_time'] == "")
    
    valid_prob_mask = df['trip_id'].isin(set(df_prob_valid['trip_id']))
    
    update_arr = mask_arrival & valid_prob_mask
    update_dep = mask_departure & valid_prob_mask
    
    df.loc[update_arr, 'arrival_time'] = novos_horarios[update_arr]
    df.loc[update_dep, 'departure_time'] = novos_horarios[update_dep]
            
    df.drop(columns=['shape_dist_traveled_num'], inplace=True)
    
    trips_corrigidas = len(set(df_prob_valid['trip_id']))
    
    print("\nRESULTADO DA CORREÇÃO:")
    print(f"├─ Trips corrigidas: {trips_corrigidas}")
    print(f"└─ Trips impossíveis de corrigir (sem shape_dist): {len(trips_impossivel)}\n")

def load_calendar(path=None):
    if path is None:
        path = BASE_DADOS / "calendario.json"
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    holidays = set([datetime.strptime(hx, "%Y-%m-%d").date() for hx in data['holidays']])
    return holidays

def criar_sumario_trips(trips_df, dias_da_semana, service_id, holidays_set):
    print(f"Processando sumário para service_id: {service_id}")
    
    # Filter days of week: in python monday=0, sunday=6. R uses 1=Sun, 2=Mon, 7=Sat.
    # We will map: 
    # R: 2,3,4,5,6 -> Python: 0,1,2,3,4
    # R: 7 -> Python: 5 (Saturday)
    # R: 1 -> Python: 6 (Sunday)
    r_to_py_days = {1:6, 2:0, 3:1, 4:2, 5:3, 6:4, 7:5}
    py_dias = [r_to_py_days[d] for d in dias_da_semana]
    
    t = trips_df[trips_df['data'].dt.dayofweek.isin(py_dias)].copy()
    
    # filter bizdays if it's a weekday
    def is_bizday(d):
        if d.dayofweek in [0,1,2,3,4] and d.date() in holidays_set:
            return False
        return True
    
    t = t[t['data'].apply(is_bizday)]
    
    def adjust_time(row):
        chegada = row['datetime_chegada']
        partida = row['datetime_partida']
        diff_secs = (chegada - partida).total_seconds()
        
        if row['servico'] == '851' and row['direction_id'] == 0:
            return diff_secs + 1800
        elif row['servico'] == 'SP485' and row['direction_id'] == 0:
            return diff_secs + 1200
        else:
            return diff_secs + 360

    t['tempo_viagem_ajustado'] = t.apply(adjust_time, axis=1)
    if t.empty:
        print("  ├─ Viagens originais: 0")
        print("  ├─ Outliers removidos: 0")
        print("  └─ Viagens finais: 0\n")
        # Return empty dataframes with correct columns
        sumario = pd.DataFrame(columns=['trip_short_name', 'direction_id', 'hora', 'velocidade', 'service_id'])
        vel_media = pd.DataFrame(columns=['hora', 'velocidade_geral', 'service_id'])
        return sumario, vel_media

    # in Python, zero div produces inf, we will replace with nan later
    t['velocidade_media'] = ((t['distancia_planejada'] * 1000.0) / t['tempo_viagem_ajustado']) * 3.6
    t['velocidade_media'] = t['velocidade_media'].replace([np.inf, -np.inf], np.nan)
    
    # Remove outliers safely using transform
    if 'servico' in t.columns and 'direction_id' in t.columns:
        grp = t.groupby(['servico', 'direction_id'])['tempo_viagem_ajustado']
        q1 = grp.transform(lambda x: x.quantile(0.25))
        q3 = grp.transform(lambda x: x.quantile(0.75))
        iqr = q3 - q1
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        
        # Valid bounds
        mask_valid = (t['tempo_viagem_ajustado'] >= lower) & (t['tempo_viagem_ajustado'] <= upper)
        # If a group has too few items, quantiles might be NaN. Keep them:
        mask_keep = mask_valid | q1.isna() | q3.isna()
        t_box = t[mask_keep].copy()
    else:
        t_box = t.copy()
        
    if t_box.empty:
        t_box = t.copy() # fallback
        
    print(f"  ├─ Viagens originais: {len(t)}")
    print(f"  ├─ Outliers removidos: {len(t) - len(t_box)}")
    print(f"  └─ Viagens finais: {len(t_box)}\n")
    
    t_box['hora'] = t_box['datetime_partida'].dt.hour
    
    sumario = t_box.groupby(['servico', 'direction_id', 'hora'], as_index=False)['velocidade_media'].mean()
    sumario.rename(columns={'servico': 'trip_short_name', 'velocidade_media': 'velocidade'}, inplace=True)
    sumario['service_id'] = service_id
    
    vel_media = t_box.groupby('hora', as_index=False)['velocidade_media'].mean()
    vel_media.rename(columns={'velocidade_media': 'velocidade_geral'}, inplace=True)
    vel_media['service_id'] = service_id
    
    return sumario, vel_media

# ==============================================================================
# PROCESSAMENTO PRINCIPAL
# ==============================================================================
if __name__ == "__main__":
    t_inicio_total = time.time()
    print("\n╔════════════════════════════════════════════════════════════════════════════╗")
    print("║              PROCESSAMENTO DE GTFS COM VELOCIDADES REAIS                   ║")
    print("╚════════════════════════════════════════════════════════════════════════════╝\n")

    print("==============================================================================")
    print("ETAPA 1: CARREGANDO DADOS DE VIAGENS")
    print("==============================================================================\n")

    bases = []
    if gtfs_processar == "brt":
        path_nm = BASE_DADOS / f"viagens/{gtfs_processar}/{ano_velocidade}/{mes_velocidade}/validas"
        if not path_nm.exists():
            raise FileNotFoundError(f"Diretório de viagens BRT não encontrado: {path_nm}")
            
        files = list(path_nm.rglob("*.csv"))
        print(f"Carregando {len(files)} arquivos CSV de BRT...")
        
        if len(files) == 0:
            raise FileNotFoundError(f"Nenhum arquivo CSV encontrado em: {path_nm}")

        for f in files:
            df = pd.read_csv(f, dtype=str)
            bases.append(df)
            
        trips = pd.concat(bases, ignore_index=True)
        trips = trips[['servico', 'direction_id', 'datetime_partida', 'datetime_chegada', 'distancia_planejada', 'data']]
        trips['distancia_planejada'] = pd.to_numeric(trips['distancia_planejada'], errors='coerce') / 1000.0

    else:
        path_nm = BASE_DADOS / f"viagens/{gtfs_processar}/{ano_velocidade}/{mes_velocidade}/"
        if not path_nm.exists():
             print(f"⚠ Aviso: Diretório de viagens {gtfs_processar} não encontrado: {path_nm}")
             files = []
        else:
            files = list(path_nm.rglob("*.parquet"))
            
        print(f"Carregando {len(files)} arquivos Parquet de SPPO...")
        for f in files:
            try:
                df = pd.read_parquet(f)
                bases.append(df)
            except Exception as e:
                print(f"Failed opening {f}: {e}")
                
        # Define the subset of columns to keep globally for SPPO and Frescões
        cols_to_keep = ['servico', 'direction_id', 'datetime_partida', 'datetime_chegada', 'distancia_planejada', 'data']

        if len(bases) > 0:
            trips = pd.concat(bases, ignore_index=True)
            if 'servico_informado' in trips.columns:
                trips['servico'] = trips['servico_informado']
            if 'sentido' in trips.columns:
                def map_sentido(s):
                    if s == "I": return 0
                    if s == "V": return 1
                    if s == "C": return 0
                    return np.nan
                trips['direction_id'] = trips['sentido'].apply(map_sentido)
            
            trips = trips[[c for c in cols_to_keep if c in trips.columns]]
            
            if 'distancia_planejada' in trips.columns:
                trips['distancia_planejada'] = pd.to_numeric(trips['distancia_planejada'], errors='coerce')
            else:
                trips['distancia_planejada'] = np.nan
        else:
            trips = pd.DataFrame(columns=['servico', 'direction_id', 'datetime_partida', 'datetime_chegada', 'distancia_planejada', 'data'])
            
        path_frescao = BASE_DADOS / f"viagens/frescao/{ano_velocidade}/{mes_velocidade}/validas/"
        if path_frescao.exists():
            files_f = list(path_frescao.rglob("*.csv"))
            if files_f:
                print(f"Carregando {len(files_f)} arquivos CSV de Frescão...")
                fresc = []
                for f in files_f:
                    df_temp = pd.read_csv(f, dtype=str)
                    if 'servico' not in df_temp.columns and 'servico_informado' in df_temp.columns:
                        df_temp.rename(columns={'servico_informado': 'servico'}, inplace=True)
                    fresc.append(df_temp)
                
                df_f = pd.concat(fresc, ignore_index=True)
                df_f['distancia_planejada'] = pd.to_numeric(df_f['distancia_planejada'], errors='coerce') / 1000.0
                
                # Keep only valid columns that exist
                cols_f = [c for c in cols_to_keep if c in df_f.columns]
                df_f = df_f[cols_f]
                
                trips = pd.concat([trips, df_f], ignore_index=True)

    print(f"\n✓ Total de viagens carregadas: {len(trips)}\n")
    if 'servico' not in trips.columns:
        raise KeyError("Column 'servico' is completely missing from the trips dataframe.")
        
    trips['data'] = pd.to_datetime(trips['data'])
    trips['datetime_partida'] = pd.to_datetime(trips['datetime_partida'])
    trips['datetime_chegada'] = pd.to_datetime(trips['datetime_chegada'])

    print("==============================================================================")
    print("ETAPA 2: PROCESSANDO SUMÁRIOS DE VELOCIDADE")
    print("==============================================================================\n")
    holidays_set = load_calendar()
    
    sum_du, vel_du = criar_sumario_trips(trips, [2,3,4,5,6], "U", holidays_set)
    sum_sab, vel_sab = criar_sumario_trips(trips, [7], "S", holidays_set)
    sum_dom, vel_dom = criar_sumario_trips(trips, [1], "D", holidays_set)
    
    del trips
    
    sum_comb = pd.concat([sum_du, sum_sab, sum_dom], ignore_index=True)
    sum_comb.drop_duplicates(subset=['trip_short_name', 'direction_id', 'service_id', 'hora'], inplace=True)
    sum_comb.rename(columns={'service_id': 'service_id_join'}, inplace=True)
    sum_comb['trip_short_name'] = sum_comb['trip_short_name'].astype(str)
    sum_comb['direction_id'] = sum_comb['direction_id'].astype(float).astype(str).str.replace(".0", "", regex=False)
    
    vel_comb = pd.concat([vel_du, vel_sab, vel_dom], ignore_index=True)
    vel_comb.drop_duplicates(subset=['service_id', 'hora'], inplace=True)
    vel_comb.rename(columns={'service_id': 'service_id_join'}, inplace=True)
    
    print("✓ Sumários de velocidade processados com sucesso!\n")
    
    print("==============================================================================")
    print("ETAPA 3: PROCESSANDO GTFS")
    print("==============================================================================\n")
    print(f"Lendo GTFS: {endereco_gtfs}\n")
    
    if not endereco_gtfs.exists():
        raise FileNotFoundError(f"Arquivo GTFS não encontrado: {endereco_gtfs}")

    gtfs_data = {}
    with zipfile.ZipFile(endereco_gtfs, 'r') as z:
        for fname in ['stop_times.txt', 'trips.txt', 'routes.txt', 'frequencies.txt']:
            if fname in z.namelist():
                with z.open(fname) as f:
                    gtfs_data[fname.split('.')[0]] = pd.read_csv(f, dtype=str)
                    
    stop_times = gtfs_data['stop_times']
    trips_gtfs = gtfs_data['trips']
    routes = gtfs_data['routes']
    frequencies = gtfs_data.get('frequencies')
    
    valid_routes = trips_gtfs['route_id'].unique()
    r_antes = len(routes)
    routes = routes[routes['route_id'].isin(valid_routes)]
    print(f"✓ Rotas removidas (sem trips): {r_antes - len(routes)} | Rotas mantidas: {len(routes)}\n")
    
    ajustar_shape_dist_traveled(stop_times)
    corrigir_horarios_faltantes(stop_times, frequencies, velocidade_padrao_kmh)
    
    colunas_originais = stop_times.columns.tolist()
    
    print("==============================================================================")
    print("PREPARANDO DADOS PARA CÁLCULO DE HORÁRIOS COM GPS")
    print("==============================================================================\n")
    stp_tms = pd.merge(stop_times, trips_gtfs, on='trip_id', how='left')
    
    if frequencies is not None:
        freq_start = frequencies.groupby('trip_id')['start_time'].first().reset_index()
        freq_start.rename(columns={'start_time': 'start_time_freq'}, inplace=True)
        stp_tms = pd.merge(stp_tms, freq_start, on='trip_id', how='left')
    else:
        stp_tms['start_time_freq'] = np.nan
        
    stp_tms.sort_values(['trip_id', 'stop_sequence'], inplace=True)
    
    # Calculate initial time 
    def calc_start(grp):
        s_freq = grp['start_time_freq'].iloc[0]
        if pd.notna(s_freq) and s_freq != "":
            return s_freq
        valid_arr = grp['arrival_time'].replace("", np.nan).dropna()
        if len(valid_arr) > 0:
            return valid_arr.iloc[0]
        return np.nan

    start_calc_series = stp_tms.groupby('trip_id').apply(calc_start)
    stp_tms['start_time_calc'] = stp_tms['trip_id'].map(start_calc_series)
    
    # Normalize >= 24h
    def norm_time(x):
        if pd.isna(x) or x == "": return "00:00:00"
        parts = str(x).split(':')
        if len(parts) >= 3:
            h = int(parts[0]) % 24
            return f"{h:02d}:{parts[1]}:{parts[2]}"
        return "00:00:00"
    
    stp_tms['start_time_normal'] = stp_tms['start_time_calc'].apply(norm_time)
    stp_tms['hora'] = stp_tms['start_time_normal'].str[:2].astype(int)
    stp_tms['direction_id'] = stp_tms['direction_id'].astype(str).str.replace(".0", "", regex=False)
    
    print("==============================================================================")
    print("ETAPA 4: CALCULANDO NOVOS HORÁRIOS COM VELOCIDADES GPS")
    print("==============================================================================\n")
    
    def get_service_id_join(sid):
        if pd.isna(sid): return sid
        if sid == 'AN31': return 'D'
        return str(sid)[0]
    stp_tms['service_id_join'] = stp_tms['service_id'].apply(get_service_id_join)
    
    # Merges
    stp_tms = pd.merge(stp_tms, sum_comb, on=['trip_short_name', 'direction_id', 'service_id_join', 'hora'], how='left')
    stp_tms = pd.merge(stp_tms, vel_comb, on=['service_id_join', 'hora'], how='left')
    
    stp_tms['shape_dist_num'] = pd.to_numeric(stp_tms['shape_dist_traveled'], errors='coerce')
    has_shape = stp_tms.groupby('trip_id')['shape_dist_num'].apply(lambda x: x.notna().any())
    trips_com_shape = has_shape[has_shape].index
    
    has_gps = stp_tms['velocidade'].notna() | stp_tms['velocidade_geral'].notna()
    trips_com_gps = set(stp_tms.loc[has_gps, 'trip_id']).intersection(trips_com_shape)
    
    print(f"Trips com dados GPS e shape: {len(trips_com_gps)}")
    print(f"Trips sem dados GPS: {stp_tms['trip_id'].nunique() - len(trips_com_gps)}\n")
    
    neg_shape = stp_tms[stp_tms['shape_dist_num'] < 0]
    if len(neg_shape) > 0:
        print("⚠ ATENÇÃO: shape_dist_traveled negativo encontrado. Salvando em csv.")
        neg_shape.to_csv("shape_dist_negativos.csv", index=False)
        
    stp_tms['recalcular_gps'] = stp_tms['trip_id'].isin(trips_com_gps)
    
    def calc_vel(row):
        if pd.notna(row['velocidade']): return row['velocidade']/3.6
        if pd.notna(row['velocidade_geral']): return row['velocidade_geral']/3.6
        return 15.0/3.6
        
    stp_tms['velocidade_seg'] = stp_tms.apply(calc_vel, axis=1)
    
    mask_recalc = stp_tms['recalcular_gps']
    tempo_viag = stp_tms.loc[mask_recalc, 'shape_dist_num'] / stp_tms.loc[mask_recalc, 'velocidade_seg']
    
    inicio_seg = horario_para_segundos(stp_tms['start_time_calc'])
    
    # Recreate arrivals & departures for recalculated
    novos = segundos_para_horario(inicio_seg[mask_recalc] + tempo_viag)
    stp_tms.loc[mask_recalc, 'arrival_time'] = novos
    stp_tms.loc[mask_recalc, 'departure_time'] = novos
    print("✓ Horários GPS calculados com sucesso!\n")
    
    print("Garantindo monotonicidade dos horários...")
    stp_tms.sort_values(['trip_id', 'stop_sequence'], inplace=True)
    
    arr_seg = horario_para_segundos(stp_tms['arrival_time'])
    dep_seg = horario_para_segundos(stp_tms['departure_time'])
    
    dep_seg = np.maximum(dep_seg, arr_seg)
    dep_seg_series = pd.Series(dep_seg, index=stp_tms.index)
    dep_seg_cummax = dep_seg_series.groupby(stp_tms['trip_id']).cummax()
    
    arr_seg_series = pd.Series(arr_seg, index=stp_tms.index)
    prev_dep = dep_seg_cummax.groupby(stp_tms['trip_id']).shift(1)
    arr_final = np.maximum(arr_seg_series, prev_dep.fillna(0))
    # exception: if we fillna(0) we might ruin early times. Let's do:
    arr_final = arr_seg_series.mask(arr_seg_series < prev_dep, prev_dep)
    
    dep_final = np.maximum(dep_seg_cummax, arr_final)
    
    stp_tms['arrival_time'] = segundos_para_horario(arr_final).fillna(stp_tms['arrival_time'])
    stp_tms['departure_time'] = segundos_para_horario(dep_final).fillna(stp_tms['departure_time'])
    print("✓ Monotonicidade garantida\n")
    
    print("==============================================================================")
    print("ETAPA 5: VERIFICANDO INTEGRIDADE DOS HORÁRIOS")
    print("==============================================================================\n")
    
    mask_inv = stp_tms['arrival_time'].isna() | (stp_tms['arrival_time'] == "") | stp_tms['departure_time'].isna() | (stp_tms['departure_time'] == "")
    if mask_inv.any():
        incons_df = stp_tms.loc[mask_inv, ['trip_id', 'stop_sequence', 'stop_id', 'arrival_time', 'departure_time']]
        print("⚠ FORAM ENCONTRADOS HORÁRIOS AUSENTES OU INVÁLIDOS!")
        print(f"Total: {len(incons_df)}")
        fpath = f"relatorio_inconsistentes_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        incons_df.to_csv(fpath, index=False)
        raise ValueError("Processamento interrompido devido a inconsistências. Verifique " + fpath)
        
    print("✓ Nenhum horário ausente ou inválido encontrado.\n")

    print("==============================================================================")
    print("ETAPA 6: SALVANDO GTFS PROCESSADO")
    print("==============================================================================\n")
    
    # keep original columns
    colunas_finais = [c for c in colunas_originais if c in stp_tms.columns]
    stop_times_final = stp_tms[colunas_finais]
    
    endereco_gtfs_proc = BASE_DADOS / f"gtfs/{ano_gtfs}/{gtfs_processar}_{ano_gtfs}-{mes_gtfs}-{quinzena_gtfs}Q_PROC.zip"
    
    print(f"Salvando em: {Path(endereco_gtfs_proc).resolve()}")
    
    # We must write back into a new ZIP.
    # We will copy all files from the original ZIP except stop_times and routes, which we will replace.
    with zipfile.ZipFile(endereco_gtfs, 'r') as zin, zipfile.ZipFile(endereco_gtfs_proc, 'w', compression=zipfile.ZIP_DEFLATED) as zout:
        for item in zin.infolist():
            if item.filename == 'stop_times.txt':
                zout.writestr('stop_times.txt', stop_times_final.to_csv(index=False))
            elif item.filename == 'routes.txt':
                zout.writestr('routes.txt', routes.to_csv(index=False))
            else:
                zout.writestr(item, zin.read(item.filename))

    t_total = time.time() - t_inicio_total
    print(f"\nPROCESSAMENTO FINALIZADO! Tempo total: {t_total:.1f} segundos\n")
