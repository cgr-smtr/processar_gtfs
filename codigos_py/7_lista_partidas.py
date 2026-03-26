import pandas as pd
import numpy as np
import zipfile
import os
import time

# ============================================================================
# CONFIGURAÇÕES INICIAIS
# ============================================================================
ano_gtfs = "2026"
endereco_gtfs = f"../../dados/gtfs/{ano_gtfs}/gtfs_rio-de-janeiro_pub.zip"
tipos_dia = ['du', 'sab', 'dom']

# FILTRO DE LINHAS A EXCLUIR
linhas_excluir = []

# Pasta de saída
from pathlib import Path
pasta_saida = (Path(__file__).resolve().parent / "../../../resultados/partidas").resolve()
pasta_saida.mkdir(parents=True, exist_ok=True)

print("\n==========================================================")
print("INICIANDO PROCESSAMENTO DE PARTIDAS")
print("==========================================================\n")

# ============================================================================
# LEITURA DO GTFS
# ============================================================================
print("ETAPA 1: Lendo GTFS base...")
if not os.path.exists(endereco_gtfs):
    raise FileNotFoundError(f"Arquivo GTFS não encontrado: {endereco_gtfs}")

gtfs = {}
with zipfile.ZipFile(endereco_gtfs, 'r') as z:
    for fname in ['routes.txt', 'trips.txt', 'stop_times.txt', 'frequencies.txt', 'agency.txt']:
        if fname in z.namelist():
            with z.open(fname) as f:
                gtfs[fname.split('.')[0]] = pd.read_csv(f, dtype=str)

df_routes = gtfs['routes']
df_trips = gtfs['trips']
df_st = gtfs['stop_times']
df_freq = gtfs.get('frequencies', pd.DataFrame())
df_agency = gtfs.get('agency', pd.DataFrame())

print(f"  ✓ GTFS carregado: {len(df_routes)} rotas, {len(df_trips)} trips")

# Filtrar frescões
frescoes = df_routes[df_routes['route_type'] == '200']['route_id'].tolist()
df_trips = df_trips[~df_trips['route_id'].isin(frescoes)]

path_fantasmas = "../../dados/insumos/trip_id_fantasma.txt"
if os.path.exists(path_fantasmas):
    trips_fantasma = pd.read_csv(path_fantasmas, header=None).iloc[:, 0].tolist()
else:
    trips_fantasma = []

# ============================================================================
# FUNÇÕES DE HORÁRIO E INTERVALO
# ============================================================================
def get_pattern(tipo_dia):
    if tipo_dia == 'du': return 'U'
    if tipo_dia == 'sab': return 'S'
    if tipo_dia == 'dom': return 'D'
    return ''

def horario_to_timedelta(hk_str):
    if pd.isna(hk_str) or hk_str == '': return pd.NaT
    # handles hour >= 24
    parts = str(hk_str).split(':')
    if len(parts) != 3: return pd.NaT
    return pd.Timedelta(hours=int(parts[0]), minutes=int(parts[1]), seconds=int(parts[2]))

def timedelta_to_horario(td):
    if pd.isna(td): return "--"
    total_seconds = int(td.total_seconds())
    h = total_seconds // 3600
    m = (total_seconds % 3600) // 60
    s = total_seconds % 60
    return f"{h:02d}:{m:02d}:{s:02d}"

# Merge de extensoes - since we can't guarantee 6 generated gpkgs are ready globally we approximate shapes or use 0
print("\nAviso: As extensões exatas (shapes) exigem EPSG:31983 processado. Simplificando via coluna shape_dist_traveled se existir.")
extensoes = {}
if 'shape_dist_traveled' in df_st.columns:
    df_st['shape_dist_num'] = pd.to_numeric(df_st['shape_dist_traveled'], errors='coerce')
    df_st_max = df_st.groupby('trip_id')['shape_dist_num'].max().reset_index()
    df_st_max.rename(columns={'shape_dist_num': 'extensao'}, inplace=True)
    
    df_trips_ext = df_trips.merge(df_st_max, on='trip_id', how='left')
    df_trips_ext['extensao'] = df_trips_ext['extensao'].fillna(0).astype(int)
    
    # Average grouped extension per direction to match the script's intention:
    extensoes_df = df_trips_ext.groupby(['trip_short_name', 'direction_id', 'route_id'])['extensao'].max().reset_index()
else:
    extensoes_df = pd.DataFrame(columns=['trip_short_name', 'direction_id', 'route_id', 'extensao'])

consolidado_lista = []

# ============================================================================
# LOOP POR TIPO DE DIA
# ============================================================================
for current_tipo_dia in tipos_dia:
    print(f"\n==========================================================")
    print(f"PROCESSANDO TIPO DE DIA: {current_tipo_dia.upper()}")
    print(f"==========================================================")
    
    pattern = get_pattern(current_tipo_dia)
    
    # Filtrar viagens do dia
    mask_day = df_trips['service_id'].notna() & df_trips['service_id'].str.contains(pattern)
    mask_desat = df_trips['service_id'].notna() & df_trips['service_id'].str.contains('DESAT')
    mask_fant = df_trips['trip_id'].isin(trips_fantasma)
    
    trips_dia = df_trips[mask_day & ~mask_desat & ~mask_fant].copy()
    
    # FREQUENCIAS 
    viagens_freq_exp = pd.DataFrame()
    if not df_freq.empty:
        df_freq_dia = df_freq[df_freq['trip_id'].isin(trips_dia['trip_id'])].copy()
        if not df_freq_dia.empty:
            df_freq_dia = df_freq_dia.merge(trips_dia[['trip_id', 'trip_short_name', 'trip_headsign', 'direction_id', 'route_id']], on='trip_id')
            
            df_freq_dia['start_td'] = df_freq_dia['start_time'].apply(horario_to_timedelta)
            df_freq_dia['end_td'] = df_freq_dia['end_time'].apply(horario_to_timedelta)
            df_freq_dia['headway_secs'] = pd.to_numeric(df_freq_dia['headway_secs'], errors='coerce').fillna(0).astype(int)
            
            exploded_rows = []
            for _, row in df_freq_dia.iterrows():
                st = row['start_td']
                ed = row['end_td']
                hw = pd.Timedelta(seconds=row['headway_secs'])
                
                if pd.isna(st) or pd.isna(ed) or hw.total_seconds() <= 0: continue
                
                # Gera as partidas 
                current_time = st
                while current_time < ed:
                    exploded_rows.append({
                        'trip_short_name': row['trip_short_name'],
                        'trip_headsign': row['trip_headsign'],
                        'start_time': current_time,
                        'direction_id': row['direction_id'],
                        'route_id': row['route_id']
                    })
                    current_time += hw
            
            viagens_freq_exp = pd.DataFrame(exploded_rows)
            print(f"  ✓ Viagens por frequência expandidas: {len(viagens_freq_exp)}")

    # REGULARES (QH)
    linhas_freq = viagens_freq_exp['trip_short_name'].unique() if not viagens_freq_exp.empty else []
    
    # Pega apenas o primeiro ponto de cada trip_id para departure time das linhas n\u00e3o frequentes
    df_st_first = df_st[df_st['stop_sequence'] == '0'][['trip_id', 'departure_time']]
    df_trips_reg = trips_dia[~trips_dia['trip_short_name'].isin(linhas_freq)]
    
    viagens_qh_regular = df_st_first.merge(df_trips_reg, on='trip_id', how='inner')
    viagens_qh_regular['start_time'] = viagens_qh_regular['departure_time'].apply(horario_to_timedelta)
    viagens_qh_regular = viagens_qh_regular[['trip_short_name', 'trip_headsign', 'direction_id', 'start_time', 'route_id']]
    
    print(f"  ✓ Viagens regulares processadas: {len(viagens_qh_regular)}")
    
    # CONSOLIDAR
    viagens_completo = pd.concat([viagens_freq_exp, viagens_qh_regular], ignore_index=True)
    viagens_completo = viagens_completo[~viagens_completo['trip_short_name'].isin(linhas_excluir)]
    
    if viagens_completo.empty:
        print("Nenhuma viagem processada para este dia.")
        continue
        
    # Formatação 
    def format_departure(td):
        if pd.isna(td): return ""
        ts = int(td.total_seconds())
        h = ts // 3600
        m = (ts % 3600) // 60
        s = ts % 60
        return f"{h:02d}:{m:02d}:{s:02d}"
        
    viagens_completo['departure_time'] = viagens_completo['start_time'].apply(format_departure)
    viagens_completo['tipo_dia'] = current_tipo_dia
    
    # Merge com routes e extensoes
    viagens_completo = viagens_completo.merge(df_routes[['route_id', 'route_long_name', 'route_type', 'agency_id']], on='route_id', how='left')
    
    if not df_agency.empty:
        viagens_completo = viagens_completo.merge(df_agency[['agency_id', 'agency_name']], on='agency_id', how='left')
    else:
        viagens_completo['agency_name'] = ""
        
    if not extensoes_df.empty:
        viagens_completo = viagens_completo.merge(extensoes_df, on=['trip_short_name', 'direction_id', 'route_id'], how='left')
    else:
        viagens_completo['extensao'] = 0
        
    # Calcular faixas
    def get_faixa(td):
        if pd.isna(td): return ""
        horas = int(td.total_seconds()) // 3600
        if horas < 1: return "00:00-01:00"
        if horas < 2: return "01:00-02:00"
        if horas < 3: return "02:00-03:00"
        if horas < 4: return "03:00-04:00"
        if horas < 5: return "04:00-05:00"
        if horas < 6: return "05:00-06:00"
        if horas < 9: return "06:00-09:00"
        if horas < 12: return "09:00-12:00"
        if horas < 15: return "12:00-15:00"
        if horas < 18: return "15:00-18:00"
        if horas < 21: return "18:00-21:00"
        if horas < 22: return "21:00-22:00"
        if horas < 23: return "22:00-23:00"
        if horas < 24: return "23:00-24:00"
        return "24:00+"
        
    viagens_completo['faixa'] = viagens_completo['start_time'].apply(get_faixa)
    
    col_order = ['trip_short_name', 'route_long_name', 'trip_headsign', 'direction_id', 'departure_time', 'faixa', 'agency_name', 'extensao', 'route_type', 'tipo_dia']
    
    viagens_c = viagens_completo[col_order].copy()
    viagens_c.sort_values(by=['trip_short_name', 'direction_id', 'departure_time'], inplace=True)
    
    # Criar campos para arquivo individual  
    viagens_ind = viagens_c.drop(columns=['route_long_name']).copy()
    
    # Calcular intervalos individual
    viagens_ind['departure_td'] = viagens_ind['departure_time'].apply(horario_to_timedelta)
    viagens_ind['interval_td'] = viagens_ind.groupby(['trip_short_name', 'direction_id'])['departure_td'].diff()
    viagens_ind['intervalo'] = viagens_ind['interval_td'].apply(timedelta_to_horario)
    viagens_ind.drop(columns=['departure_td', 'interval_td'], inplace=True)
    
    # Ordenar colunas colocando intervalo no final
    cols_ind = [c for c in viagens_ind.columns if c != 'intervalo'] + ['intervalo']
    viagens_ind = viagens_ind[cols_ind]
    
    # Salvar
    arq_csv = os.path.join(pasta_saida, f"partidas_{current_tipo_dia}.csv")
    viagens_ind.to_csv(arq_csv, index=False)
    print(f"  ✓ Arquivo salvo: partidas_{current_tipo_dia}.csv")
    
    consolidado_lista.append(viagens_c)

print("\n==========================================================")
print("SALVANDO RESULTADO FINAL")
print("==========================================================\n")

if consolidado_lista:
    df_final = pd.concat(consolidado_lista, ignore_index=True)
    
    # Parquet on Windows (pyarrow/fastparquet) throws [Errno 22] Invalid argument when allocating memory
    # for massive mixed-type object columns (like str + NaN + Timedeltas). Coercing all objects to strict string.
    for col in df_final.columns:
        if df_final[col].dtype == 'object' or pd.api.types.is_timedelta64_dtype(df_final[col]):
            df_final[col] = df_final[col].fillna("").astype(str)
            
    # Salvando em Parquet como consolidado principal e CSV total
    from pathlib import Path
    pasta_out = Path(pasta_saida).resolve()
    
    csv_path = str(pasta_out / "partidas.csv")
    parq_path = str(pasta_out / "partidas.parquet")
    
    df_final.to_csv(csv_path, index=False)
    # Requer pyarrow
    with open(parq_path, 'wb') as f:
        df_final.to_parquet(f, engine='pyarrow', index=False)
        
    print("✓ Arquivos finais salvos: partidas.csv e partidas.parquet")
    
    print(f"✓ Total de linhas processadas: {len(df_final)}")
else:
    print("Nenhuma partida compilada ao final do script.")

print("==========================================================")
print("PROCESSAMENTO COMPLETO FINALIZADO!")
print("==========================================================")
