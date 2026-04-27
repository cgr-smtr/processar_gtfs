import pandas as pd
import numpy as np
import zipfile
import io
import os
import time
from pathlib import Path
import warnings

warnings.filterwarnings('ignore', category=pd.errors.DtypeWarning)

# ==============================================================================
# CONFIGURAÇÕES
# ==============================================================================
BASE_DADOS = Path("C:/R_SMTR/dados")

ano_gtfs      = "2026"
mes_gtfs      = "05"
estudo_gtfs = "01" #ESTUDO, NÃO CONSIDERAR MAIS QUINZENA!!!!
sufixo        = f"{ano_gtfs}-{mes_gtfs}-{estudo_gtfs}Q"

endereco_sppo       = BASE_DADOS / f"gtfs/{ano_gtfs}/sppo_{sufixo}_PROC.zip"
endereco_brt        = BASE_DADOS / f"gtfs/{ano_gtfs}/brt_{sufixo}_PROC.zip"
endereco_gtfs_combi = BASE_DADOS / f"gtfs/{ano_gtfs}/gtfs_combi_{sufixo}.zip"

pasta_substituicao_combi = BASE_DADOS / "insumos/gtfs_combi"
pasta_substituicao_pub   = BASE_DADOS / "insumos/gtfs_pub"

# ==============================================================================
# FUNÇÕES AUXILIARES
# ==============================================================================
def log_msg(msg):
    # Formats similar to R's [%H:%M:%S] msg
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

def atualizar_cores_gtfs(gtfs_dict, caminho_cores):
    if not os.path.exists(caminho_cores):
        raise FileNotFoundError(f"Arquivo de cores não encontrado: {caminho_cores}")
    
    cores = pd.read_csv(caminho_cores, dtype=str)
    
    needed = ['route_short_name', 'route_color', 'route_text_color']
    for c in needed:
        if c not in cores.columns:
            raise KeyError(f"O arquivo de cores deve conter as colunas: {needed}")
            
    # duplicate drops
    cores = cores.drop_duplicates(subset=['route_short_name'], keep='first')
    
    df_routes = gtfs_dict.get('routes', pd.DataFrame())
    if df_routes.empty: return gtfs_dict
    
    # Save original colors for 702
    cores_702 = df_routes[df_routes['route_type'] == '702'][['route_id', 'route_short_name', 'route_color', 'route_text_color']].copy()
    
    # Reset colors for non-702
    mask_not_702 = df_routes['route_type'] != '702'
    df_routes.loc[mask_not_702, ['route_color', 'route_text_color']] = np.nan
    
    # Merge
    df_merged = df_routes.merge(cores, on='route_short_name', how='left', suffixes=('', '_csv'))
    
    mask_has_csv = df_merged['route_color_csv'].notna()
    df_merged.loc[mask_has_csv, 'route_color'] = df_merged.loc[mask_has_csv, 'route_color_csv']
    df_merged.loc[mask_has_csv, 'route_text_color'] = df_merged.loc[mask_has_csv, 'route_text_color_csv']
    
    df_merged.drop(columns=['route_color_csv', 'route_text_color_csv'], inplace=True, errors='ignore')
    
    # Restore 702
    df_merged.set_index('route_id', inplace=True)
    cores_702.set_index('route_id', inplace=True)
    df_merged.update(cores_702[['route_color', 'route_text_color']])
    df_merged.reset_index(inplace=True)
    
    # Fix 200 colors
    mask_200 = df_merged['route_type'] == '200'
    df_merged.loc[mask_200, 'route_color'] = "030478"
    df_merged.loc[mask_200, 'route_text_color'] = "FFFFFF"
    
    gtfs_dict['routes'] = df_merged
    return gtfs_dict

def substituir_arquivos_gtfs(caminho_zip, pasta_origem, arquivos=["calendar_dates.txt", "fare_attributes.txt", "fare_rules.txt", "feed_info.txt"]):
    if not os.path.exists(caminho_zip):
        raise FileNotFoundError(f"Arquivo ZIP não encontrado: {caminho_zip}")
    if not os.path.exists(pasta_origem):
        raise FileNotFoundError(f"Pasta de origem não encontrada: {pasta_origem}")
        
    log_msg(f"Substituindo arquivos em {os.path.basename(caminho_zip)} usando {pasta_origem}")
    
    # Read existing zip to a dictionary mapping filenames to content
    zin_data = {}
    with zipfile.ZipFile(caminho_zip, 'r') as zin:
        for item in zin.infolist():
            zin_data[item.filename] = zin.read(item.filename)
            
    # Replace contents from pasta_origem
    for arq in arquivos:
        orig = os.path.join(pasta_origem, arq)
        if os.path.exists(orig):
            with open(orig, 'rb') as f:
                zin_data[arq] = f.read()
            log_msg(f"  ✔ Substituído: {arq}")
        else:
            log_msg(f"  ⚠ Arquivo {arq} não encontrado na origem, mantido o original")
            
    # Write back
    caminho_temp = caminho_zip.with_name(caminho_zip.name.replace('.zip', '_TEMP.zip'))
    with zipfile.ZipFile(caminho_temp, 'w', compression=zipfile.ZIP_DEFLATED) as zout:
        for fname, content in zin_data.items():
            zout.writestr(fname, content)
            
    os.replace(caminho_temp, caminho_zip)
    log_msg(f"  ✓ Arquivo {os.path.basename(caminho_zip)} recriado com sucesso")

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

# ==============================================================================
# PROCESSAMENTO PRINCIPAL
# ==============================================================================
print("\n╔════════════════════════════════════════════════════════════════════════════╗")
print("║                    COMBINAÇÃO DE GTFS - SPPO + BRT                         ║")
print("╚════════════════════════════════════════════════════════════════════════════╝\n")

tempo_inicio = time.time()

# ----------------- 1. CARREGAR E PROCESSAR SPPO -----------------
if not os.path.exists(endereco_sppo):
    raise FileNotFoundError(f"Arquivo SPPO não encontrado: {endereco_sppo}")

gtfs_sppo = read_gtfs(endereco_sppo)
log_msg("Processando SPPO...")

# routes SPPO
df_rs = gtfs_sppo['routes']
df_rs['numero'] = df_rs['route_short_name'].str.extract(r'([0-9]+)').fillna(0).astype(int)
df_rs['route_type'] = '700'
df_rs.loc[df_rs['numero'] > 1000, 'route_type'] = '200'
df_rs.loc[df_rs['route_short_name'].isin(["LECD124", "LECD125"]), 'route_type'] = '200'
df_rs.drop(columns=['numero'], inplace=True)

# adjust services
gtfs_sppo['trips'] = ajustar_service_id(gtfs_sppo['trips'])
if 'calendar' in gtfs_sppo: gtfs_sppo['calendar'] = ajustar_service_id(gtfs_sppo['calendar'])

# Verify blank schedules
df_st_sppo = gtfs_sppo['stop_times']
mask_vazios = df_st_sppo['arrival_time'].isna() | (df_st_sppo['arrival_time'] == "") | df_st_sppo['departure_time'].isna() | (df_st_sppo['departure_time'] == "")
stops_vazios_sppo = df_st_sppo[mask_vazios]

if not stops_vazios_sppo.empty:
    log_msg(f"⚠️  SPPO tem {len(stops_vazios_sppo)} stops com horários vazios!")
    raise ValueError("⛔ SPPO processado está com horários vazios.")

trips_com_st = df_st_sppo['trip_id'].unique()
trips_excep = gtfs_sppo['trips'][gtfs_sppo['trips']['service_id'] == "EXCEP"]['trip_id'].unique()
trips_manter = np.unique(np.concatenate([trips_com_st, trips_excep]))

path_fantasmas = BASE_DADOS / "insumos/trip_id_fantasma.txt"
if os.path.exists(path_fantasmas):
    trips_fantasma = pd.read_csv(path_fantasmas, header=None).iloc[:, 0].tolist()
else:
    trips_fantasma = []

trips_final_sppo = set(trips_manter) - set(trips_fantasma)

# Filter trips
gtfs_sppo['trips'] = gtfs_sppo['trips'][gtfs_sppo['trips']['trip_id'].isin(trips_final_sppo)]
# Call clean_gtfs to cascade removal of unused stop_times, routes, shapes, etc
gtfs_sppo = clean_gtfs(gtfs_sppo)

log_msg(f"✓ SPPO processado — {len(gtfs_sppo['routes'])} rotas, {len(gtfs_sppo['trips'])} trips")


# ----------------- 2. CARREGAR E PROCESSAR BRT -----------------
if not os.path.exists(endereco_brt):
    raise FileNotFoundError(f"Arquivo BRT não encontrado: {endereco_brt}")

gtfs_brt = read_gtfs(endereco_brt)
log_msg("Processando BRT...")

df_rb = gtfs_brt['routes']
# Preserva o route_type original do GTFS BRT
if 'route_type' not in df_rb.columns:
    df_rb['route_type'] = np.nan

routes_usar_brt = gtfs_brt['trips']['trip_short_name'].unique()
gtfs_brt['routes'] = gtfs_brt['routes'][gtfs_brt['routes']['route_short_name'].isin(routes_usar_brt)]

gtfs_brt['trips'] = ajustar_service_id(gtfs_brt['trips'])
if 'calendar' in gtfs_brt: gtfs_brt['calendar'] = ajustar_service_id(gtfs_brt['calendar'])
if 'feed_info' in gtfs_brt: gtfs_brt['feed_info'] = pd.DataFrame(columns=gtfs_brt['feed_info'].columns) # clear feed_info

df_st_brt = gtfs_brt['stop_times']
mask_vazios_brt = df_st_brt['arrival_time'].isna() | (df_st_brt['arrival_time'] == "") | df_st_brt['departure_time'].isna() | (df_st_brt['departure_time'] == "")
stops_vazios_brt = df_st_brt[mask_vazios_brt]

if not stops_vazios_brt.empty:
    log_msg(f"⚠️  BRT tem {len(stops_vazios_brt)} stops com horários vazios!")
    raise ValueError("⛔ BRT processado está com horários vazios.")

gtfs_brt = clean_gtfs(gtfs_brt)

log_msg(f"✓ BRT processado — {len(gtfs_brt['routes'])} rotas, {len(gtfs_brt['trips'])} trips")
write_gtfs(gtfs_brt, endereco_brt)


# ----------------- 3. COMBINAR GTFS -----------------
log_msg("Combinando GTFS SPPO + BRT...")
gtfs_combi = {}

all_keys = set(list(gtfs_sppo.keys()) + list(gtfs_brt.keys()))
for k in all_keys:
    dfs = []
    if k in gtfs_sppo and not gtfs_sppo[k].empty: dfs.append(gtfs_sppo[k])
    if k in gtfs_brt and not gtfs_brt[k].empty: dfs.append(gtfs_brt[k])
    
    if dfs:
        # concatenate and drop strict duplicates
        gtfs_combi[k] = pd.concat(dfs, ignore_index=True).drop_duplicates()
    else:
        gtfs_combi[k] = pd.DataFrame()

log_msg("✓ GTFS combinados")


# ----------------- 4. LIMPEZA E AJUSTES DO GTFS COMBINADO -----------------
log_msg("Aplicando limpezas e ajustes...")

if 'stops' in gtfs_combi:
    df_stops = gtfs_combi['stops']
    pontos_apagar = df_stops[df_stops['stop_name'] == "APAGAR"]['stop_id'].tolist()
    # keeping unique stop_ids and not in pontos_apagar
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

if 'calendar' in gtfs_combi: gtfs_combi['calendar'].drop_duplicates(inplace=True)
if 'calendar_dates' in gtfs_combi: gtfs_combi['calendar_dates'].drop_duplicates(inplace=True)

if 'stop_times' in gtfs_combi:
    df_st = gtfs_combi['stop_times']
    df_st = remover_colunas(df_st, ["pickup_type", "drop_off_type", "continuous_pickup", "continuous_drop_off"])
    df_st['timepoint'] = '0'
    
    if 'shape_dist_traveled' in df_st.columns:
        df_st['shape_dist_traveled'] = pd.to_numeric(df_st['shape_dist_traveled'], errors='coerce').round(2)
        
    df_st = df_st[~df_st['stop_id'].isin(pontos_apagar)]
    gtfs_combi['stop_times'] = df_st

if 'fare_attributes' in gtfs_combi:
    gtfs_combi['fare_attributes']['currency_type'] = "BRL"

log_msg("✓ Limpezas aplicadas")

log_msg("Verificando e ordenando shapes...")
if 'shapes' in gtfs_combi and not gtfs_combi['shapes'].empty:
    df_sh = gtfs_combi['shapes']
    df_sh['shape_pt_sequence'] = pd.to_numeric(df_sh['shape_pt_sequence'], errors='coerce').fillna(0).astype(int)
    df_sh.sort_values(by=['shape_id', 'shape_pt_sequence'], inplace=True)
    
    # Check invalid shapes
    shape_counts = df_sh['shape_id'].value_counts()
    shapes_invalidos = shape_counts[shape_counts < 2].index.tolist()
    if shapes_invalidos:
        log_msg(f"⚠️  Encontrados {len(shapes_invalidos)} shapes com menos de 2 pontos.")
    else:
        log_msg("✓ Todos os shapes possuem pelo menos 2 pontos e estão ordenados.")
    gtfs_combi['shapes'] = df_sh

log_msg("Validando horários no GTFS combinado...")
df_st_final = gtfs_combi.get('stop_times', pd.DataFrame())
if not df_st_final.empty:
    mask = df_st_final['arrival_time'].isna() | (df_st_final['arrival_time'] == "") | df_st_final['departure_time'].isna() | (df_st_final['departure_time'] == "")
    if mask.any():
        raise ValueError("⛔ ERRO: GTFS combinado tem stops com horários vazios!")
log_msg(f"✓ Todos os {len(df_st_final)} stops têm horários válidos")

# Cleanup final combi to guarantee no orphaned records due to explicit point deletions
gtfs_combi = clean_gtfs(gtfs_combi)

# ==============================================================================
# 5. SALVAMENTO GTFS COMBI e SUBSTITUIÇÃO
# ==============================================================================
log_msg("═══════════════════════════════════════════════════════════════")
log_msg("ESTATÍSTICAS DO GTFS COMBINADO:")
for k, v in gtfs_combi.items():
    if k == 'shapes':
        log_msg(f"  ├─ Shapes únicos:   {v['shape_id'].nunique()}")
    else:
        log_msg(f"  ├─ {k}:           {len(v)}")
log_msg("═══════════════════════════════════════════════════════════════")

write_gtfs(gtfs_combi, endereco_gtfs_combi)
log_msg("✓ Arquivos GTFS salvos com sucesso")

substituir_arquivos_gtfs(endereco_gtfs_combi, pasta_substituicao_combi)

# ==============================================================================
# FILTRAGEM FINAL E APLICAÇÃO DE CORES
# ==============================================================================
gtfs_pub = dict(gtfs_combi)

log_msg("Removendo viagens com service_id EXCEP...")
if 'trips' in gtfs_pub:
    trips_pub = gtfs_pub['trips']
    trips_excep = trips_pub[trips_pub['service_id'] == "EXCEP"]['trip_id'].unique()
    gtfs_pub['trips'] = trips_pub[trips_pub['service_id'] != "EXCEP"]
    
    # Clean up downstream associations
    gtfs_pub = clean_gtfs(gtfs_pub)

log_msg("Aplicando cores personalizadas às rotas...")
gtfs_pub = atualizar_cores_gtfs(gtfs_pub, BASE_DADOS / "insumos/gtfs_cores.csv")

log_msg("Salvando GTFS público final...")
caminho_gtfs_pub = BASE_DADOS / f"gtfs/{ano_gtfs}/gtfs_rio-de-janeiro_pub.zip"
write_gtfs(gtfs_pub, caminho_gtfs_pub)

substituir_arquivos_gtfs(caminho_gtfs_pub, pasta_substituicao_pub)

# ==============================================================================
# FINALIZAÇÃO
# ==============================================================================
tempo_total = time.time() - tempo_inicio
print("\n╔════════════════════════════════════════════════════════════════════════════╗")
print("║                       PROCESSAMENTO FINALIZADO!                            ║")
print(f"║                    Tempo total: {tempo_total:.1f} segundos                             ║")
print("╚════════════════════════════════════════════════════════════════════════════╝\n")
