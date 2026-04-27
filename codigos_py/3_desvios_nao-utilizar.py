import pandas as pd
import numpy as np
import zipfile
import io
import os
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta
from pathlib import Path
import warnings

warnings.filterwarnings('ignore', category=pd.errors.DtypeWarning)

# ==============================================================================
# CONFIGURATIONS
# ==============================================================================
BASE_DADOS = Path("C:/R_SMTR/dados")

ano_gtfs = "2026"  # Changed to 2026 as per the previous script testing
mes_gtfs = "03"
quinzena_gtfs = "04"

endereco_gtfs = BASE_DADOS / f"gtfs/{ano_gtfs}/sppo_{ano_gtfs}-{mes_gtfs}-{quinzena_gtfs}Q_PROC.zip"

desvios_tabela_id = "1QYSf_E7HrDcSDVVaF_KrolS-LRL3kTF5WnhQGh3RMy0"

# Service Account JSON file path (user needs to provide this)
SERVICE_ACCOUNT_FILE = 'credentials.json'

print("\n╔════════════════════════════════════════════════════════════════════════════╗")
print("║              PROCESSAMENTO DE GTFS - AJUSTE DE DESVIOS (CALENDÁRIO)        ║")
print("╚════════════════════════════════════════════════════════════════════════════╝\n")

# ==============================================================================
# CARREGAR DADOS DOS CSVs (INSUMOS)
# ==============================================================================
path_linhas = Path("insumos_desvios/linhas_desvios.csv")
path_desc = Path("insumos_desvios/descricao_desvios.csv")

if not path_linhas.exists() or not path_desc.exists():
    raise FileNotFoundError("As planilhas CSV não foram encontradas na pasta 'insumos_desvios/'.")

# Read 'linhas_desvios'
tabela_desvios = pd.read_csv(path_linhas)

# Read 'descricao_desvios'
descricao_desvios = pd.read_csv(path_desc)

print(f"Linhas desvios carregadas: {len(tabela_desvios)} registros.")
print(f"Descrições carregadas: {len(descricao_desvios)} registros.")

# Format dates
descricao_desvios['data_inicio'] = pd.to_datetime(descricao_desvios['data_inicio'], format="%Y-%m-%d", errors="coerce").dt.date
descricao_desvios['data_fim'] = pd.to_datetime(descricao_desvios['data_fim'], format="%Y-%m-%d", errors="coerce").dt.date

hoje = datetime.now().date()

# Filter descriptions
descricao_desvios = descricao_desvios[(descricao_desvios['data_inicio'] < hoje) & (descricao_desvios['data_fim'] > hoje)].copy()
print(f"Descrições ativas (data_inicio < hoje < data_fim): {len(descricao_desvios)}")

if descricao_desvios.empty:
    print("Nenhum desvio ativo encontrado.")
    # Script would normally exit or skip, but we follow the R logic.
    desvios = ['servico']
else:
    desvios = ['servico'] + descricao_desvios['cod_desvio'].tolist()

# Keep only columns that exist
cols_to_keep = [c for c in desvios if c in tabela_desvios.columns]
tabela_desvios = tabela_desvios[cols_to_keep]

# Identify logical columns (boolean/checkboxes) corresponding to active detours
logical_cols = [c for c in cols_to_keep if c != 'servico']
# In Google Sheets, checkboxes might be 'TRUE'/'FALSE' strings or booleans or 'VERDADEIRO'. Let's normalize.
for c in logical_cols:
    tabela_desvios[c] = tabela_desvios[c].astype(str).str.upper().map({'TRUE': True, 'VERDADEIRO': True, '1': True}).fillna(False)

# find affected routes
if logical_cols:
    linhas_afetadas_desvios = tabela_desvios[tabela_desvios[logical_cols].any(axis=1)]['servico'].tolist()
else:
    linhas_afetadas_desvios = []

# ==============================================================================
# LOAD GTFS FILES
# ==============================================================================
print(f"\nCarregando GTFS de: {endereco_gtfs}")
gtfs_data = {}
with zipfile.ZipFile(endereco_gtfs, 'r') as z:
    for fname in z.namelist():
        if fname.endswith('.txt'):
            with z.open(fname) as f:
                # read as string to preserve GTFS integrity exactly
                gtfs_data[fname.split('.')[0]] = pd.read_csv(f, dtype=str)

df_trips = gtfs_data['trips']
df_routes = gtfs_data['routes']
df_calendar = gtfs_data.get('calendar', pd.DataFrame())
df_calendar_dates = gtfs_data.get('calendar_dates', pd.DataFrame())
df_feed_info = gtfs_data.get('feed_info', pd.DataFrame())

rotas_afetadas_desvios = df_routes[df_routes['route_short_name'].isin(linhas_afetadas_desvios)]['route_id'].tolist()
print(f"Total rotas afetadas por desvios gerais: {len(rotas_afetadas_desvios)}")

# Modify trips: suffix service_id with _REG if U/S/D and NOT affected
mask_usd = df_trips['service_id'].isin(["U", "S", "D"])
mask_not_affected = ~df_trips['route_id'].isin(rotas_afetadas_desvios)
df_trips.loc[mask_usd & mask_not_affected, 'service_id'] = df_trips.loc[mask_usd & mask_not_affected, 'service_id'] + "_REG"

# Modify calendar: recode U/S/D to U_REG, S_REG, D_REG
if not df_calendar.empty:
    recode_map = {"U": "U_REG", "S": "S_REG", "D": "D_REG"}
    df_calendar['service_id'] = df_calendar['service_id'].map(lambda x: recode_map.get(x, x))

lista_eventos = descricao_desvios['cod_desvio'].dropna().unique().tolist()
excecoes_list = []

for evento in lista_eventos:
    print(f"\nProcessando evento: {evento}")
    if evento not in tabela_desvios.columns:
        print(f"Atenção: A coluna {evento} não existe na planilha 'linhas_desvios'.")
        continue

    # Get affected short_names for this event
    linhas_afet_ev = tabela_desvios[tabela_desvios[evento] == True]['servico'].tolist()
    # match with route_ids
    linhas_afetadas_evento = df_routes[df_routes['route_short_name'].isin(linhas_afet_ev)]['route_id'].tolist()
    
    # trips
    mask_usd = df_trips['service_id'].isin(["U", "S", "D"])
    mask_affected_ev = df_trips['route_id'].isin(linhas_afetadas_evento)
    df_trips.loc[mask_usd & mask_affected_ev, 'service_id'] = df_trips.loc[mask_usd & mask_affected_ev, 'service_id'] + f"_DESAT_{evento}"
    
    if df_calendar.empty:
        continue
        
    # extract new calendars
    cal_mask = df_calendar['service_id'].isin([f"{sid}_{evento}" for sid in ["U", "S", "D"]])
    calendarios_novos = df_calendar[cal_mask].copy()
    
    def adjust_cal_sid(sid):
        if sid in [f"{c}_{evento}" for c in ["U", "S", "D"]]:
            return f"{sid.split('_')[0]}_DESAT_{evento}"
        return sid
    
    calendarios_novos['service_id'] = calendarios_novos['service_id'].apply(adjust_cal_sid)
    df_calendar = pd.concat([df_calendar, calendarios_novos], ignore_index=True)
    
    # Dates handling
    tabela_evento = descricao_desvios[descricao_desvios['cod_desvio'] == evento].iloc[0]
    data_inicio = tabela_evento['data_inicio']
    data_fim = tabela_evento['data_fim']
    
    # Generate list of dates within the range
    delta = data_fim - data_inicio
    dias_evento = [data_inicio + timedelta(days=i) for i in range(delta.days + 1)]
    dias_evento_str = [d.strftime("%Y%m%d") for d in dias_evento]
    
    if not df_calendar_dates.empty:
        # excecoes_datas_ativar
        mask_ativar = df_calendar_dates['date'].isin(dias_evento_str)
        excecoes_datas_ativar = df_calendar_dates[mask_ativar].copy()
        excecoes_datas_ativar['service_id'] = excecoes_datas_ativar['service_id'].apply(lambda sid: f"{sid}_{evento}" if sid in ["U", "S", "D"] else sid)
        
        # excecoes_datas_desativar
        excecoes_datas_desativar = df_calendar_dates[mask_ativar].copy()
        excecoes_datas_desativar['service_id'] = excecoes_datas_desativar['service_id'].apply(lambda sid: f"{sid}_DESAT_{evento}" if sid in ["U", "S", "D"] else sid)
        excecoes_datas_desativar['exception_type'] = "2"
    else:
        excecoes_datas_ativar = pd.DataFrame(columns=['service_id', 'date', 'exception_type'])
        excecoes_datas_desativar = pd.DataFrame(columns=['service_id', 'date', 'exception_type'])

    # helper to map weekday to service_id prefix
    def map_wday(d):
        wd = d.weekday() # Monday is 0 and Sunday is 6
        if wd in [0, 1, 2, 3, 4]: return "U"
        if wd == 5: return "S" # Saturday
        if wd == 6: return "D" # Sunday
        
    df_dias = pd.DataFrame({
        'service_id_base': [map_wday(d) for d in dias_evento],
        'date': dias_evento_str
    })
    
    # calendar_dates_incluir
    calendar_dates_incluir = df_dias[~df_dias['date'].isin(excecoes_datas_ativar['date'])].copy()
    calendar_dates_incluir['service_id'] = calendar_dates_incluir['service_id_base'] + f"_{evento}"
    calendar_dates_incluir['exception_type'] = "1"
    calendar_dates_incluir.drop(columns=['service_id_base'], inplace=True)
    
    # calendar_dates_excluir
    calendar_dates_excluir = df_dias[~df_dias['date'].isin(excecoes_datas_desativar['date'])].copy()
    calendar_dates_excluir['service_id'] = calendar_dates_excluir['service_id_base'] + f"_DESAT_{evento}"
    calendar_dates_excluir['exception_type'] = "2"
    calendar_dates_excluir.drop(columns=['service_id_base'], inplace=True)
    
    excecoes_list.extend([
        calendar_dates_incluir, 
        calendar_dates_excluir, 
        excecoes_datas_ativar, 
        excecoes_datas_desativar
    ])
    
if excecoes_list:
    excecoes = pd.concat(excecoes_list, ignore_index=True)
else:
    excecoes = pd.DataFrame(columns=['service_id', 'date', 'exception_type'])

if not df_calendar_dates.empty:
    def adjust_cal_date_sid(sid):
        if sid in ["U", "S", "D"]:
            return f"{sid}_REG"
        return sid
        
    df_calendar_dates['service_id'] = df_calendar_dates['service_id'].apply(adjust_cal_date_sid)
    df_calendar_dates['exception_type'] = df_calendar_dates['exception_type'].astype(str)
    
    df_calendar_dates = pd.concat([df_calendar_dates, excecoes], ignore_index=True)
else:
    df_calendar_dates = excecoes

# Filter calendar_dates
if not df_feed_info.empty:
    feed_start_str = df_feed_info['feed_start_date'].iloc[0]
    feed_end_str = df_feed_info['feed_end_date'].iloc[0]
    
    try:
        feed_start = datetime.strptime(str(feed_start_str), "%Y%m%d").date()
        feed_end = datetime.strptime(str(feed_end_str), "%Y%m%d").date() + timedelta(days=60)
        
        valid_dates = [ (feed_start + timedelta(days=i)).strftime("%Y%m%d") for i in range((feed_end - feed_start).days + 1) ]
        
        antes = len(df_calendar_dates)
        df_calendar_dates = df_calendar_dates[df_calendar_dates['date'].isin(valid_dates)]
        print(f"Filtro de datas do calendário: de {antes} para {len(df_calendar_dates)} registros.")
    except Exception as e:
        print(f"Atenção: falha ao filtrar datas com base em feed_info: {e}")

# ==============================================================================
# SALVAR GTFS
# ==============================================================================
print(f"\nSalvando modificações no GTFS em: {endereco_gtfs}")

gtfs_data['trips'] = df_trips
if not df_calendar.empty: gtfs_data['calendar'] = df_calendar
if not df_calendar_dates.empty: gtfs_data['calendar_dates'] = df_calendar_dates

endereco_temp = endereco_gtfs.with_name(endereco_gtfs.name.replace('.zip', '_TEMP.zip'))

with zipfile.ZipFile(endereco_gtfs, 'r') as zin, zipfile.ZipFile(endereco_temp, 'w', compression=zipfile.ZIP_DEFLATED) as zout:
    for item in zin.infolist():
        fname = item.filename
        key = fname.split('.')[0]
        if key in ['trips', 'calendar', 'calendar_dates']:
            # Overwrite with modified data
            csv_str = gtfs_data[key].to_csv(index=False)
            zout.writestr(fname, csv_str)
        else:
            # Copy original file
            zout.writestr(item, zin.read(item.filename))

# Swap files
os.replace(endereco_temp, endereco_gtfs)

print("✓ GTFS atualizado com desvios de calendário com sucesso!\n")
