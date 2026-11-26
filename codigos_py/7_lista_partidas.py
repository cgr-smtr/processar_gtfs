import pandas as pd
import numpy as np
import zipfile
import os
import time
from pathlib import Path

import argparse
import json

# ============================================================================
# LEITURA DE CONFIGURAÇÃO (Streamlit)
# ============================================================================
_parser = argparse.ArgumentParser()
_parser.add_argument('--config', type=str, default='', help='Caminho para arquivo JSON de configuração')
_args, _ = _parser.parse_known_args()
_config = {}
if _args.config:
    with open(_args.config, 'r', encoding='utf-8') as _f:
        _config = json.load(_f)

# ============================================================================
# CONFIGURAÇÕES INICIAIS
# ============================================================================
BASE_DADOS = Path(_config.get("BASE_DADOS", "C:/R_SMTR/dados"))
BASE_RESULTADOS = Path(_config.get("BASE_RESULTADOS", "C:/R_SMTR/resultados"))

ano_gtfs = _config.get("ano_gtfs", "2028")
mes_gtfs = _config.get("mes_gtfs", '11')
estudo_gtfs = _config.get("estudo_gtfs", '05')  # ESTUDO, NÃO CONSIDERAR MAIS QUINZENA!!!!

sufixo = f"{ano_gtfs}-{mes_gtfs}-{estudo_gtfs}Q"

endereco_gtfs = Path(_config.get("endereco_gtfs", BASE_DADOS / f"gtfs/{ano_gtfs}/gtfs_combi_2028-11-05Q.zip"))
tipos_dia = _config.get("tipos_dia", ['du', 'sab', 'dom', 'pf', 'du_ver', 'sab_ver', 'dom_ver'])
if isinstance(tipos_dia, str):
    tipos_dia = [x.strip() for x in tipos_dia.split(',') if x.strip()]

# FILTRO DE LINHAS A EXCLUIR
linhas_excluir = _config.get("linhas_excluir", [])
if isinstance(linhas_excluir, str):
    linhas_excluir = [x.strip() for x in linhas_excluir.split(',') if x.strip()]

# Pasta de saída
pasta_saida = BASE_RESULTADOS / "partidas"

WEEKDAY_COLS = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday']


# ============================================================================
# FUNÇÕES DE HORÁRIO, CALENDÁRIO E INTERVALO
# ============================================================================
def get_pattern(tipo_dia):
    if tipo_dia == 'du': return 'U'
    if tipo_dia == 'sab': return 'S'
    if tipo_dia == 'dom': return 'D'
    if tipo_dia == 'pf': return 'F'
    return ''


def _is_excluded_service(service_id):
    sid = str(service_id).strip()
    if not sid or sid == 'EXCEP' or 'DESAT' in sid:
        return True
    return False


def _is_verao_service(service_id):
    up = str(service_id).strip().upper()
    return (
        '_VER' in up
        or up.startswith('VER_')
        or 'VERAO' in up
        or 'VERÃO' in up
    )


def _belongs_exclusively_to_other_day(service_id, tipo_dia):
    """Evita que calendários explícitos (ex: F_REG com monday..friday=1) vazem para outro tipo_dia."""
    sid = str(service_id).strip()
    if tipo_dia != 'pf' and (sid == 'F' or sid.startswith('F_')):
        return True
    if tipo_dia != 'sab' and (sid == 'S' or sid.startswith('S_')):
        return True
    if tipo_dia != 'dom' and (sid == 'D' or sid.startswith('D_')):
        return True
    if tipo_dia != 'du' and (sid == 'U' or sid.startswith('U_')):
        return True
    return False


def resolver_service_ids_por_tipo_dia(tipo_dia, df_trips, df_calendar=None):
    """
    Resolve os service_ids aplicáveis a um tipo_dia ('du', 'sab', 'dom', 'pf',
    variantes de verão 'du_ver', 'sab_ver', 'dom_ver', 'pf_ver' ou service_id explícito),
    combinando o padrão legado (U/S/D/F) com os dias da semana definidos em calendar.txt.
    Calendários de verão não são misturados aos regulares.
    """
    if df_trips.empty or 'service_id' not in df_trips.columns:
        return set()

    sids_trips = {
        str(s).strip()
        for s in df_trips['service_id'].dropna().unique()
        if not _is_excluded_service(s)
    }

    tipos_base = {'du', 'sab', 'dom', 'pf'}
    tipos_verao = {'du_ver': 'du', 'sab_ver': 'sab', 'dom_ver': 'dom', 'pf_ver': 'pf'}

    if tipo_dia in tipos_verao:
        base_tipo = tipos_verao[tipo_dia]
        sids_candidatos = {s for s in sids_trips if _is_verao_service(s)}
    elif tipo_dia in tipos_base:
        base_tipo = tipo_dia
        sids_candidatos = {s for s in sids_trips if not _is_verao_service(s)}
    else:
        return {tipo_dia} if tipo_dia in sids_trips else set()

    pattern = get_pattern(base_tipo)
    matched = set()

    cal_sids = set()
    if df_calendar is not None and not df_calendar.empty and 'service_id' in df_calendar.columns:
        cal = df_calendar.copy()
        cal['service_id'] = cal['service_id'].astype(str).str.strip()
        cal = cal[cal['service_id'].isin(sids_candidatos)]
        cal_sids = set(cal['service_id'])

        if base_tipo == 'du':
            cols = [c for c in WEEKDAY_COLS if c in cal.columns]
            if cols:
                mask = cal[cols].apply(lambda col: col.astype(str).str.strip() == '1').any(axis=1)
                for sid in cal.loc[mask, 'service_id']:
                    if not _belongs_exclusively_to_other_day(sid, 'du'):
                        matched.add(sid)
        elif base_tipo == 'sab' and 'saturday' in cal.columns:
            mask = cal['saturday'].astype(str).str.strip() == '1'
            for sid in cal.loc[mask, 'service_id']:
                if not _belongs_exclusively_to_other_day(sid, 'sab'):
                    matched.add(sid)
        elif base_tipo == 'dom' and 'sunday' in cal.columns:
            mask = cal['sunday'].astype(str).str.strip() == '1'
            for sid in cal.loc[mask, 'service_id']:
                if not _belongs_exclusively_to_other_day(sid, 'dom'):
                    matched.add(sid)

    # Padrão legado por substring (U, S, D, F):
    # para du/sab/dom, aplica apenas a service_ids que não estejam definidos em calendar.txt,
    # evitando falsos positivos (ex.: 'S' em 'SEG_SEX' ou 'D' em 'SABADO').
    if pattern:
        for sid in sids_candidatos:
            if base_tipo in {'du', 'sab', 'dom'} and sid in cal_sids:
                continue
            if pattern in sid and not _belongs_exclusively_to_other_day(sid, base_tipo):
                matched.add(sid)

    # Ponto facultativo: também reconhece nomes explícitos (PF, FACULT, FERIADO)
    if base_tipo == 'pf':
        for sid in sids_candidatos:
            up = sid.upper()
            if ('PF' in up or 'FACULT' in up or 'FERIADO' in up) and not _belongs_exclusively_to_other_day(sid, base_tipo):
                matched.add(sid)

    return matched


def horario_to_timedelta(hk_str):
    if pd.isna(hk_str) or hk_str == '': return pd.NaT
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


def get_faixa(td):
    if pd.isna(td): return ""
    total_horas = int(td.total_seconds()) // 3600
    horas = total_horas % 24

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
    return "23:00-24:00"


def main():
    pasta_saida.mkdir(parents=True, exist_ok=True)

    print("\n==========================================================")
    print("INICIANDO PROCESSAMENTO DE PARTIDAS")
    print("==========================================================\n")

    # ========================================================================
    # LEITURA DO GTFS
    # ========================================================================
    print("ETAPA 1: Lendo GTFS base...")
    if not os.path.exists(endereco_gtfs):
        raise FileNotFoundError(f"Arquivo GTFS não encontrado: {endereco_gtfs}")

    gtfs = {}
    with zipfile.ZipFile(endereco_gtfs, 'r') as z:
        for fname in ['routes.txt', 'trips.txt', 'stop_times.txt', 'frequencies.txt', 'agency.txt', 'shapes.txt', 'calendar.txt']:
            if fname in z.namelist():
                with z.open(fname) as f:
                    try:
                        gtfs[fname.split('.')[0]] = pd.read_csv(f, dtype=str)
                    except pd.errors.EmptyDataError:
                        gtfs[fname.split('.')[0]] = pd.DataFrame()

    df_routes = gtfs['routes']
    df_trips = gtfs['trips']
    df_st = gtfs['stop_times']
    df_freq = gtfs.get('frequencies', pd.DataFrame())
    df_agency = gtfs.get('agency', pd.DataFrame())
    df_shapes = gtfs.get('shapes', pd.DataFrame())
    df_calendar = gtfs.get('calendar', pd.DataFrame())

    print(f"  ✓ GTFS carregado: {len(df_routes)} rotas, {len(df_trips)} trips, {len(df_calendar)} calendários")

    # Filtrar frescões
    frescoes = df_routes[df_routes['route_type'] == '200']['route_id'].tolist()
    df_trips = df_trips[~df_trips['route_id'].isin(frescoes)]

    path_fantasmas = BASE_DADOS / "insumos/trip_id_fantasma.txt"
    if os.path.exists(path_fantasmas):
        trips_fantasma = pd.read_csv(path_fantasmas, header=None).iloc[:, 0].tolist()
    else:
        trips_fantasma = []

    # Pré-calcula a primeira parada de cada trip_id (compatível com stop_sequence iniciando em 0 ou 1)
    df_st_first = (
        df_st.assign(_seq=pd.to_numeric(df_st['stop_sequence'], errors='coerce').fillna(0))
        .sort_values(['trip_id', '_seq'])
        .drop_duplicates(subset=['trip_id'], keep='first')[['trip_id', 'departure_time']]
    )

    # ========================================================================
    # CÁLCULO DE EXTENSÕES (GIS - EPSG:31983)
    # ========================================================================
    print("\nETAPA 2: Calculando extensões via GIS (EPSG:31983)...")

    if not df_shapes.empty:
        import geopandas as gpd
        from shapely.geometry import LineString

        df_shapes['shape_pt_lon'] = pd.to_numeric(df_shapes['shape_pt_lon'], errors='coerce')
        df_shapes['shape_pt_lat'] = pd.to_numeric(df_shapes['shape_pt_lat'], errors='coerce')
        df_shapes['shape_pt_sequence'] = pd.to_numeric(df_shapes['shape_pt_sequence'], errors='coerce')
        df_shapes.sort_values(by=['shape_id', 'shape_pt_sequence'], inplace=True)

        def create_linestring(group):
            coords = list(zip(group['shape_pt_lon'], group['shape_pt_lat']))
            return LineString(coords)

        line_geometries = df_shapes.groupby('shape_id').apply(create_linestring).reset_index(name='geometry')
        gdf_shapes = gpd.GeoDataFrame(line_geometries, geometry='geometry', crs="EPSG:4326")

        gdf_shapes_proj = gdf_shapes.to_crs(epsg=31983)
        gdf_shapes_proj['extensao'] = gdf_shapes_proj.geometry.length.astype(int)

        # Regra do script 8: extensão vem só dos shapes regulares (sem EXCEP/desvio),
        # senão o max() pega o traçado de exceção e infla a extensão (ex.: 006 Ida 8460 -> 8866)
        df_trips_reg = df_trips[
            (df_trips['service_id'] != 'EXCEP')
            & ~(df_trips['trip_headsign'].notna() & df_trips['trip_headsign'].str.contains(r'\['))
        ]
        df_trips_ext = df_trips_reg.merge(gdf_shapes_proj[['shape_id', 'extensao']], on='shape_id', how='left')
        extensoes_df = df_trips_ext.groupby(['trip_short_name', 'direction_id', 'route_id'])['extensao'].max().reset_index()
        print(f"  ✓ Extensões calculadas para {len(extensoes_df)} itinerários (sem EXCEP/desvio)")
    else:
        print("  ⚠️ shapes.txt não encontrado. Extensões ficarão zeradas.")
        extensoes_df = pd.DataFrame(columns=['trip_short_name', 'direction_id', 'route_id', 'extensao'])

    consolidado_lista = []

    # ========================================================================
    # LOOP POR TIPO DE DIA
    # ========================================================================
    for current_tipo_dia in tipos_dia:
        print(f"\n==========================================================")
        print(f"PROCESSANDO TIPO DE DIA: {current_tipo_dia.upper()}")
        print(f"==========================================================")

        service_ids_dia = resolver_service_ids_por_tipo_dia(current_tipo_dia, df_trips, df_calendar)
        print(f"  • Calendários (service_id) selecionados: {sorted(service_ids_dia) if service_ids_dia else 'nenhum'}")

        if not service_ids_dia:
            print("Nenhuma viagem processada para este dia.")
            continue

        mask_day = df_trips['service_id'].astype(str).str.strip().isin(service_ids_dia)
        mask_fant = df_trips['trip_id'].isin(trips_fantasma)

        trips_dia = df_trips[mask_day & ~mask_fant].copy()

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

                    if pd.isna(st) or pd.isna(ed) or hw.total_seconds() <= 0:
                        continue

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

        viagens_completo['departure_time'] = viagens_completo['start_time'].apply(timedelta_to_horario)
        viagens_completo['tipo_dia'] = current_tipo_dia

        viagens_completo = viagens_completo.merge(df_routes[['route_id', 'route_long_name', 'route_type', 'agency_id']], on='route_id', how='left')

        if not df_agency.empty:
            viagens_completo = viagens_completo.merge(df_agency[['agency_id', 'agency_name']], on='agency_id', how='left')
        else:
            viagens_completo['agency_name'] = ""

        if not extensoes_df.empty:
            viagens_completo = viagens_completo.merge(extensoes_df, on=['trip_short_name', 'direction_id', 'route_id'], how='left')
            viagens_completo['extensao'] = viagens_completo['extensao'].fillna(0).astype(int)
        else:
            viagens_completo['extensao'] = 0

        viagens_completo['faixa'] = viagens_completo['start_time'].apply(get_faixa)

        col_order = ['trip_short_name', 'route_long_name', 'trip_headsign', 'direction_id', 'departure_time', 'faixa', 'agency_name', 'extensao', 'route_type', 'tipo_dia']

        viagens_c = viagens_completo[col_order].copy()
        viagens_c.sort_values(by=['trip_short_name', 'direction_id', 'departure_time'], inplace=True)

        viagens_ind = viagens_c.drop(columns=['route_long_name']).copy()

        viagens_ind['departure_td'] = viagens_ind['departure_time'].apply(horario_to_timedelta)
        viagens_ind['interval_td'] = viagens_ind.groupby(['trip_short_name', 'direction_id'])['departure_td'].diff()
        viagens_ind['intervalo'] = viagens_ind['interval_td'].apply(timedelta_to_horario)
        viagens_ind.drop(columns=['departure_td', 'interval_td'], inplace=True)

        cols_ind = [c for c in viagens_ind.columns if c != 'intervalo'] + ['intervalo']
        viagens_ind = viagens_ind[cols_ind]

        arq_csv = os.path.join(pasta_saida, f"partidas_{current_tipo_dia}.csv")
        viagens_ind.to_csv(arq_csv, index=False)
        print(f"  ✓ Arquivo salvo: partidas_{current_tipo_dia}.csv")

        consolidado_lista.append(viagens_c)

    print("\n==========================================================")
    print("SALVANDO RESULTADO FINAL")
    print("==========================================================\n")

    if consolidado_lista:
        df_final = pd.concat(consolidado_lista, ignore_index=True)

        for col in df_final.columns:
            if df_final[col].dtype == 'object' or pd.api.types.is_timedelta64_dtype(df_final[col]):
                df_final[col] = df_final[col].fillna("").astype(str)

        pasta_out = Path(pasta_saida).resolve()

        csv_path = str(pasta_out / f"partidas_{sufixo}.csv")
        parq_path = str(pasta_out / f"partidas_{sufixo}.parquet")

        df_final.to_csv(csv_path, index=False)
        try:
            with open(parq_path, 'wb') as f:
                df_final.to_parquet(f, engine='pyarrow', index=False)
            print("✓ Arquivos finais salvos: partidas.csv e partidas.parquet")
        except ImportError:
            print("⚠️ pyarrow não instalado; salvo apenas partidas.csv")
        print(f"✓ Total de linhas processadas: {len(df_final)}")
    else:
        print("Nenhuma partida compilada ao final do script.")

    print("==========================================================")
    print("PROCESSAMENTO COMPLETO FINALIZADO!")
    print("==========================================================")


if __name__ == "__main__":
    main()
