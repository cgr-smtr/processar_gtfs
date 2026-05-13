import pandas as pd
import numpy as np
import zipfile
import os
import time
import geopandas as gpd
from shapely.geometry import LineString
from pathlib import Path
import warnings

warnings.filterwarnings('ignore', category=pd.errors.DtypeWarning)

# ==============================================================================
# CONFIGURAÇÕES
# ==============================================================================
BASE_DADOS = Path("C:/R_SMTR/dados")
BASE_RESULTADOS = Path("C:/R_SMTR/resultados")

ano_gtfs = "2026"
# Novo formato de entrada: rio_YYYY-MM
sufixo = f"rio_{ano_gtfs}-05"

endereco_gtfs = BASE_DADOS / "gtfs" / ano_gtfs / f"{sufixo}.zip"
pasta_saida = BASE_RESULTADOS / "extensoes"
caminho_saida = pasta_saida / f"extensoes_{sufixo}.csv"

# ==============================================================================
# FUNÇÕES AUXILIARES
# ==============================================================================
def log_msg(msg):
    t = time.strftime("%H:%M:%S")
    print(f"[{t}] {msg}")

def map_sentido(direction_id):
    d = str(direction_id).strip().lower()
    if d in ("", "nan", "none", "0.0"):
        return "Circular"
    if d == "0":
        return "Ida"
    if d == "1":
        return "Volta"
    return "Circular"

def create_linestring(group):
    coords = list(zip(group['shape_pt_lon'], group['shape_pt_lat']))
    return LineString(coords)

# ==============================================================================
# PROCESSAMENTO PRINCIPAL
# ==============================================================================
# ==============================================================================
# PROCESSAMENTO PRINCIPAL
# ==============================================================================
print("-" * 80)
print("              LISTAGEM DE LINHAS POR SENTIDO, VISTA E EXTENSÃO             ")
print(f"                         PADRÃO: {sufixo.upper()}")
print("-" * 80)

tempo_inicio = time.time()

if not os.path.exists(endereco_gtfs):
    raise FileNotFoundError(f"Arquivo GTFS não encontrado: {endereco_gtfs}")

log_msg(f"Carregando GTFS de: {endereco_gtfs}")

gtfs = {}
with zipfile.ZipFile(endereco_gtfs, 'r') as z:
    for fname in ['routes.txt', 'trips.txt', 'shapes.txt']:
        if fname in z.namelist():
            with z.open(fname) as f:
                gtfs[fname.split('.')[0]] = pd.read_csv(f, dtype=str)

df_routes = gtfs['routes']
df_trips = gtfs['trips']
df_shapes = gtfs['shapes']

log_msg(f"Rotas: {len(df_routes)} | Trips: {len(df_trips)} | Shapes: {len(df_shapes)}")

# ==============================================================================
# FILTRAR ROTAS (apenas SPPO regular - route_type = 700)
# ==============================================================================
rotas_700 = df_routes[df_routes['route_type'] == '700']['route_id']
df_trips = df_trips[df_trips['route_id'].isin(rotas_700)].copy()
df_routes = df_routes[df_routes['route_type'] == '700'].copy()
log_msg(f"Rotas filtradas (route_type=700): {len(df_routes)} | Trips: {len(df_trips)}")

# ==============================================================================
# FILTRAR EXCEP / DESVIOS
# ==============================================================================
mask_excep = df_trips['service_id'] == "EXCEP"
mask_desvio = df_trips['trip_headsign'].notna() & df_trips['trip_headsign'].str.contains(r"\[")
total_excluir = (mask_excep | mask_desvio).sum()
df_trips = df_trips[~(mask_excep | mask_desvio)].copy()
log_msg(f"Trips EXCEP/desvio removidas: {total_excluir} | Restantes: {len(df_trips)}")

# ==============================================================================
# CALCULAR EXTENSÕES DOS SHAPES (GIS - EPSG:31983)
# ==============================================================================
log_msg("Calculando extensões via GIS (EPSG:31983)...")

df_shapes['shape_pt_lat'] = pd.to_numeric(df_shapes['shape_pt_lat'], errors='coerce')
df_shapes['shape_pt_lon'] = pd.to_numeric(df_shapes['shape_pt_lon'], errors='coerce')
df_shapes['shape_pt_sequence'] = pd.to_numeric(df_shapes['shape_pt_sequence'], errors='coerce')
df_shapes.sort_values(by=['shape_id', 'shape_pt_sequence'], inplace=True)

line_geometries = df_shapes.groupby('shape_id').apply(create_linestring).reset_index(name='geometry')
gdf_shapes = gpd.GeoDataFrame(line_geometries, geometry='geometry', crs="EPSG:4326")
gdf_shapes_proj = gdf_shapes.to_crs(epsg=31983)
gdf_shapes_proj['extensao_m'] = gdf_shapes_proj.geometry.length.astype(int)

log_msg(f"Extensões calculadas para {len(gdf_shapes_proj)} shapes")

# ==============================================================================
# CONSOLIDAR LISTAGEM
# ==============================================================================
log_msg("Consolidando listagem por linha/sentido...")

df_trips_ext = df_trips.merge(
    gdf_shapes_proj[['shape_id', 'extensao_m']], on='shape_id', how='left'
)

df_trips_ext = df_trips_ext.merge(
    df_routes[['route_id', 'route_short_name', 'route_long_name']], on='route_id', how='left'
)

# Identificar linhas circulares (todos os trip_headsign são "Circular")
circ_por_servico = df_trips_ext.groupby('route_short_name')['trip_headsign'].apply(
    lambda x: (x == "Circular").all()
)
servicos_circulares = set(circ_por_servico[circ_por_servico].index)

# Definir sentido por trip: circular sobrescreve direction_id
df_trips_ext['sentido'] = df_trips_ext.apply(
    lambda r: "Circular" if r['route_short_name'] in servicos_circulares else map_sentido(r['direction_id']),
    axis=1
)

# Pegar maior extensão por linha + sentido
listagem = (
    df_trips_ext.groupby(['route_short_name', 'route_long_name', 'sentido'], as_index=False)['extensao_m']
    .max()
)

listagem.rename(columns={
    'route_short_name': 'Serviço',
    'route_long_name': 'Vista',
    'sentido': 'Sentido',
    'extensao_m': 'Extensão'
}, inplace=True)

listagem = listagem[['Serviço', 'Vista', 'Sentido', 'Extensão']]
listagem.sort_values(by=['Serviço', 'Sentido'], inplace=True)
listagem.reset_index(drop=True, inplace=True)

log_msg(f"Listagem gerada com {len(listagem)} linhas")

# ==============================================================================
# SALVAR RESULTADO
# ==============================================================================
pasta_saida.mkdir(parents=True, exist_ok=True)
listagem.to_csv(caminho_saida, index=False)
log_msg(f"Arquivo salvo: {caminho_saida}")

# ==============================================================================
# RESUMO
# ==============================================================================
print("-" * 80)
print("                               RESUMO                                      ")
print("-" * 80)
print(f"  Total de linhas (serviços únicos): {listagem['Serviço'].nunique()}")
print(f"  Total de linhas na listagem:        {len(listagem)}")
print(f"  Total de shapes processados:        {len(gdf_shapes_proj)}")
print(f"  Arquivo gerado:                     {caminho_saida}")

tempo_total = time.time() - tempo_inicio
print(f"\n  Tempo total: {tempo_total:.1f} segundos")
print("-" * 80)
