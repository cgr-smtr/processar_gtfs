import pandas as pd
import numpy as np
import zipfile
import io
import os
from pathlib import Path
import warnings
import geopandas as gpd
from shapely.geometry import LineString

warnings.filterwarnings('ignore')

# ==============================================================================
# CONFIGURAÇÕES
# ==============================================================================
BASE_DADOS = Path("C:/R_SMTR/dados")

ano_gtfs = "2026"
mes_gtfs = "03"
quinzena_gtfs = "04" #ESTUDO, NÃO CONSIDERAR MAIS QUINZENA!!!!

endereco_gtfs = BASE_DADOS / f"gtfs/{ano_gtfs}/sppo_{ano_gtfs}-{mes_gtfs}-{quinzena_gtfs}Q_PROC.zip"
caminho_saida = BASE_DADOS / f"os/os_{ano_gtfs}-{mes_gtfs}-{quinzena_gtfs}_excep.csv"

# Filtrar por calendários específicos (service_id). Se vazio, utiliza todos.
# Exemplo: ["U", "S", "D"]
CALENDARIOS_ALVO = ["EXCEP"] 

print("\n╔════════════════════════════════════════════════════════════════════════════╗")
print("║              PROCESSAMENTO DE GTFS - TRAJETOS ALTERNATIVOS                 ║")
print("╚════════════════════════════════════════════════════════════════════════════╝\n")

print(f"Carregando GTFS de: {endereco_gtfs}")

gtfs_data = {}
with zipfile.ZipFile(endereco_gtfs, 'r') as z:
    for fname in ['trips.txt', 'routes.txt', 'agency.txt', 'shapes.txt']:
        if fname in z.namelist():
            with z.open(fname) as f:
                gtfs_data[fname.split('.')[0]] = pd.read_csv(f, dtype=str)

df_trips = gtfs_data['trips']
df_routes = gtfs_data['routes']
df_agency = gtfs_data['agency']
df_shapes = gtfs_data['shapes']

# ==============================================================================
# FILTRAR TRIPS DE DESVIO E ROTAS
# ==============================================================================
print("Identificando viagens de desvio...")
# Filter by service_id if specified
if CALENDARIOS_ALVO:
    print(f"Filtrando viagens para os calendários: {CALENDARIOS_ALVO}")
    df_trips = df_trips[df_trips['service_id'].isin(CALENDARIOS_ALVO)].copy()

# Filter trips containing '[' in trip_headsign
mask_desvio = df_trips['trip_headsign'].notna() & df_trips['trip_headsign'].str.contains(r"\[")
trips_desvio = df_trips[mask_desvio][['trip_id', 'trip_short_name', 'trip_headsign', 'shape_id', 'route_id', 'direction_id', 'service_id']].copy()

# Extract detour text (between '[' and ']')
trips_desvio['desvio'] = trips_desvio['trip_headsign'].str.extract(r"(\[.*?\])")[0]

print(f"Total de viagens com desvio: {len(trips_desvio)}")

# Remove frescões
frescoes = df_routes[df_routes['route_type'] == '200']['route_id'].tolist()
trips_desvio = trips_desvio[~trips_desvio['route_id'].isin(frescoes)]

print(f"Total de viagens com desvio após filtro de frescões: {len(trips_desvio)}")

if trips_desvio.empty:
    print("\nNenhum itinerário de desvio encontrado. Relatório vazio gerado.")
    relatorio_desvios = pd.DataFrame(columns=['Serviço', 'Vista', 'Consórcio', 'Sentido', 'Extensão', 'Evento'])
    os.makedirs(os.path.dirname(caminho_saida), exist_ok=True)
    relatorio_desvios.to_csv(caminho_saida, index=False, sep=',', decimal='.')
    exit(0)

trips_desvio_ids = trips_desvio['trip_id'].unique()
shapes_afetados = trips_desvio['shape_id'].dropna().unique()

df_shapes_desvio = df_shapes[df_shapes['shape_id'].isin(shapes_afetados)].copy()

# ==============================================================================
# CALCULAR EXTENSÕES DOS SHAPES (GIS - EPSG:31983)
# ==============================================================================
print("Calculando distâncias dos percursos via GIS (EPSG:31983)...")
df_shapes_desvio['shape_pt_lat'] = pd.to_numeric(df_shapes_desvio['shape_pt_lat'], errors='coerce')
df_shapes_desvio['shape_pt_lon'] = pd.to_numeric(df_shapes_desvio['shape_pt_lon'], errors='coerce')
df_shapes_desvio['shape_pt_sequence'] = pd.to_numeric(df_shapes_desvio['shape_pt_sequence'], errors='coerce')

df_shapes_desvio.sort_values(by=['shape_id', 'shape_pt_sequence'], inplace=True)

# Generate LineStrings
def create_linestring(group):
    coords = list(zip(group['shape_pt_lon'], group['shape_pt_lat']))
    return LineString(coords)

line_geometries = df_shapes_desvio.groupby('shape_id').apply(create_linestring).reset_index(name='geometry')
gdf_shapes_desvio = gpd.GeoDataFrame(line_geometries, geometry='geometry', crs="EPSG:4326")

# Project and calculate length in km
gdf_shapes_proj = gdf_shapes_desvio.to_crs(epsg=31983)
gdf_shapes_proj['extensao'] = gdf_shapes_proj.geometry.length / 1000.0

shapes_extensao = gdf_shapes_proj[['shape_id', 'extensao']].copy()

# ==============================================================================
# GERAR RELATÓRIO
# ==============================================================================
print("Consolidando relatório final...")

# Left joins
relatorio_master = trips_desvio.merge(df_routes[['route_id', 'route_short_name', 'route_long_name', 'agency_id']], on='route_id', how='left')
relatorio_master = relatorio_master.merge(df_agency[['agency_id', 'agency_name']], on='agency_id', how='left')
relatorio_master = relatorio_master.merge(shapes_extensao, on='shape_id', how='left')

# Map direction
def map_sentido(d_id):
    if str(d_id) == "0" or str(d_id) == "0.0": return "Ida"
    if str(d_id) == "1" or str(d_id) == "1.0": return "Volta"
    return "Circular"

relatorio_master['Sentido'] = relatorio_master['direction_id'].apply(map_sentido)

# Group and summarise
relatorio_agrupado = relatorio_master.groupby(['trip_short_name', 'route_long_name', 'agency_name', 'Sentido', 'desvio'], dropna=False)['extensao'].sum().reset_index()

relatorio_agrupado['Extensão'] = relatorio_agrupado['extensao'].apply(lambda x: f"{x:.3f}")

# Select and rename columns matching R
relatorio_final = relatorio_agrupado[['trip_short_name', 'route_long_name', 'agency_name', 'Sentido', 'Extensão', 'desvio']]
relatorio_final.rename(columns={
    'trip_short_name': 'Serviço',
    'route_long_name': 'Vista',
    'agency_name': 'Consórcio',
    'desvio': 'Evento'
}, inplace=True)

relatorio_final.sort_values(by=['Serviço', 'Evento', 'Sentido'], inplace=True)

print(f"Relatório gerado com {len(relatorio_final)} linhas agrupadas.")

# Ensure dir exists
os.makedirs(os.path.dirname(caminho_saida), exist_ok=True)

print(f"Salvando em: {caminho_saida}")
relatorio_final.to_csv(caminho_saida, index=False, sep=',', decimal='.')

print("\n✓ Processamento de caminhos alternativos/desvios concluído com sucesso!")
