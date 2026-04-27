import pandas as pd
import numpy as np
import geopandas as gpd
from shapely.geometry import Point, LineString
import zipfile
import os
import time
from pathlib import Path

import warnings
warnings.filterwarnings('ignore', category=pd.errors.DtypeWarning)
warnings.filterwarnings('ignore', category=UserWarning) # Ignores some shapely warnings for M-coordinates if present

# ==============================================================================
# CONFIGURAÇÕES
# ==============================================================================
BASE_DADOS = Path("C:/R_SMTR/dados")

ano_gtfs = "2026"
mes_gtfs = "05"
estudo_gtfs = "01" #ESTUDO, NÃO CONSIDERAR MAIS QUINZENA!!!!

endereco_gtfs_combi = BASE_DADOS / f"gtfs/{ano_gtfs}/gtfs_rio-de-janeiro_pub.zip"
pasta_shape_sppo = BASE_DADOS / f"shapes/{ano_gtfs}"

print("\n╔════════════════════════════════════════════════════════════════════════════╗")
print("║                       GERAÇÃO DE SHAPES (GIS)                              ║")
print("╚════════════════════════════════════════════════════════════════════════════╝\n")

# ==============================================================================
# LENDO GTFS PÚBLICO
# ==============================================================================
print(f"Lendo GTFS: {endereco_gtfs_combi}")
if not os.path.exists(endereco_gtfs_combi):
    raise FileNotFoundError(f"Arquivo GTFS público não encontrado: {endereco_gtfs_combi}")

gtfs = {}
with zipfile.ZipFile(endereco_gtfs_combi, 'r') as z:
    for fname in ['shapes.txt', 'trips.txt', 'routes.txt', 'agency.txt', 'stops.txt', 'stop_times.txt', 'fare_attributes.txt', 'fare_rules.txt']:
        if fname in z.namelist():
            with z.open(fname) as f:
                gtfs[fname.split('.')[0]] = pd.read_csv(f, dtype=str)

df_shapes = gtfs['shapes']
df_trips = gtfs['trips']
df_routes = gtfs['routes']
df_agency = gtfs.get('agency', pd.DataFrame())
df_stops = gtfs.get('stops', pd.DataFrame())
df_stop_times = gtfs.get('stop_times', pd.DataFrame())
df_fare_rules = gtfs.get('fare_rules', pd.DataFrame())
df_fare_attr = gtfs.get('fare_attributes', pd.DataFrame())

# ==============================================================================
# ORDENAÇÃO DE SHAPES E VERIFICAÇÃO DE PONTOS INVÁLIDOS
# ==============================================================================
print("Ordenando coordenadas e verificando integridade geométrica...")
df_shapes['shape_pt_sequence'] = pd.to_numeric(df_shapes['shape_pt_sequence'], errors='coerce').fillna(0).astype(int)
df_shapes.sort_values(by=['shape_id', 'shape_pt_sequence'], inplace=True)

# Checks for invalid shapes
shape_counts = df_shapes['shape_id'].value_counts()
shapes_invalidos = shape_counts[shape_counts < 2].index.tolist()

if shapes_invalidos:
    print("\n⚠️  ATENÇÃO: Foram encontrados shapes com menos de 2 pontos!")
    print("  Esses shapes NÃO formam uma linha, mas precisamos lidar com eles (ignorados na criação geométrica).")
    print(f"  Total inválidos: {len(shapes_invalidos)}")
    
    trips_invalidas = df_trips[df_trips['shape_id'].isin(shapes_invalidos)][['shape_id', 'route_id', 'trip_short_name', 'direction_id', 'service_id']].drop_duplicates()
    if not trips_invalidas.empty:
        print(f"  Trips associadas: {len(trips_invalidas)}")
        trips_invalidas.to_csv("shapes_invalidos_com_trips.csv", index=False)
        print("  Relatório salvo em 'shapes_invalidos_com_trips.csv'")
    
    # Actually filter them out of our processing dataset since Shapely LineString requires >= 2 points
    df_shapes = df_shapes[~df_shapes['shape_id'].isin(shapes_invalidos)].copy()

# ==============================================================================
# 1. PREPARAR TABELA DE SHAPES COM INFORMAÇÕES DAS TRIPS
# ==============================================================================
print("Cruzando itinerários base com suas geometrias...")
# u_reg
mask_u_reg = df_trips['service_id'] == 'U_REG'
trips_base_u_reg = df_trips[mask_u_reg][['trip_id', 'shape_id', 'trip_short_name', 'trip_headsign', 'direction_id', 'service_id']]
trips_base_u_reg = trips_base_u_reg.drop_duplicates(subset=['shape_id']).drop(columns=['trip_id'])

# reg
mask_reg = df_trips['service_id'].notna() & df_trips['service_id'].str.contains("REG") & ~df_trips['shape_id'].isin(trips_base_u_reg['shape_id'])
trips_base_reg = df_trips[mask_reg][['trip_id', 'shape_id', 'trip_short_name', 'trip_headsign', 'direction_id', 'service_id']]
trips_base_reg = trips_base_reg.drop_duplicates(subset=['shape_id']).drop(columns=['trip_id'])

# trip_especial_u
mask_esp_u = df_trips['service_id'].notna() & df_trips['service_id'].str.contains("U") & ~df_trips['shape_id'].isin(trips_base_u_reg['shape_id']) & ~df_trips['shape_id'].isin(trips_base_reg['shape_id'])
trip_especial_u = df_trips[mask_esp_u][['trip_id', 'shape_id', 'trip_short_name', 'trip_headsign', 'direction_id', 'service_id']]
trip_especial_u = trip_especial_u.drop_duplicates(subset=['shape_id']).drop(columns=['trip_id'])

# trip_especial
mask_esp = ~df_trips['shape_id'].isin(trips_base_u_reg['shape_id']) & ~df_trips['shape_id'].isin(trips_base_reg['shape_id']) & ~df_trips['shape_id'].isin(trip_especial_u['shape_id'])
trip_especial = df_trips[mask_esp][['trip_id', 'shape_id', 'trip_short_name', 'trip_headsign', 'direction_id', 'service_id']]
trip_especial = trip_especial.drop_duplicates(subset=['shape_id']).drop(columns=['trip_id'])

# Join base
trips_base = pd.concat([trips_base_u_reg, trips_base_reg, trip_especial_u, trip_especial], ignore_index=True)

# Generate LineStrings
print("Gerando vetores das rotas...")
df_shapes['shape_pt_lon'] = pd.to_numeric(df_shapes['shape_pt_lon'], errors='coerce')
df_shapes['shape_pt_lat'] = pd.to_numeric(df_shapes['shape_pt_lat'], errors='coerce')

# Convert shapes coordinates to LineString geometries
def create_linestring(group):
    coords = list(zip(group['shape_pt_lon'], group['shape_pt_lat']))
    return LineString(coords)

line_geometries = df_shapes.groupby('shape_id').apply(create_linestring).reset_index(name='geometry')
gdf_shapes = gpd.GeoDataFrame(line_geometries, geometry='geometry', crs="EPSG:4326")

# Merge trip info
gdf_shapes = gdf_shapes.merge(trips_base, on='shape_id', how='left')

gdf_shapes['service_id_original'] = gdf_shapes['service_id']
# R logical cleanup for service_id prefix grouping
mask_desat = gdf_shapes['service_id'].notna() & gdf_shapes['service_id'].str.contains("_DESAT_")
gdf_shapes.loc[mask_desat, 'service_id'] = gdf_shapes.loc[mask_desat, 'service_id'].str[0]

mask_reg = gdf_shapes['service_id'].notna() & gdf_shapes['service_id'].str.contains("REG")
gdf_shapes.loc[mask_reg, 'service_id'] = gdf_shapes.loc[mask_reg, 'service_id'].str[0]

# ==============================================================================
# 3. PREPARAR DADOS PARA EXPORTAÇÃO E ATRIBUTOS
# ==============================================================================
print("Preenchendo metadados do GeoPackage/Shapefile...")
# Note: Google sheets was used in R, we read the offline insumo downloaded earlier!
path_desc = "insumos_desvios/descricao_desvios.csv"
if os.path.exists(path_desc):
    descricao_desvios = pd.read_csv(path_desc)[['cod_desvio', 'descricao_desvio', 'data_inicio', 'data_fim']]
else:
    print("Aviso: 'descricao_desvios.csv' não encontrado. Eventos ficarão sem descrição nos vetores.")
    descricao_desvios = pd.DataFrame(columns=['cod_desvio', 'descricao_desvio'])

trips_join = df_trips[['shape_id', 'route_id']].drop_duplicates(subset=['shape_id'])

# Length calculation in CRS 31983 (Meters)
# Reprojecting
gdf_shapes_proj = gdf_shapes.to_crs(epsg=31983)
gdf_shapes_proj['extensao'] = gdf_shapes_proj.geometry.length.astype(int)

# Filter AN31
gdf_shapes_proj = gdf_shapes_proj[gdf_shapes_proj['service_id'] != 'AN31']

# Reverting projection
shapes_ext = gdf_shapes_proj.to_crs(epsg=4326).copy()

shapes_ext = shapes_ext[['trip_short_name', 'trip_headsign', 'direction_id', 'service_id', 'service_id_original', 'extensao', 'shape_id', 'geometry']]
shapes_ext.rename(columns={
    'trip_short_name': 'servico',
    'trip_headsign': 'destino',
    'direction_id': 'direcao',
    'service_id': 'tipo_dia'
}, inplace=True)

# R logic: cod_desvio = substr(service_id_original, 3, nchar(service_id_original))
# In Python, service_id_original[2:] if len > 2 else NA
def get_cod_desvio(sid):
    if pd.isna(sid): return sid
    if len(sid) > 2: return sid[2:]
    return np.nan

shapes_ext['cod_desvio'] = shapes_ext['service_id_original'].apply(get_cod_desvio)

# Merge back the detour descriptions
shapes_ext = shapes_ext.merge(descricao_desvios, on='cod_desvio', how='left').drop(columns=['cod_desvio'])

# Merge with trips and routes
shapes_ext = shapes_ext.merge(trips_join, on='shape_id', how='left')
df_routes_sub = df_routes[['route_id', 'agency_id', 'route_type', 'route_desc']]
shapes_ext = shapes_ext.merge(df_routes_sub, on='route_id', how='left')
shapes_ext.rename(columns={'route_desc': 'descricao'}, inplace=True)

# Merge Fares
if not df_fare_rules.empty and not df_fare_attr.empty:
    df_fares = df_fare_rules.merge(df_fare_attr[['fare_id', 'price']], on='fare_id', how='left')
    df_fares = df_fares[['route_id', 'price']].drop_duplicates(subset=['route_id'])
    shapes_ext = shapes_ext.merge(df_fares, on='route_id', how='left')
    shapes_ext.rename(columns={'price': 'tarifas'}, inplace=True)
else:
    shapes_ext['tarifas'] = np.nan

# Type of route
def get_tipo_rota(rt):
    if rt == '200': return 'frescao'
    if rt == '702': return 'brt'
    if rt == '700': return 'regular'
    return np.nan

shapes_ext['route_type'] = shapes_ext['route_type'].astype(str)
shapes_ext['tipo_rota'] = shapes_ext['route_type'].apply(get_tipo_rota)

# Merge Agency
if not df_agency.empty:
    df_ag_sub = df_agency[['agency_id', 'agency_name']]
    shapes_ext = shapes_ext.merge(df_ag_sub, on='agency_id', how='left')
    shapes_ext.rename(columns={'agency_name': 'consorcio'}, inplace=True)
else:
    shapes_ext['consorcio'] = np.nan

# Drop utility columns
shapes_ext.drop(columns=['route_id', 'agency_id', 'route_type', 'service_id_original'], inplace=True, errors='ignore')

# Hide detour column if all values are null/empty
if 'descricao_desvio' in shapes_ext.columns:
    if shapes_ext['descricao_desvio'].isna().all() or (shapes_ext['descricao_desvio'] == "").all():
        print("Removendo coluna 'descricao_desvio' (sem desvios ativos)")
        shapes_ext.drop(columns=['descricao_desvio'], inplace=True)

# ==============================================================================
# 4. EXPORTAR SHAPES (TRAJETOS)
# ==============================================================================
print(f"Salvando geometrias completas em: {pasta_shape_sppo}")
os.makedirs(pasta_shape_sppo, exist_ok=True)

# Shapefile version (rename for 10-char limit)
shapes_ext_shp = shapes_ext.copy()

mapeamento_shp = {
    'servico': 'servico',
    'destino': 'destino',
    'direcao': 'direcao',
    'tipo_dia': 'tipo_dia',
    'extensao': 'extensao',
    'consorcio': 'consorcio',
    'tipo_rota': 'tipo_rota',
    'descricao': 'descricao',
    'tarifas': 'tarifas'
}

if 'descricao_desvio' in shapes_ext_shp.columns:
    shapes_ext_shp.rename(columns={'descricao_desvio': 'desvio'}, inplace=True)
    mapeamento_shp['desvio'] = 'desvio'

# Select only desired columns that actually exist
cols_shp = [c for c in mapeamento_shp.values() if c in shapes_ext_shp.columns]
shapes_ext_shp = shapes_ext_shp[cols_shp + ['geometry']]

nome_arquivo = f"shapes_trajetos_{ano_gtfs}-{mes_gtfs}-{estudo_gtfs}Q"
endereco_shp = os.path.join(pasta_shape_sppo, f"{nome_arquivo}.shp")
endereco_gpkg = os.path.join(pasta_shape_sppo, f"{nome_arquivo}.gpkg")

# Save files using Geopandas
shapes_ext_shp.to_file(endereco_shp, driver='ESRI Shapefile', engine='pyogrio')
shapes_ext.to_file(endereco_gpkg, driver='GPKG', engine='pyogrio')
print(f"✓ Salvo {nome_arquivo}.shp")
print(f"✓ Salvo {nome_arquivo}.gpkg")

# ==============================================================================
# 5. EXPORTAR PONTOS DE PARADA
# ==============================================================================
if not df_stops.empty and not df_stop_times.empty:
    print("Gerando vetores dos pontos de parada...")
    pontos_usados = df_stop_times['stop_id'].unique()
    gtfs_stops_filt = df_stops[df_stops['stop_id'].isin(pontos_usados)].copy()

    # route_type por parada: agrega todos os tipos de rota das viagens que passam no ponto
    stop_route_types = (
        df_stop_times[['trip_id', 'stop_id']]
        .merge(df_trips[['trip_id', 'route_id']], on='trip_id', how='left')
        .merge(df_routes[['route_id', 'route_type']], on='route_id', how='left')
    )
    stop_route_types = (
        stop_route_types.dropna(subset=['stop_id', 'route_type'])
        .groupby('stop_id')['route_type']
        .apply(lambda s: ','.join(sorted(set(s.astype(str)))))
        .reset_index(name='route_type')
    )
    gtfs_stops_filt = gtfs_stops_filt.merge(stop_route_types, on='stop_id', how='left')
    
    gtfs_stops_filt['stop_lon'] = pd.to_numeric(gtfs_stops_filt['stop_lon'], errors='coerce')
    gtfs_stops_filt['stop_lat'] = pd.to_numeric(gtfs_stops_filt['stop_lat'], errors='coerce')
    
    # Convert points into shapely geometry Points
    def create_point(row):
        return Point(row['stop_lon'], row['stop_lat'])
        
    gtfs_stops_filt['geometry'] = gtfs_stops_filt.apply(create_point, axis=1)
    gdf_pontos = gpd.GeoDataFrame(gtfs_stops_filt, geometry='geometry', crs="EPSG:4326")
    
    nome_pontos = f"shapes_pontos_{ano_gtfs}-{mes_gtfs}-{estudo_gtfs}Q"
    endereco_pontos_shp = os.path.join(pasta_shape_sppo, f"{nome_pontos}.shp")
    endereco_pontos_gpkg = os.path.join(pasta_shape_sppo, f"{nome_pontos}.gpkg")
    
    # Shapefile version (rename for 10-char limit)
    gdf_pontos_shp = gdf_pontos.copy()
    gdf_pontos_shp.rename(columns={
        'location_type': 'loc_type',
        'parent_station': 'parent_sta',
        'stop_timezone': 'timezone',
        'wheelchair_boarding': 'wheelchair',
        'platform_code': 'platform'
    }, inplace=True)
    
    gdf_pontos_shp.to_file(endereco_pontos_shp, driver='ESRI Shapefile', engine='pyogrio')
    gdf_pontos.to_file(endereco_pontos_gpkg, driver='GPKG', engine='pyogrio')
    
    print(f"✓ Salvo {nome_pontos}.shp (colunas ajustadas para o limite de 10 caracteres)")
    print(f"✓ Salvo {nome_pontos}.gpkg (com nomes de colunas originais)")
else:
    print("Aviso: stops.txt não disponível ou vazio.")

print("\n✓ Geração de geometrias e shapefiles finalizada com sucesso!")
