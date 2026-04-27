import pandas as pd
import zipfile
import io
import os
from pathlib import Path

# Parameters
BASE_DADOS = Path("C:/R_SMTR/dados")
BASE_RESULTADOS = Path("C:/R_SMTR/resultados")

ano_gtfs = "2026"
mes_gtfs = "05"
estudo_gtfs = "01" #ESTUDO, NÃO CONSIDERAR MAIS QUINZENA!!!!

# GTFS file path
end_gtfs = BASE_DADOS / f"gtfs/{ano_gtfs}/gtfs_combi_{ano_gtfs}-{mes_gtfs}-{estudo_gtfs}Q.zip"

# Read GTFS (frequencies and trips) directly from the ZIP file
# We'll use pandas to read the specific CSVs from within the zip archive
print(f"Lendo arquivo GTFS de: {end_gtfs}")

with zipfile.ZipFile(end_gtfs, 'r') as z:
    with z.open('frequencies.txt') as f:
        frequencies = pd.read_csv(f)
    with z.open('trips.txt') as f:
        trips = pd.read_csv(f)

# Lines to run
linhas_rodar = ["249"]
services_to_run = ["U_REG","S_REG", "D_REG"] # 

# Process trips first: keep only needed columns and filter by requested lines
trips_filtered = trips[['trip_id', 'trip_short_name', 'trip_headsign', 'service_id']]

# Join frequencies with trips
# frequencies_desvios <- left_join(gtfs$frequencies, select(gtfs$trips, ...))
frequencias_desvios = pd.merge(frequencies, trips_filtered, on='trip_id', how='left')

# Filter
# filter(trip_short_name %in% linhas_rodar) %>% filter(service_id %in% c("S_REG", "D_REG"))
frequencias_desvios = frequencias_desvios[
    (frequencias_desvios['trip_short_name'].isin(linhas_rodar)) & 
    (frequencias_desvios['service_id'].isin(services_to_run))
]

# linhas_processar: distinct trip_short_name, trip_headsign, service_id
# arrange(trip_short_name, desc(service_id))
linhas_processar = frequencias_desvios[['trip_short_name', 'trip_headsign', 'service_id']]\
    .drop_duplicates()\
    .sort_values(by=['trip_short_name', 'service_id'], ascending=[True, False])

# Define the output directory based on R script
pasta_qh = BASE_RESULTADOS / f"quadro_horario_extraido/{ano_gtfs}/{mes_gtfs}/qh_por_linha/{estudo_gtfs}Q/"
# R equivalent to dir.create:
os.makedirs(pasta_qh, exist_ok=True)

print(f"Serão processados os seguintes serviços:\n{linhas_processar}")

# Iterate through distinct rows and save files
for index, row in linhas_processar.iterrows():
    servico = row['trip_short_name']
    vista = row['trip_headsign']
    calendario = row['service_id']
    
    # Filter the joined dataframe based on current row
    quadro_linha = frequencias_desvios[
        (frequencias_desvios['trip_short_name'] == servico) &
        (frequencias_desvios['trip_headsign'] == vista) &
        (frequencias_desvios['service_id'] == calendario)
    ].copy()
    
    # Arrange by start_time
    quadro_linha = quadro_linha.sort_values(by='start_time')
    
    # Mutate trip_id = "" and select specific columns
    quadro_linha['trip_id'] = ""
    quadro_linha = quadro_linha[['trip_id', 'trip_headsign', 'trip_short_name', 'start_time', 'end_time', 'headway_secs']]
    
    nome_arq = f"horarios_{servico}_{vista}_{calendario}.csv"
    caminho_arquivo = pasta_qh / nome_arq
    
    print(f"Salvando: {caminho_arquivo}")
    # Write to CSV
    quadro_linha.to_csv(caminho_arquivo, index=False)

print("Processamento concluído.")