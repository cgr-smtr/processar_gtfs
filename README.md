# Processamento de GTFS - Rio de Janeiro

Este repositório contém uma pipeline de processamento para dados GTFS (General Transit Feed Specification) da cidade do Rio de Janeiro (SPPO, BRT e Frescão). O objetivo é realizar a limpeza, o ajuste de horários com base em dados históricos de GPS, a gestão de desvios de itinerário e a geração de produtos para análise (GIS e relatórios).

A pipeline está implementada tanto em **Python** (`codigos_py/`) quanto em **R** (`codigos_R/`).

## 🚀 Pipeline de Processamento (7 Passos)

O processamento é dividido em sete etapas sequenciais, cada uma representada por um script numerado:

1.  **`1_extrair_qh_especificado_no_gtfs.py`**:
    *   Extrai o Quadro de Horários (QH) diretamente dos arquivos `frequencies.txt` e `trips.txt` de um ZIP GTFS.
    *   Filtra por linhas e serviços específicos para gerar arquivos CSV individuais por serviço.
2.  **`2_ajustar_stop_times.py`**:
    *   Recalcula os horários de chegada (`arrival_time`) e partida (`departure_time`) em todas as paradas.
    *   Utiliza velocidades médias históricas de GPS (armazenadas em Parquet/CSV) para garantir que os tempos previstos sejam realistas.
    *   Corrige inconsistências em `shape_dist_traveled` e preenche horários faltantes.
3.  **`3_desvios.py`**:
    *   Gerencia o calendário de operações sincronizando desvios de itinerário temporários.
    *   Lê fontes externas (planilhas de desvios) e ajusta o `calendar_dates.txt` para ativar ou desativar serviços específicos durante eventos.
4.  **`4_trajetos_alternativos.py`**:
    *   Identifica viagens que utilizam percursos alternativos (detecção via `trip_headsign`).
    *   Calcula extensões (quilometragem) desses trajetos usando a fórmula de Haversine e gera relatórios de OS (Ordem de Serviço).
5.  **`5_juntar_gtfs.py`**:
    *   Combina os feeds de SPPO e BRT em um único arquivo GTFS consolidado.
    *   Realiza a limpeza final (remoção de colunas redundantes e trips "fantasma") e aplica a paleta de cores oficial às rotas.
6.  **`6_gerar_shapes.py`**:
    *   Gera arquivos de geometria (Shapefile e GeoPackage) para visualização em GIS (QGIS, ArcGIS).
    *   Processa tanto os trajetos (`shapes.txt`) quanto os pontos de parada (`stops.txt`) com metadados enriquecidos.
7.  **`7_lista_partidas.py`**:
    *   Gera a lista consolidada de todas as partidas programadas para o sistema.
    *   Expande frequências em horários discretos e gera relatórios em CSV e Parquet, organizados por tipo de dia (DU, SAB, DOM).

## 🛠️ Pré-requisitos

### Python
As principais bibliotecas necessárias são:
*   `pandas` e `numpy`
*   `geopandas`, `shapely`, `pyogrio` (para GIS)
*   `zipfile`, `pathlib`
*   `pyarrow` (para leitura de arquivos Parquet)

### R
Depende de pacotes como:
*   `gtfstools`, `dplyr`, `purrr`, `readr`, `data.table`

## 📁 Estrutura de Pastas Esperada

```text
/processar_gtfs
  ├── codigos_py/           # Scripts Python da pipeline
  ├── codigos_R/            # Scripts R equivalentes
  ├── ../dados/             # Insumos (GTFS originais, GPS, Planilhas)
  └── ../resultados/        # Saídas (GTFS processados, Relatórios, GIS)
```

## 📝 Como Usar

1.  Certifique-se de que os dados de entrada (GTFS e arquivos de GPS) estão nas pastas corretas conforme definido nos parâmetros iniciais dos scripts.
2.  Execute os scripts na ordem numérica (1 a 7).
3.  Os arquivos GTFS finais serão gerados na pasta `../dados/gtfs/` com os sufixos `_PROC.zip` ou `_pub.zip`.