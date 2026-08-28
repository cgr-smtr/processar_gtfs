# Resumo dos Códigos Python — `codigos_py/`

Os scripts formam um **pipeline sequencial** de processamento de dados GTFS para o transporte público do Rio de Janeiro. Cada script lê a saída do anterior e produz insumos para o próximo.

---

## 🖥️ Interface Gráfica (Streamlit)

O projeto agora conta com uma aplicação web amigável desenvolvida em Streamlit para orquestrar a execução de todos os scripts do pipeline.

### Como Iniciar a Aplicação
1. Certifique-se de que as dependências estão instaladas (`pip install -r requirements.txt`).
2. Execute o comando abaixo na raiz do projeto:
```bash
python -m streamlit run src/app.py
```
3. Acesse pelo navegador em `http://localhost:8501`.

### Como Funciona
A interface possui uma barra lateral (Sidebar) para configuração de variáveis globais como `BASE_DADOS`, ano, mês e quinzena/estudo. O menu principal está dividido em abas, agrupando os scripts por finalidade (por exemplo, todos os scripts de "Juntar GTFS" na aba 5). O usuário pode fornecer parâmetros via interface (campos de texto ou upload de arquivos) e o script executará em segundo plano, com os logs (stdout) sendo exibidos em tempo real na tela.

### Execução via CLI (`--config`)
Todos os 10 scripts do repositório foram refatorados para aceitar um argumento dinâmico `--config` apontando para um JSON temporário gerado pela interface com todos os parâmetros a serem aplicados. Caso nenhum `--config` seja passado (como na execução clássica pelo terminal), os scripts mantêm **retrocompatibilidade** e usam as variáveis _hardcoded_ definidas no topo de cada arquivo Python.

---

## 0️⃣ `0_validar_gtfs_entrada.py`
**Objetivo:** Validar a integridade do GTFS de entrada **antes** de iniciar o pipeline de processamento, detectando problemas antecipadamente.

| Item | Descrição |
|------|-----------|
| **Entrada** | Arquivo GTFS ZIP original (SPPO, BRT ou Rio) — tabelas `routes.txt`, `trips.txt`, `stop_times.txt`, `calendar.txt`, `calendar_dates.txt`, `shapes.txt` |
| **Validações** | 1. Todas as routes possuem pelo menos uma trip associada |
|  | 2. Trips de excepcionalidade (com `[...]` no `trip_headsign`) seguem o padrão: sem acentos, espaços substituídos por `_`, apenas minúsculas e caracteres `[a-z0-9_]` |
|  | 3. (Extra) Trips sem `route_id` preenchido |
|  | 4. (Extra) Inventário de `service_id` utilizados |
| **Saída** | Relatório CSV com timestamp em `resultados/validacoes_snapshot/validacao_gtfs_entrada_<tipo>_<sufixo>_<timestamp>.csv` |
| **Retorno** | `sys.exit(0)` se todas as validações críticas passaram; `sys.exit(1)` se houver falhas |
| **Dependências** | `pandas`, `zipfile`, `re`, `unicodedata`, `pathlib` |

> [!TIP]
> Execute este script antes do `2_ajustar_stop_times.py`. Ele gera sugestões automáticas de correção para os `trip_headsign` fora do padrão.

---

## 1️⃣ `1_extrair_qh_especificado_no_gtfs.py`
**Objetivo:** Extrair o Quadro Horário (QH) de linhas específicas, a partir do GTFS combinado.

| Item | Descrição |
|------|-----------|
| **Entrada** | Arquivo GTFS ZIP combinado (`gtfs_combi_YYYY-MM-QQ.zip`) — tabelas `frequencies.txt` e `trips.txt` |
| **Filtros** | Lista de linhas (`linhas_rodar`) e calendários (`services_to_run`, ex: `U_REG`, `S_REG`, `D_REG`) |
| **Processamento** | Junta `frequencies` com `trips`, filtra por linha e serviço, identifica combinações únicas de (serviço, vista, calendário) |
| **Saída** | Um CSV por combinação em `resultados/quadro_horario_extraido/YYYY/MM/qh_por_linha/QQ/` com colunas: `trip_id`, `trip_headsign`, `trip_short_name`, `start_time`, `end_time`, `headway_secs` |
| **Dependências** | `pandas`, `zipfile`, `pathlib` |

---

## 2️⃣ `2_ajustar_stop_times.py`
**Objetivo:** Recalcular os horários de parada (`stop_times`) do GTFS usando **velocidades reais extraídas de dados GPS** de viagens realizadas.

| Item | Descrição |
|------|-----------|
| **Entrada** | GTFS original (SPPO ou BRT) + dados de viagens reais (Parquet/CSV) + calendário de feriados (`calendario.json`) |
| **Etapas** | 1. Carrega viagens GPS (BRT=CSV, SPPO=Parquet + Frescão) |
|  | 2. Calcula sumários de velocidade média por (serviço, direção, hora, tipo de dia) com remoção de outliers via IQR |
|  | 3. Lê GTFS, ajusta `shape_dist_traveled` para começar em 0, corrige horários faltantes usando distância + velocidade padrão |
|  | 4. Recalcula `arrival_time`/`departure_time` usando velocidades GPS reais, garantindo monotonicidade |
|  | 5. Valida integridade (nenhum horário vazio/nulo) |
| **Saída** | Novo ZIP GTFS com sufixo `_PROC.zip` (apenas `stop_times.txt` e `routes.txt` são substituídos) |
| **Dependências** | `pandas`, `numpy`, `zipfile`, `json` |

> [!IMPORTANT]
> Este é o script mais complexo do pipeline (~584 linhas). Ele é o coração do ajuste de qualidade do GTFS.

---

~~## 3️⃣ `3_desvios_nao-utilizar.py`~~

> Este código está datado e **não tem mais utilidade**. Usar `4_trajetos_alternativos.py` para trabalhar com excepcionalidades.

---

## 4️⃣ `4_trajetos_alternativos.py`
**Objetivo:** Gerar um **relatório CSV** dos trajetos alternativos (desvios), listando serviços, vistas, consórcios, sentidos e extensões em km.

| Item | Descrição |
|------|-----------|
| **Entrada** | GTFS processado (`_PROC.zip`) — tabelas `trips`, `routes`, `agency`, `shapes` |
| **Processamento** | 1. Identifica viagens de desvio (trip_headsign contendo `[...]`) |
|  | 2. Remove frescões (`route_type == 200`) |
|  | 3. Calcula extensão geográfica dos shapes via GIS (projeção EPSG:31983) |
|  | 4. Agrupa por (serviço, vista, consórcio, sentido, evento) |
| **Saída** | CSV em `dados/os/os_YYYY-MM-QQ_excep.csv` com colunas: Serviço, Vista, Consórcio, Sentido, Extensão, Evento |
| **Dependências** | `pandas`, `geopandas`, `shapely` |

---

## 5️⃣ `5_juntar_gtfs.py`
**Objetivo:** **Combinar** os GTFS processados do SPPO e do BRT em um único GTFS unificado, aplicar limpezas, cores e gerar versão pública.

| Item | Descrição |
|------|-----------|
| **Entrada** | `sppo_YYYY-MM-QQ_PROC.zip` + `brt_YYYY-MM-QQ_PROC.zip` + insumos (cores, trip_id_fantasma, arquivos de substituição) |
| **Etapas** | 1. Carrega e processa SPPO: define `route_type` (700/200), ajusta `service_id`, remove trips fantasma e sem stop_times |
|  | 2. Carrega e processa BRT: preserva `route_type` original, ajusta `service_id` |
|  | 3. Concatena todos os arquivos GTFS (trips, routes, stops, shapes, etc.) |
|  | 4. Limpeza: remove paradas "APAGAR", colunas desnecessárias, ordena shapes, valida horários |
|  | 5. Salva GTFS combinado e substitui arquivos (calendar_dates, fare_attributes, fare_rules, feed_info) com insumos externos |
|  | 6. Gera versão pública: remove trips EXCEP, aplica cores personalizadas (`gtfs_cores.csv`), salva `gtfs_rio-de-janeiro_pub.zip` |
| **Saída** | `gtfs_combi_YYYY-MM-QQ.zip` (interno) + `gtfs_rio-de-janeiro_pub.zip` (público) |
| **Dependências** | `pandas`, `numpy`, `zipfile` |

> [!TIP]
> A função `clean_gtfs()` implementa uma limpeza em cascata equivalente ao `gtfstools::filter_by_trip_id` do R — remove registros órfãos de todas as tabelas associadas.

---

## 5️⃣.1️⃣ `5.1_juntar_gtfs_sppo.py`
**Objetivo:** Variante do script 5 para processar **um único GTFS** (SPPO, BRT ou Rio), sem combinar com outro modal. Gera o GTFS combinado e público a partir de uma única fonte.

| Item | Descrição |
|------|-----------|
| **Entrada** | Um único GTFS processado (`_PROC.zip`) do tipo `sppo`, `brt` ou `rio` + insumos de substituição |
| **Diferencial** | Suporta múltiplas **etapas** do GTFS Rio (ex: `"ETAPA_01,ETAPA_02"`), agregando `calendar_dates` e demais arquivos de substituição de múltiplas pastas |
| **Etapas** | Idênticas ao script 5, mas para uma única fonte (sem etapa de combinação SPPO+BRT) |
| **Pastas de substituição** | Resolução hierárquica: busca pastas específicas por etapa, com fallback para a pasta base |
| **Saída** | `gtfs_combi_YYYY-MM-QQ.zip` (interno) + `gtfs_rio-de-janeiro_pub.zip` (público) |
| **Dependências** | `pandas`, `numpy`, `zipfile` |

> [!NOTE]
> Use este script quando apenas **um modal** (ex: somente Rio) precisa ser publicado, sem combinar SPPO+BRT.

---

## 5️⃣.2️⃣ `5.2_juntar_gtfs_simples.py`
**Objetivo:** Concatenação simples de **N arquivos GTFS ZIP** em um único arquivo, sem limpezas avançadas ou lógica de negócio. Útil para testes e inspeções rápidas.

| Item | Descrição |
|------|-----------|
| **Entrada** | Lista de arquivos GTFS ZIP (`INPUT_ZIPS`) configurada diretamente no script |
| **Processamento** | 1. Lê todos os ZIPs e concatena cada tabela `.txt` |
|  | 2. Remove linhas exatamente duplicadas de cada tabela |
| **Saída** | Um único ZIP em `OUTPUT_ZIP` configurado no script |
| **Dependências** | `pandas`, `zipfile` |

---

## 5️⃣.3️⃣ `5.3_juntar_gtfs_substituicao.py`
**Objetivo:** Juntar dois arquivos GTFS garantindo que as linhas (`route_short_name`) do segundo GTFS substituam integralmente as linhas correspondentes no primeiro GTFS.

| Item | Descrição |
|------|-----------|
| **Entrada** | Dois arquivos GTFS ZIP (`GTFS_1_PATH` e `GTFS_2_PATH`) |
| **Processamento** | 1. Carrega ambos os GTFSs. <br>2. Identifica rotas redundantes no GTFS 1 com base no `route_short_name` do GTFS 2. <br>3. Remove essas rotas do GTFS 1 e aplica a limpeza em cascata (`clean_gtfs`) para remover registros órfãos. <br>4. Concatena os registros restantes do GTFS 1 com o GTFS 2. <br>5. Elimina duplicatas exatas. |
| **Saída** | GTFS unificado salvo em `OUTPUT_ZIP` |
| **Dependências** | `pandas`, `numpy`, `zipfile`, `pathlib` |

---

## 5️⃣.4️⃣ `5.4_eliminar_duplicatas_gtfs.py`
**Objetivo:** **Eliminar duplicatas em linhas regulares** em arquivos GTFS (`gtfs_combi`, `gtfs_pub` ou qualquer GTFS indicado), **preservando 100% das viagens de excepcionalidades (`EXCEP` e desvios)**.

| Item | Descrição |
|------|-----------|
| **Entrada** | Caminho do arquivo GTFS ZIP no PC (`endereco_gtfs`, ex: `C:/R_SMTR/dados/GTFS/2027/gtfs_combi_2026-07-02Q.zip`) |
| **Critério de preservação** | Viagens com `service_id` contendo `EXCEP` ou com `[...]` no `trip_headsign` são mantidas 100% intactas sem qualquer exclusão |
| **Processamento** | 1. Separa viagens regulares de viagens `EXCEP`. <br>2. Identifica rotas regulares duplicadas por `route_short_name` e unifica `route_id` mantendo a primeira ocorrência. <br>3. Remove trips regulares redundantes por assinatura `(serviço, direction_id, service_id, horários/frequência de partida)`. <br>4. Aplica `clean_gtfs` em cascata para remover registros órfãos nas demais tabelas (`stop_times`, `shapes`, `stops`, `calendar`, etc.). |
| **Saída** | Arquivo GTFS atualizado sem duplicatas (sobrescreve o original ou salva no caminho definido em `caminho_saida`) |
| **Dependências** | `pandas`, `numpy`, `zipfile`, `pathlib` |

---

## 9️⃣ `9_filtrar_gtfs_por_lista.py`
**Objetivo:** Gerar um **GTFS filtrado** contendo apenas as trips presentes em uma lista de excepcionalidades/desvios, combinando cinco critérios de correspondência.

| Item | Descrição |
|------|-----------|
| **Entrada** | GTFS processado (`_PROC.zip`) + lista embutida no script (TSV com colunas Serviço, Vista, Consórcio, Sentido, Extensão, Evento) |
| **Critério de filtro** | Junção por chave composta: `trip_short_name` + `route_long_name` + `agency_name` + `direction_id` + evento extraído do `trip_headsign` (`[...]`) |
| **Processamento** | 1. Filtra trips pelo calendário alvo (padrão: `EXCEP`) |
|  | 2. Enriquece trips com `route_long_name` e `agency_name` via join |
|  | 3. Extrai o evento do `trip_headsign` e compõe a chave de junção |
|  | 4. Aplica `clean_gtfs` em cascata para remover stop_times, shapes, stops e calendar_dates órfãos |
|  | 5. Reporta entradas da lista sem match no GTFS |
| **Saída** | `sppo_YYYY-MM-QQ_FILTRADO.zip` |
| **Dependências** | `pandas`, `numpy`, `zipfile` |

> [!TIP]
> A coluna **Extensão** da lista de filtro é apenas referência — não entra na chave de correspondência. O script reporta explicitamente quais entradas da lista **não tiveram match** no GTFS.

---

## 1️⃣0️⃣ `10_juntar_dois_gtfs.py`
**Objetivo:** **Juntar dois arquivos GTFS** em um único, eliminando duplicatas com controle de precedência por chave primária GTFS.

| Item | Descrição |
|------|-----------|
| **Entrada** | `GTFS_PRINCIPAL` (tem precedência) + `GTFS_SECUNDARIO` — tipicamente o GTFS filtrado + o GTFS completo |
| **Estratégia de deduplicação** | Para cada tabela `.txt`: |
|  | 1. Remove linhas 100% idênticas (`drop_duplicates` na linha inteira) |
|  | 2. Remove conflitos de chave primária, mantendo o registro do PRINCIPAL (`keep='first'`) |
| **Chaves primárias** | Definidas em `CHAVES_PRIMARIAS` por tabela: `trip_id` para trips, `[trip_id, stop_sequence]` para stop_times, `[shape_id, shape_pt_sequence]` para shapes, etc. |
| **Pós-processamento** | Aplica `clean_gtfs` para remover registros órfãos resultantes da fusão |
| **Saída** | `sppo_YYYY-MM-QQ_COMBINADO.zip` |
| **Dependências** | `pandas`, `numpy`, `zipfile` |

> [!IMPORTANT]
> O GTFS definido em `GTFS_PRINCIPAL` **sempre vence** em caso de conflito de chave primária. Use o GTFS filtrado (saída do script 9) como principal para garantir que os trajetos alternativos corretos prevaleçam.

---

## 6️⃣ `6_gerar_shapes.py`
**Objetivo:** Gerar **arquivos geoespaciais** (Shapefile + GeoPackage) dos trajetos (linhas) e pontos de parada a partir do GTFS público.

| Item | Descrição |
|------|-----------|
| **Entrada** | GTFS público (`gtfs_rio-de-janeiro_pub.zip`) + `descricao_desvios.csv` (opcional) |
| **Processamento** | 1. Ordena shapes e remove inválidos (< 2 pontos) |
|  | 2. Prioriza shapes por tipo de serviço: U_REG → *_REG → especial U → outros |
|  | 3. Converte coordenadas em `LineString`, projeta para EPSG:31983, calcula extensão |
|  | 4. Enriquece com metadados: consórcio, tipo de rota (regular/BRT/frescão), tarifas, descrição de desvios |
|  | 5. Exporta trajetos (linhas) e pontos de parada |
| **Saída** | Em `dados/shapes/YYYY/`: |
|  | - `shapes_trajetos_YYYY-MM-QQ.shp` + `.gpkg` (trajetos como LineStrings) |
|  | - `shapes_pontos_YYYY-MM-QQ.shp` + `.gpkg` (paradas como Points, com `route_type` agregado) |
| **Dependências** | `pandas`, `numpy`, `geopandas`, `shapely`, `pyogrio` |

---

## 7️⃣ `7_lista_partidas.py`
**Objetivo:** Gerar a **lista completa de partidas** (horários de saída) por tipo de dia, consolidando viagens por frequência e por quadro horário regular.

| Item | Descrição |
|------|-----------|
| **Entrada** | GTFS público (`gtfs_rio-de-janeiro_pub.zip`) |
| **Processamento** | 1. Filtra frescões e trips fantasma |
|  | 2. Calculates extensões dos shapes via GIS (EPSG:31983) |
|  | 3. Para cada tipo de dia (DU/SAB/DOM): |
|  |    a. Expande `frequencies.txt` em partidas individuais (start → end, incrementando headway) |
|  |    b. Extrai horários do primeiro ponto (`stop_sequence=0`) para linhas sem frequência |
|  |    c. Enriquece com nome da rota, agência, extensão, faixa horária |
|  |    d. Calcula intervalos entre partidas consecutivas |
| **Saída** | Em `resultados/partidas/`: |
|  | - `partidas_du.csv`, `partidas_sab.csv`, `partidas_dom.csv` (individuais com intervalo) |
|  | - `partidas.csv` + `partidas.parquet` (consolidado de todos os dias) |
| **Dependências** | `pandas`, `numpy`, `geopandas`, `shapely`, `pyarrow` |

---

## 8️⃣ `8_gerar_extensoes.py`
**Objetivo:** Gerar uma listagem consolidada de linhas por sentido e vista, com o cálculo da maior extensão (em metros) para cada serviço.

| Item | Descrição |
|------|-----------|
| **Entrada** | GTFS público (`gtfs_rio-de-janeiro_pub.zip`) |
| **Processamento** | 1. Filtra rotas SPPO (route_type=3 ou 700). |
|  | 2. Remove viagens de exceção/desvio. |
|  | 3. Calculates extensões geográficas de todos os shapes (EPSG:31983). |
|  | 4. Consolida a maior extensão por (Serviço, Vista, Sentido). |
| **Saída** | CSV em `resultados/extensoes/extensoes_YYYY-MM-QQQ.csv` |
| **Dependências** | `pandas`, `geopandas`, `shapely` |

---

## 8️⃣.1️⃣ `8.1_gerar_extensoes_rio.py`
**Objetivo:** Variante do script 8 para processar arquivos GTFS no formato simplificado `rio_YYYY-MM.zip`.

| Item | Descrição |
|------|-----------|
| **Entrada** | GTFS em `dados/gtfs/YYYY/rio_YYYY-MM.zip` |
| **Processamento** | Idêntico ao script 8, mas adaptado para o novo padrão de nomenclatura de arquivos. |
| **Saída** | CSV em `resultados/extensoes/extensoes_rio_YYYY-MM.csv` |

---

## 📊 Visão Geral do Pipeline

```mermaid
graph TD
    ENTRADA["GTFS Original SPPO/BRT/Rio"]
    A0["0 - Validar GTFS (opcional)"]
    B["2 - Ajustar Stop Times GPS"]
    D["4 - Trajetos Alternativos"]
    E5["5 - Juntar GTFS SPPO+BRT"]
    E51["5.1 - Juntar GTFS único modal"]
    E52["5.2 - Concatenar simples"]
    E53["5.3 - Juntar com substituição"]
    E54["5.4 - Eliminar Duplicatas GTFS"]
    E9["9 - Filtrar GTFS por Lista"]
    E10["10 - Juntar Dois GTFS"]
    PUB["gtfs_combi.zip + pub.zip"]
    FILTRADO["GTFS Filtrado (_FILTRADO.zip)"]
    COMBINADO["GTFS Combinado (_COMBINADO.zip)"]
    F["6 - Gerar Shapes"]
    G["7 - Lista Partidas"]
    H["8 - Gerar Extensões"]
    A["1 - Extrair QH"]
    OUT4["os_excep.csv"]
    OUT6["Arquivos GIS"]
    OUT7["Lista de partidas"]
    OUT8["extensoes.csv"]
    OUT1["Quadros horários"]
    LISTA["Lista de Excepcionalidades CSV/TSV"]
 
    ENTRADA --> A0
    ENTRADA --> B
    B --> D
    B --> E5
    B --> E51
    B --> E53
    B --> E9
    D --> OUT4
    LISTA --> E9
    E9 --> FILTRADO
    FILTRADO --> E10
    ENTRADA --> E10
    E10 --> COMBINADO
    E5 --> PUB
    E51 --> PUB
    E52 --> PUB
    E53 --> PUB
    PUB --> E54
    E54 --> PUB
    PUB --> F
    PUB --> G
    PUB --> H
    PUB --> A
    F --> OUT6
    G --> OUT7
    H --> OUT8
    A --> OUT1
 
    style A0 fill:#e07b39,color:#fff
    style B fill:#4a90d9,color:#fff
    style D fill:#e8a838,color:#fff
    style E5 fill:#50b848,color:#fff
    style E51 fill:#50b848,color:#fff
    style E52 fill:#50b848,color:#fff
    style E53 fill:#50b848,color:#fff
    style E54 fill:#50b848,color:#fff
    style E9 fill:#c0392b,color:#fff
    style E10 fill:#c0392b,color:#fff
    style F fill:#9b59b6,color:#fff
    style G fill:#9b59b6,color:#fff
    style H fill:#9b59b6,color:#fff
    style A fill:#9b59b6,color:#fff
    style LISTA fill:#f39c12,color:#fff
    style FILTRADO fill:#e74c3c,color:#fff
    style COMBINADO fill:#e74c3c,color:#fff
```

> [!NOTE]
> O script **0** é opcional mas recomendado antes de iniciar o pipeline. O script **2** é a entrada principal para todos os modais. Os scripts **5**, **5.1**, **5.2** e **5.3** são alternativas de combinação; o script **5.4** elimina duplicatas em linhas regulares (preservando EXCEP) nos GTFS combinados/públicos. Os scripts **4**, **6**, **7**, **8** e **8.1** são etapas de pós-processamento/exportação independentes. Os scripts **9** e **10** formam um sub-fluxo independente para geração de GTFSs com excepcionalidades/desvios selecionados.
