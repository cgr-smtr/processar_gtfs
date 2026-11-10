---
name: gtfs-pipeline
description: GTFS pipeline for Rio transit — steps, files, invariants.
version: 0.1.0
author: Upstage (Hermes Agent)
license: MIT
platforms: [linux, macos, windows]
metadata:
  hermes:
    tags: [gtfs, rio, pipeline, processamento-de-dados]
    related_skills: []
---

# GTFS Pipeline Skill

Pipeline de processamento/validação de GTFS do transporte público do Rio de
Janeiro. Ajusta stop_times por GPS, combina modais SPPO/BRT/Rio, gera shapes,
partidas, extensões e GTFS filtrados. Python + pandas/geopandas, orquestrado por
`src/app.py` (Streamlit) e pelos scripts em `codigos_py/`.

## When to Use

- Partir de um novo arquivo GTFS para rodar o pipeline.
- Precisar saber qual script rodar e qual saída alimenta qual entrada.
- Corrigir um bug em uma etapa específica (partidas, QH, shapes, extensões).
- Lembrar das convenções: `--config <json>`, EPSG:31983, `trip_id` vazio por
  design, agrupamento por `(ciclo, faixa)`.
- Não usar para validação OS × GTFS: esse pertence ao projeto irmão
  `validacao_gtfs` (scripts `0_validar_gtfs_entrada.py`,
  `1_validar_gtfs.py`, `2_comparar_os_gtfs.py`).

## Prerequisites

- Ambiente Python com pandas, geopandas, pyogrio, openpyxl (veja `requirements.txt`).
- Variáveis `BASE_DADOS` (GTFS + viagens) e `BASE_RESULTADOS` (saídas).
- Todos os scripts aceitam `--config <json>`; manhem retrocompatibilidade com
  variáveis hardcoded no topo (`BASE_DADOS`, ano, mês, estudo).
- Leitura de CSVs/GTFS com `dtype=str`. Projeção geográfica EPSG:31983.
- Saídas em `resultados/` e `dados/`.
- Scripts numerados 1 a 10, com variantes `.1`/`.2`, e um bloco `11_`
  (investigação/comparação OS).

## How to Run

GUI (orquestra todas as abas):

```
python -m streamlit run src/app.py
```

CLI por etapa:

```
python codigos_py/<script>.py --config config.json
```

O `--config` aponta para um JSON com as chaves de cada script (veja cada bloco
abaixo). Sem `--config`, o script usa as variáveis `hardcoded` no topo (retrocompatibilidade).

## Quick Reference

| Passo | Script | Entrada | Saída | Execução |
|-------|--------|---------|-------|----------|
| 1 extrair QH faixas | `1_extrair_qh_especificado_no_gtfs.py` | GTFS: `frequencies.txt` + `trips.txt` | CSV por `(servico,vista,calendario)` em `qh_por_linha/` | CLI |
| 1.1 extrair partidas | `1.1_extrair_qh_partidas.py` | `trips`, `frequencies`, `stop_times` | CSV `partidas_<servico>_<vista>_<calendario>.csv` | CLI |
| 1.2 combinar QH | `1.2_combinar_quadros_horarios.py` | CSVs do script 1 | CSV em `qh_por_linha_combinado/` | CLI |
| 1.3 QH a partir de contagens | `1.3_qh_a_partir_de_partidas.py` | Tabela de contagens (longo ou wide) | CSV + Excel + relatório + gate | CLI |
| 1.3.1 conferir | `1.3.1_conferir_partidas_geradas.py` | Partidas + quadros gerados | Excel/relatório, exit 0/1 | CLI |
| 1.4 QH ponto facultativo | `1.4_qh_pf_a_partir_do_gtfs.py` | `frequencies.txt` de um GTFS | `qh_por_linha_pf_gtfs/{estudo}Q/` | CLI |
| 2 ajustar stop_times | `2_ajustar_stop_times.py` | GTFS (sppo/brt/rio) + viagens GPS | GTFS processado (novo ZIP) | CLI |
| 4 trajetos alternativos | `4_trajetos_alternativos.py` | GTFS | trajetos alternativos | CLI |
| 5 juntar GTFS | `5_juntar_gtfs.py` (+ 5.1–5.4) | dois GTFSs | GTFS combinado | CLI |
| 6 gerar shapes | `6_gerar_shapes.py` | GTFS com `shapes.txt` | Shapefile/GPKG `shapes_trajetos_...` | CLI |
| 7 lista de partidas | `7_lista_partidas.py` | GTFS | `partidas_{tipo}.csv`/parquet | CLI |
| 7.1 / 7.2 | variações do 7 | GTFS | partidas detalhadas | CLI |
| 8 gerar extensões | `8_gerar_extensoes.py` | GTFS | `extensoes_{sufixo}.csv` | CLI |
| 8.1 extensões Rio | `8.1_gerar_extensoes_rio.py` | GTFS | extensões ria | CLI |
| 9 filtrar GTFS | `9_filtrar_gtfs_por_lista.py` | GTFS + lista de filtro | GTFS filtrado | CLI |
| 10 juntar dois GTFS | `10_juntar_dois_gtfs.py` | dois GTFSs | GTFS combinado (sem duplicatas) | CLI |
| 11 investigar OS×GTFS | `11.1_investigar_os190_vs_gtfs0174.py` | OS + GTFS 0174 | CSVs + resumo | CLI |
| 11 comparar duas OS | `11_comparar_duas_os.py` | duas OS | CSVs + resumo | CLI |

## Procedure

### 1 → 1.1 (extrair QH e partidas de um GTFS)

1. Leia `frequencies.txt` e `trips.txt` do ZIP (com `dtype=str`).
2. Filtre por `linhas_rodar` e `services_to_run` (vazios = todas).
3. Para cada via com frequência, expanda o headway em `trip_id_freq_{seq}`.
4. Para viais sem frequência, pega o `departure_time` do primeiro ponto
   (`stop_sequence == 0`) em `stop_times.txt`.
5. Valide que `partidas_coletadas` não está vazio antes de agrupar.

> Check: arquivo `partidas_<servico>_<vista>_<calendario>.csv` gerado e
> `ordem_partida` começa em 1.

### 2 — Ajustar stop_times por GPS

1. Carregue as viagens (`validas`) de BRT (CSV) ou SPPO/Frescão (parquet).
2. Itere dias: útil `[2..6]`, sábado `[7]`, domingo `[1]`, mapeando de R para
   Python (`{1:6, 2:0, 3:1, 4:2, 5:3, 6:4, 7:5}`).
3. `ajustar_shape_dist_traveled()`: normaliza o `shape_dist_traveled` para começar
   em 0 por `trip_id`.
4. `corrigir_horarios_faltantes()`: estima hora inicial a partir de
   `frequencies.txt`, depois transporta tempo via distancia/velocidade média.
5. Reconcilie `arrival_time`/`departure_time`; garanta monotonicidade (cummax por
   `trip_id`).
6. Levante `relatorio_inconsistentes_<timestamp>.csv` e lance `ValueError` se
   faltarem horários.

> Check: o novo ZIP contém `stop_times.txt` e `routes.txt` substituídos; demais
> tabelas copiadas.

### 6 — Gerar shapes (GIS)

1. Lea `shapes.txt`, ordene por `shape_id` + `shape_pt_sequence`.
2. Descarte shapes com < 2 pontos (gravam `shapes_invalidos_com_trips.csv`).
3. Transforme coordenadas em `LineString` (EPSG:4326 → reproje para EPSG:31983).
4. Calcule `extensao = geometry.length.astype(int)` (m).
5. Exclua `AN31` e reverta a projeção para exportar Shapefile/GPKG.

> Check: `extensao` em metros, `crs` reverter para EPSG:4326 no export.

### 7 — Lista de partidas (por tipo de dia)

1. Leia `routes.txt`, `trips.txt`, `stop_times.txt`, `frequencies.txt`, `shapes.txt`.
2. Exclua frescões (`route_type == 200`) e viagens de "fantasma" (arquivo
   `trip_id_fantasma.txt` se existir).
3. Calcule extensões do script 8 no fundo (mesma regra: max do shape por
   itinerário, sem `EXCEP` e sem `[` no headsign).
4. Para cada `tipo_dia` em `du/sab/dom/pf`, expanda frequências e lide com QH.
5. Atribua `faixa` pela hora do dia (14 faixas padrão).
6. Salve `partidas_<tipo>.csv` (colunas: linha, vista, sentido, hora, faixa,
   extensão, intervalo) e consolidado `partidas_<sufixo>.csv`/`.parquet`.

> Check: `partidas_pf.csv` tem `extensao` preenchida e `intervalo` calculado.

### 8 — Listagem de linhas por sentido e extensão

1. Filtre `route_type` em `['3', '700']` (SPPO regular) e retire
   `service_id == 'EXCEP'` e headsign com `[`.
2. Reporte cada `shape_id` em um `LineString` e calcule extensão em EPSG:31983.
3. Defina `sentido`: "Circular" sobrescreve `direction_id` se todos os
   headsigns dessa linha forem "Circular".
4. Agregue `extensao_m` máximo por `(route_short_name, route_long_name, sentido)`.

> Check: `extensoes_<sufixo>.csv` com colunas Serviço/Vista/Sentido/Extensão.

### 1.3 — QH a partir de contagens de partidas (invariante central)

1. Leia a tabela de contagens: formato largo (colunas de faixas) ou longo
   (`faixa` + `partidas_faixa`); o script `melt`s o largo sozinho.
2. Valide: `sentido` em `{Ida, Volta, Circular}`, `faixa` membro das 14 padrão,
   sem duplicatas `(servico, sentido, faixa)`, contagens não negativas.
3. Para cada `(servico, sentido)` com total > 0: `h = (dur//n // 300)*300`,
   se `h==0` usa `dur//n` (a contagem é preservada, a granularidade pode ceder).
   `start = início da faixa`; `end = start + n*h` (sem vazamento).
4. Gate antes de gravar: reexpansão = entrada; nenhuma banda fora da faixa;
   headway múltiplo de 300s; total entrada == total saída.
5. Grave CSV (formato do script 1, `trip_id` vazio), Excel (aba por linha,
   bloco por quadro, partidas expandidas) + `relatorio_bandas.csv` + `_resumo.txt`.

> Check: `Gate: OK` impresso e total entrada == total saída.

## Pitfalls

- `trip_id` vazio nos CSVs de QH por **design** (Sigmob preenche ao subir).
- Nunca agrupe só por `faixa`: a faixa dá a volta em 24h e o gráfico é por
  `(ciclo, faixa)`.
- Extensão de linha: `max()` do comprimento GIS **sem** incluir trips `EXCEP` nem
  desvios (`[` no headsign) — senão o `max()` pega o shape de exceção e infla a
  extensão (ex.: 006 Ida 8460 → 8866). Scripts 8/8.1 filtram `[3,700]`;
  o script 7 **não** filtra `route_type` na extensão (cobre BRT 702).
- `shape_dist_traveled` do GTFS fica ~21 m acima do GIS (verdade do GTFS).
- Decimais/segundos nas partidas do script 7 herdados do `stop_times` ajustado
  por GPS (script 2); rode `.round('min')` apenas se o usuário pedir.
- `get_pattern(tipo_dia)` lança `ValueError` para token desconhecido
  (du→U, sab→S, dom→D, pf→F).
- O script 2 mapeia dias de R para Python com uma tabela;
  erro de mapeamento inverte sábado/domingo.

## Verification

- Rodar o teste do formato: `pytest tests/test_qh_format.py -q`.
- Script 1.3 impõe gate de concordância; quebra => `SystemExit(1)` (verifique a
  tabela de contagens).
- Script 1.3.1 confronta partidas geradas independentemente dos quadros do 1.3
  contra a entrada; retorna exit 0/1.
- Script 11 (`comparar_duas_os`) compara duas OS entre si; saída na raiz.
- Script 11.1 (`investigar_os190_vs_gtfs0174`) cruza OS×D_REG por serviço/sentido/
  faixa; a fórmula default `floor` (corrigida `floor_mais_1` em 2026-10-22) não
  deve duplicar fronteiras de bandas contíguas.

## See Also

- `AGENTS.md` — memória de sessão do projeto.
- `README.md` — resumo dos scripts (versão antiga, em português).
- `PLANO_1.3_qh_a_partir_de_partidas.md` — plano do script 1.3.
- Projeto irmão `validacao_gtfs` — validação OS × GTFS (scripts 0/1/2).
