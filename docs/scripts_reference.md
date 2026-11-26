# Catálogo Técnico de Scripts — `processar_gtfs`

Índice técnico pesquisável de todos os **23 scripts** do pipeline em `codigos_py/`, das **aplicações e bibliotecas** em `src/` e das **9 abas** da interface Streamlit em `src/tabs/`.

Para visão macro do sistema, fluxo entre etapas, detalhes de parâmetros e schemas de dados, consulte:
- [Arquitetura](./architecture.md)
- [Fluxo do Pipeline](./pipeline.md)
- [Configuração e Parâmetros](./configuration.md)
- [Dicionário de Dados](./data_dictionary.md)
- [Testes e Validação](./testing.md)
- [Solução de Problemas](./troubleshooting.md)

---

## Índice de Navegação

### Por Etapa / Grupo
- [Tabela-Índice Geral](#tabela-índice-geral)
- [Grupo A — Etapa 1: Quadros de Horários (QH)](#grupo-a--etapa-1-quadros-de-horários-qh)
- [Grupo B — Etapas 2 a 4: Ajuste de Stop Times e Desvios](#grupo-b--etapas-2-a-4-ajuste-de-stop-times-e-desvios)
- [Grupo C — Etapa 5: Junção, Substituição e Deduplicação de GTFS](#grupo-c--etapa-5-junção-substituição-e-deduplicação-de-gtfs)
- [Grupo D — Etapas 6 a 8: Geração de Produtos (Shapes, Partidas e Extensões)](#grupo-d--etapas-6-a-8-geração-de-produtos-shapes-partidas-e-extensões)
- [Grupo E — Etapas 9 e 10: Filtragem e Mesclagem por Chave Primária](#grupo-e--etapas-9-e-10-filtragem-e-mesclagem-por-chave-primária)
- [Grupo F — Aplicações Streamlit, Orquestrador e Bibliotecas Puras (`src/`)](#grupo-f--aplicações-streamlit-orquestrador-e-bibliotecas-puras-src)
- [Grupo G — Abas da Interface Streamlit (`src/tabs/`)](#grupo-g--abas-da-interface-streamlit-srctabs)
- [Legado R (`codigos_R/` e `1.2 app_format_qh.R`)](#legado-r-codigos_r-e-12-app_format_qhr)

### Por Categoria Funcional
- **Extração:** `1`, `1.1`, `4`, `7`, `8`, `8.1`
- **Transformação / Geração de QH:** `1.2`, `1.3`, `1.4`, `2`, `7.1`, `7.2`
- **Validação / Auditoria:** `1.3.1`
- **Carga:** `1.5`, `src/app_carregar_qh.py`
- **Junção / Limpeza:** `5`, `5.1`, `5.2`, `5.3`, `5.4`, `9`, `10`
- **Exportação GIS:** `6`
- **Interface / Orquestração:** `src/app.py`, `src/runner.py`, `src/app_format_qh.py`, `src/tabs/tab_*.py`
- **Biblioteca Pura:** `src/qh_format.py`, `src/gtfs_carregar_qh.py`
- **Descontinuado:** `3`

---

## Tabela-Índice Geral

> **Convenção CLI:** todos os 23 scripts de `codigos_py/` aceitam `--config <arquivo.json>`, **exceto** `3_desvios_nao-utilizar.py` (descontinuado, parâmetros hardcoded).

| Script | Categoria | Responsabilidade | Etapa / Domínio | Status |
|---|---|---|---|---|
| [`codigos_py/1_extrair_qh_especificado_no_gtfs.py`](../codigos_py/1_extrair_qh_especificado_no_gtfs.py) | Extração | Extrai quadros de horários por faixas de frequência (`frequencies.txt`) do GTFS para CSVs por linha, vista e calendário. | Etapa 1 — QH | Ativo |
| [`codigos_py/1.1_extrair_qh_partidas.py`](../codigos_py/1.1_extrair_qh_partidas.py) | Extração | Expande frequências e viagens de `stop_times.txt` em grades de partidas detalhadas com tempos de viagem e intervalo efetivo. | Etapa 1 — QH | Ativo |
| [`codigos_py/1.2_combinar_quadros_horarios.py`](../codigos_py/1.2_combinar_quadros_horarios.py) | Transformação | Combina faixas horárias de dois serviços distintos (`servico_origem` e `servico_destino`) em uma janela configurável (`hora_inicio`–`hora_fim`). | Etapa 1 — QH | Ativo |
| [`codigos_py/1.3_qh_a_partir_de_partidas.py`](../codigos_py/1.3_qh_a_partir_de_partidas.py) | Transformação | Gera quadros de horários factíveis (CSVs + Excel + relatório) a partir de tabela de contagens de partidas por faixa (formato longo ou wide). | Etapa 1 — QH | Ativo |
| [`codigos_py/1.3.1_conferir_partidas_geradas.py`](../codigos_py/1.3.1_conferir_partidas_geradas.py) | Validação / Auditoria | Audita de forma independente os CSVs e o Excel gerados pelo script `1.3` contra a tabela de entrada, retornando código de saída `0` ou `1`. | Etapa 1 — QH | Ativo |
| [`codigos_py/1.4_qh_pf_a_partir_do_gtfs.py`](../codigos_py/1.4_qh_pf_a_partir_do_gtfs.py) | Transformação | Gera quadros de ponto facultativo (`F_REG`) direto do `frequencies.txt` do GTFS aplicando regras por consórcio (`U_REG` vs `S_REG`) e excluindo MOBI-Rio (`20001`). | Etapa 1 — QH | Ativo |
| [`codigos_py/1.5_carregar_qh_no_gtfs.py`](../codigos_py/1.5_carregar_qh_no_gtfs.py) | Carga | Carrega CSVs de quadros de horários no GTFS via `src.gtfs_carregar_qh`, substituindo frequências com invariante 1:1 entre `trips.txt` e `frequencies.txt`. | Etapa 1 — QH | Ativo |
| [`codigos_py/2_ajustar_stop_times.py`](../codigos_py/2_ajustar_stop_times.py) | Transformação | Recalcula `arrival_time`, `departure_time` e `shape_dist_traveled` em `stop_times.txt` usando velocidades reais de GPS (Parquet) e gera `*_PROC.zip`. | Etapa 2 — Stop Times | Ativo |
| [`codigos_py/3_desvios_nao-utilizar.py`](../codigos_py/3_desvios_nao-utilizar.py) | Transformação | Script legado que aplicava calendários de desvios a partir de CSVs/Google Sheets diretamente no GTFS; não exposto na interface. | Etapa 3 — Desvios | Descontinuado |
| [`codigos_py/4_trajetos_alternativos.py`](../codigos_py/4_trajetos_alternativos.py) | Extração | Identifica trajetos alternativos/exceções (`[...]` no `trip_headsign`), calcula extensões GIS em `EPSG:31983` e exporta relatório CSV para a OS. | Etapa 4 — Alternativos | Ativo |
| [`codigos_py/5_juntar_gtfs.py`](../codigos_py/5_juntar_gtfs.py) | Junção | Combina GTFS SPPO e BRT, sobrescreve o ZIP do BRT após filtragem, aplica substituições de insumos e gera `gtfs_combi_*.zip` e `gtfs_rio-de-janeiro_pub.zip`. | Etapa 5 — Junção | Ativo |
| [`codigos_py/5.1_juntar_gtfs_sppo.py`](../codigos_py/5.1_juntar_gtfs_sppo.py) | Junção | Processa e empacota um único modal (`sppo`, `brt` ou `rio`) com ajustes de `service_id`, cores e insumos em `gtfs_combi_*.zip` e `gtfs_rio-de-janeiro_pub.zip`. | Etapa 5 — Junção | Ativo |
| [`codigos_py/5.2_juntar_gtfs_simples.py`](../codigos_py/5.2_juntar_gtfs_simples.py) | Junção | Concatena múltiplas tabelas de dois ou mais arquivos ZIP GTFS diretamente com `drop_duplicates()` por tabela, sem regras de domínio. | Etapa 5 — Junção | Ativo |
| [`codigos_py/5.3_juntar_gtfs_substituicao.py`](../codigos_py/5.3_juntar_gtfs_substituicao.py) | Junção | Substitui no GTFS base (`GTFS_1_PATH`) as rotas/viagens presentes no GTFS secundário (`GTFS_2_PATH`) e executa limpeza em cascata (`clean_gtfs`). | Etapa 5 — Junção | Ativo |
| [`codigos_py/5.4_eliminar_duplicatas_gtfs.py`](../codigos_py/5.4_eliminar_duplicatas_gtfs.py) | Limpeza | Remove viagens e frequências duplicadas dentro de um ou mais arquivos GTFS preservando exceções (`EXCEP`) e executando limpeza em cascata. | Etapa 5 — Limpeza | Ativo |
| [`codigos_py/6_gerar_shapes.py`](../codigos_py/6_gerar_shapes.py) | Exportação GIS | Converte `shapes.txt` e `stops.txt` do GTFS em camadas vetoriais Shapefile (`.shp`) e GeoPackage (`.gpkg`) com atributos de tarifa, consórcio e desvio. | Etapa 6 — Shapes | Ativo |
| [`codigos_py/7_lista_partidas.py`](../codigos_py/7_lista_partidas.py) | Extração | Expande frequências em lista individual de partidas por tipo de dia (`du`, `sab`, `dom`, `pf`), calcula extensão GIS (`EPSG:31983`) e exporta CSV/Parquet. | Etapa 7 — Partidas | Ativo |
| [`codigos_py/7.1_resumo_os_domingo_du.py`](../codigos_py/7.1_resumo_os_domingo_du.py) | Transformação | Gera tabela pivô de 14 faixas para domingo importando dia útil na janela configurada; inclui linhas exclusivas de dia útil e deixa km em branco. | Etapa 7 — Partidas | Evitar / com ressalva |
| [`codigos_py/7.2_combinador_partidas.py`](../codigos_py/7.2_combinador_partidas.py) | Transformação | Combina partidas de domingo (`00h–06h` e `20h+`) com dia útil (`06h–20h`) apenas para serviços operantes no domingo, compatibilizando intervalos de transição. | Etapa 7 — Partidas | Ativo |
| [`codigos_py/8_gerar_extensoes.py`](../codigos_py/8_gerar_extensoes.py) | Extração | Calcula a extensão máxima GIS em metros (`EPSG:31983`) por serviço e sentido para SPPO (`route_type` `3`/`700`), excluindo `EXCEP` e desvios `[`. | Etapa 8 — Extensões | Ativo |
| [`codigos_py/8.1_gerar_extensoes_rio.py`](../codigos_py/8.1_gerar_extensoes_rio.py) | Extração | Variante do script `8` parametrizada por `sufixo` livre (padrão `rio_YYYY-MM`) para gerar listagem de extensões SPPO (`EPSG:31983`). | Etapa 8 — Extensões | Ativo |
| [`codigos_py/9_filtrar_gtfs_por_lista.py`](../codigos_py/9_filtrar_gtfs_por_lista.py) | Limpeza | Filtra um GTFS mantendo apenas viagens que correspondem a uma lista CSV/TSV (serviço, vista, consórcio, sentido e evento `[...]`) com `clean_gtfs()`. | Etapa 9 — Filtragem | Ativo |
| [`codigos_py/10_juntar_dois_gtfs.py`](../codigos_py/10_juntar_dois_gtfs.py) | Junção | Mescla dois GTFS (`GTFS_PRINCIPAL` e `GTFS_SECUNDARIO`) resolvendo conflitos por tabela via mapa explícito de `CHAVES_PRIMARIAS` com prioridade ao principal. | Etapa 10 — Mesclagem | Ativo |
| [`src/app.py`](../src/app.py) | Interface / Orquestração | Aplicação Streamlit principal do pipeline com barra lateral de parâmetros globais e 9 abas correspondentes às etapas ativas. | Interface Principal | Ativo |
| [`src/runner.py`](../src/runner.py) | Orquestração | Executa scripts Python em subprocesso UTF-8 passando `--config <tmp.json>`, com barra de progresso assintótica e streaming de logs em tempo real. | Infraestrutura UI | Ativo |
| [`src/app_format_qh.py`](../src/app_format_qh.py) | Interface | Aplicação Streamlit standalone (sucessora do Shiny `1.2 app_format_qh.R`) para inspecionar, comparar e exportar quadros horários a partir de um GTFS. | App Standalone — QH | Ativo |
| [`src/qh_format.py`](../src/qh_format.py) | Biblioteca Pura | Implementa leitura de GTFS, conversão/desdobramento de horários, derivação de bandas de headway e tabelas comparativas sem dependência de Streamlit. | Biblioteca — QH | Ativo |
| [`src/app_carregar_qh.py`](../src/app_carregar_qh.py) | Interface / Carga | Aplicação Streamlit standalone para upload de GTFS e CSVs de QH, pré-visualização de alterações, autorização de clonagem de moldes e download do ZIP atualizado. | App Standalone — Carga | Ativo |
| [`src/gtfs_carregar_qh.py`](../src/gtfs_carregar_qh.py) | Biblioteca Pura | Implementa leitura/escrita GTFS, matching de rotas/headsigns, detecção de alterações, clonagem de moldes e aplicação 1:1 de `frequencies.txt`/`trips.txt`/`stop_times.txt`. | Biblioteca — Carga | Ativo |

---

## Grupo A — Etapa 1: Quadros de Horários (QH)

### `1_extrair_qh_especificado_no_gtfs.py`
- **Arquivo:** [`codigos_py/1_extrair_qh_especificado_no_gtfs.py`](../codigos_py/1_extrair_qh_especificado_no_gtfs.py)
- **Categoria:** Extração
- **Descrição:** Extrai faixas de frequência do GTFS em arquivos CSV individuais por serviço, sentido (`trip_headsign`) e calendário (`service_id`).
- **Responsabilidades:**
  - Ler `trips.txt` e `frequencies.txt` do ZIP GTFS informado.
  - Filtrar pelas listas `linhas_rodar` (`trip_short_name`) e `services_to_run` (`service_id`).
  - Ordenar faixas por `start_time` e exportar CSVs com `trip_id` intencionalmente vazio (preenchido posteriormente no sistema Sigmob).
- **Entradas:**
  - ZIP GTFS (`end_gtfs`, padrão `BASE_DADOS/gtfs/{ano}/gtfs_combi_{ano}-{mes}-{estudo}Q.zip`) contendo `trips.txt` e `frequencies.txt`.
  - Parâmetros `--config`: `BASE_DADOS`, `BASE_RESULTADOS`, `ano_gtfs`, `mes_gtfs`, `estudo_gtfs`, `end_gtfs`, `linhas_rodar`, `services_to_run`.
- **Saídas:**
  - Arquivos `horarios_{servico}_{vista}_{calendario}.csv` em `BASE_RESULTADOS/quadro_horario_extraido/{ano}/{mes}/qh_por_linha/{estudo}Q/` com colunas `trip_id`, `trip_headsign`, `trip_short_name`, `start_time`, `end_time`, `headway_secs`.
- **Dependências:** `pandas`, `zipfile`; consumidor típico do GTFS combinado gerado na Etapa 5 (`5_juntar_gtfs.py`); suas saídas alimentam `1.2_combinar_quadros_horarios.py` e `1.5_carregar_qh_no_gtfs.py`.
- **Execução:**
  - CLI: `python codigos_py/1_extrair_qh_especificado_no_gtfs.py --config config.json`
  - UI: `src/app.py` → Aba **1. Extrair QH** → modo **1**.

### `1.1_extrair_qh_partidas.py`
- **Arquivo:** [`codigos_py/1.1_extrair_qh_partidas.py`](../codigos_py/1.1_extrair_qh_partidas.py)
- **Categoria:** Extração
- **Descrição:** Gera a grade detalhada de horários de partida por linha, vista e calendário expandindo `frequencies.txt` e consultando `stop_times.txt`.
- **Responsabilidades:**
  - Ler `trips.txt`, `frequencies.txt` e `stop_times.txt` do GTFS.
  - Suportar filtro por lista de linhas ou processamento completo (`linhas_rodar = "todas"`).
  - Expandir faixas de frequência em partidas individuais com horários acima de `24:00:00` preservados e sanitizar nomes de arquivos de saída.
- **Entradas:**
  - ZIP GTFS (`end_gtfs`) contendo `trips.txt`, `frequencies.txt` e `stop_times.txt`.
  - Parâmetros `--config`: `BASE_DADOS`, `BASE_RESULTADOS`, `ano_gtfs`, `mes_gtfs`, `estudo_gtfs`, `end_gtfs`, `pasta_saida`, `linhas_rodar`, `services_to_run`.
- **Saídas:**
  - Arquivos `partidas_{servico}_{vista}_{calendario}.csv` em `pasta_saida` (padrão `BASE_RESULTADOS/quadro_horario_extraido/{ano}/{mes}/qh_partidas_por_linha/{estudo}Q/`).
- **Dependências:** `pandas`, `zipfile`; consome GTFS gerado na Etapa 5 ou Etapa 2.
- **Execução:**
  - CLI: `python codigos_py/1.1_extrair_qh_partidas.py --config config.json`
  - UI: `src/app.py` → Aba **1. Extrair QH** → modo **1.1**.

### `1.2_combinar_quadros_horarios.py`
- **Arquivo:** [`codigos_py/1.2_combinar_quadros_horarios.py`](../codigos_py/1.2_combinar_quadros_horarios.py)
- **Categoria:** Transformação
- **Descrição:** Combina quadros horários CSV de dois calendários distintos, injetando as faixas de `servico_origem` dentro da janela `[hora_inicio, hora_fim)` sobre o quadro de `servico_destino`.
- **Responsabilidades:**
  - Identificar pares de arquivos `horarios_{servico}_{vista}_{service_id}.csv` na `pasta_entrada` via `parse_filename()`.
  - Recortar faixas horárias nas fronteiras `hora_inicio` e `hora_fim` (padrão `06:00`–`21:00`).
  - Mesclar as faixas resultantes ordenadas por `start_time` mantendo `trip_id` vazio.
- **Entradas:**
  - CSVs no formato do script `1` localizados em `pasta_entrada`.
  - Parâmetros `--config`: `BASE_DADOS`, `BASE_RESULTADOS`, `ano_gtfs`, `mes_gtfs`, `estudo_gtfs`, `pasta_entrada`, `pasta_saida`, `servico_origem`, `servico_destino`, `hora_inicio`, `hora_fim`, `linhas_rodar` (`[]` = todas as linhas).
- **Saídas:**
  - Arquivos `horarios_{servico}_{vista}_{servico_destino}.csv` em `pasta_saida` (padrão `BASE_RESULTADOS/quadro_horario_extraido/{ano}/{mes}/qh_por_linha_combinado/{estudo}Q/`).
- **Dependências:** `pandas`; consome saídas de `1_extrair_qh_especificado_no_gtfs.py` e pode alimentar `1.5_carregar_qh_no_gtfs.py`.
- **Execução:**
  - CLI: `python codigos_py/1.2_combinar_quadros_horarios.py --config config.json`
  - UI: `src/app.py` → Aba **1. Extrair QH** → modo **1.2**.

### `1.3_qh_a_partir_de_partidas.py`
- **Arquivo:** [`codigos_py/1.3_qh_a_partir_de_partidas.py`](../codigos_py/1.3_qh_a_partir_de_partidas.py)
- **Categoria:** Transformação
- **Descrição:** Gera quadros de horários factíveis (faixas de `headway_secs`) a partir de uma tabela de contagens de partidas por linha, sentido e faixa horária, preservando a contagem exata de partidas.
- **Responsabilidades:**
  - Aceitar tabela de entrada em **formato longo** (`Faixa` + `Partidas`) ou **formato wide** (14 colunas de faixa horária, com melt automático).
  - Tolerar contagens formatadas com vírgula decimal ou casas decimais, além de traços (`traco_ausente`).
  - Derivar `trip_headsign` automaticamente a partir da coluna `vista` (`derivar_headsign()`).
  - Regularizar headways conforme `granularidade_headway` garantindo que a reexpansão reproduza a contagem de partidas solicitada, executando gate interno de conferência.
  - Exportar CSVs individuais (com `trip_id` vazio), relatório consolidado de bandas e pasta de trabalho Excel com uma aba por linha.
- **Entradas:**
  - Arquivo CSV ou Excel (`caminho_partidas`) com contagens de partidas por serviço/sentido/faixa.
  - Parâmetros `--config`: `BASE_RESULTADOS`, `ano_gtfs`, `mes_gtfs`, `estudo_gtfs`, `caminho_partidas`, `pasta_saida`, `service_id` (padrão `F_REG`), `tipo_dia`, `granularidade_headway`, `linhas_rodar`, `traco_ausente`, `gerar_excel`, `nome_excel`, `gerar_relatorio`.
- **Saídas:**
  - CSVs `horarios_{servico}_{headsign}_{service_id}.csv` em `pasta_saida` (padrão `BASE_RESULTADOS/quadro_horario_extraido/{ano}/{mes}/qh_por_linha_partidas/{estudo}Q/`).
  - `relatorio_bandas.csv` (quando `gerar_relatorio` ativo).
  - `{nome_excel}` (padrão `quadros_partidas_{service_id}.xlsx`, uma aba por linha, quando `gerar_excel` ativo).
- **Dependências:** `pandas`, `openpyxl` (para Excel); auditado por `1.3.1_conferir_partidas_geradas.py`; suas saídas alimentam `1.5_carregar_qh_no_gtfs.py` e `src/app_carregar_qh.py`.
- **Execução:**
  - CLI: `python codigos_py/1.3_qh_a_partir_de_partidas.py --config config.json`
  - UI: `src/app.py` → Aba **1. Extrair QH** → modo **1.3**.

### `1.3.1_conferir_partidas_geradas.py`
- **Arquivo:** [`codigos_py/1.3.1_conferir_partidas_geradas.py`](../codigos_py/1.3.1_conferir_partidas_geradas.py)
- **Categoria:** Validação / Auditoria
- **Descrição:** Audita de forma independente os CSVs e o Excel produzidos pelo script `1.3` contra a tabela de contagens de entrada, encerrando com código `0` (aprovado) ou `1` (divergência).
- **Responsabilidades:**
  - Reler diretamente a tabela de entrada (`caminho_partidas`) e os arquivos `horarios_*.csv` gravados no disco (sem confiar em relatórios gerados pelo `1.3`).
  - Exigir inteiros puros nas contagens da tabela de entrada para validação estrita.
  - Reexpandir cada banda (`start_time`, `end_time`, `headway_secs`) e comparar a contagem por `(servico, sentido, faixa)`.
  - Opcionalmente validar também as abas do arquivo Excel consolidado (`verificar_excel`).
- **Entradas:**
  - Tabela de entrada original (`caminho_partidas`) e diretório `pasta_saida` contendo os `horarios_*.csv` e opcionalmente `{nome_excel}`.
  - Parâmetros `--config`: `BASE_RESULTADOS`, `ano_gtfs`, `mes_gtfs`, `estudo_gtfs`, `caminho_partidas`, `pasta_saida`, `service_id`, `linhas_rodar`, `traco_ausente`, `verificar_excel`, `nome_excel`.
- **Saídas:**
  - `relatorio_conferencia_partidas.csv` em `pasta_saida`.
  - Log resumido no stdout e código de saída `0` (sem divergências) ou `1` (divergências detectadas).
- **Dependências:** `pandas`, `openpyxl` (se `verificar_excel=True`); depende da execução prévia de `1.3_qh_a_partir_de_partidas.py`.
- **Execução:**
  - CLI: `python codigos_py/1.3.1_conferir_partidas_geradas.py --config config.json`
  - UI: `src/app.py` → Aba **1. Extrair QH** → modo **1.3.1**.

### `1.4_qh_pf_a_partir_do_gtfs.py`
- **Arquivo:** [`codigos_py/1.4_qh_pf_a_partir_do_gtfs.py`](../codigos_py/1.4_qh_pf_a_partir_do_gtfs.py)
- **Categoria:** Transformação
- **Descrição:** Gera quadros de horários de ponto facultativo (`F_REG`) diretamente do `frequencies.txt` de um GTFS aplicando regras diferenciadas por consórcio.
- **Responsabilidades:**
  - Filtrar rotas com `route_type == '700'` (SPPO) e excluir integralmente agências em `agencias_excluidas` (padrão `['20001']`, MOBI-Rio).
  - Selecionar o calendário-base conforme consórcio (`mapa_base`): `U_REG` (dia útil) para consórcios `22002` a `22005` (Internorte, Intersul, Transcarioca, Santa Cruz) e `S_REG` (sábado) para `A2`/`B2` (GTU/TUSE).
  - Converter e validar as bandas geradas com gate interno de contagem (`n_partidas_banda()`), exportando CSVs (com `trip_id` vazio), relatório de bandas e Excel.
- **Entradas:**
  - ZIP GTFS (`end_gtfs`, padrão `0171_gtfs_out_26_1E_ret.zip` na raiz ou caminho configurado) contendo `routes.txt`, `trips.txt` e `frequencies.txt`.
  - Parâmetros `--config`: `BASE_RESULTADOS`, `ano_gtfs`, `mes_gtfs`, `estudo_gtfs`, `end_gtfs`, `pasta_saida`, `service_id` (padrão `F_REG`), `tipo_dia`, `route_types_permitidos`, `agencias_excluidas`, `mapa_base`, `linhas_rodar`, `gerar_excel`, `nome_excel`, `gerar_relatorio`.
- **Saídas:**
  - CSVs `horarios_{servico}_{headsign}_{service_id}.csv`, `relatorio_bandas.csv` e `quadros_partidas_{service_id}.xlsx` em `pasta_saida` (padrão `BASE_RESULTADOS/quadro_horario_extraido/{ano}/{mes}/qh_por_linha_pf_gtfs/{estudo}Q/`).
- **Dependências:** `pandas`, `zipfile`, `openpyxl`; alimenta `1.5_carregar_qh_no_gtfs.py` e `src/app_carregar_qh.py`.
- **Execução:**
  - CLI: `python codigos_py/1.4_qh_pf_a_partir_do_gtfs.py --config config.json`
  - UI: `src/app.py` → Aba **1. Extrair QH** → modo **1.4**.

### `1.5_carregar_qh_no_gtfs.py`
- **Arquivo:** [`codigos_py/1.5_carregar_qh_no_gtfs.py`](../codigos_py/1.5_carregar_qh_no_gtfs.py)
- **Categoria:** Carga
- **Descrição:** Realiza a operação inversa dos scripts `1`/`1.3`/`1.4`, carregando CSVs de quadros horários em um GTFS base e preservando a relação 1:1 entre `trips.txt` e `frequencies.txt`.
- **Responsabilidades:**
  - Coletar arquivos `horarios_*.csv` de uma pasta ou lista explícita (`arquivos_qh`).
  - Delegar leitura, detecção de alterações e aplicação para `src.gtfs_carregar_qh`.
  - Garantir que cada faixa em `frequencies.txt` possua um `trip_id` exclusivo em `trips.txt` e `stop_times.txt`, com `departure_time` da primeira parada alinhado ao `start_time` da faixa (evitando violação da chave `[feed_start_date, id_viagem]` no dbt).
  - Opcionalmente clonar viagens molde de `U_REG` quando `permitir_clonagem=True` e o calendário-alvo ainda não existir para a linha no GTFS.
- **Entradas:**
  - GTFS base (`endereco_gtfs_base`) e pasta ou lista de CSVs de QH (`arquivos_qh`).
  - Parâmetros `--config`: `BASE_DADOS`, `ano_gtfs`, `mes_gtfs`, `estudo_gtfs`, `endereco_gtfs_base`, `caminho_saida`, `arquivos_qh`, `service_id_fallback` (padrão `F_REG`), `permitir_clonagem` (padrão `False`).
- **Saídas:**
  - Arquivo ZIP GTFS atualizado em `caminho_saida` (padrão `BASE_DADOS/gtfs/{ano}/gtfs_combi_{ano}-{mes}-{estudo}Q_com_qh.zip`).
- **Dependências:** `src.gtfs_carregar_qh` (`parse_qh_csv`, `read_gtfs`, `write_gtfs`, `detectar_alteracoes`, `aplicar_quadros_gtfs`); consome CSVs de `1`, `1.2`, `1.3` ou `1.4`.
- **Execução:**
  - CLI: `python codigos_py/1.5_carregar_qh_no_gtfs.py --config config.json`
  - UI: `src/app.py` → Aba **1. Extrair QH** → modo **1.5** (ou app interativo `src/app_carregar_qh.py`).

---

## Grupo B — Etapas 2 a 4: Ajuste de Stop Times e Desvios

### `2_ajustar_stop_times.py`
- **Arquivo:** [`codigos_py/2_ajustar_stop_times.py`](../codigos_py/2_ajustar_stop_times.py)
- **Categoria:** Transformação
- **Descrição:** Ajusta `arrival_time`, `departure_time` e `shape_dist_traveled` em `stop_times.txt` usando velocidades reais medidas por GPS (Parquet para SPPO/Rio e CSV para BRT/Frescão).
- **Responsabilidades:**
  - Carregar base de velocidades GPS por modal: `*.parquet` em `BASE_DADOS/viagens/{sppo,rio}/{ano_velocidade}/{mes_velocidade}/` e `*.csv` em `BASE_DADOS/viagens/{brt,frescao}/{ano_velocidade}/{mes_velocidade}/validas/`.
  - Normalizar `shape_dist_traveled` quando a primeira parada não inicia em zero (`ajustar_shape_dist_traveled()`).
  - Recalcular tempos de deslocamento entre paradas com base nas velocidades médias observadas, aplicando fallback `velocidade_padrao_kmh` (padrão `15.0` km/h) para trechos ou horários faltantes (`corrigir_por_distancia_inplace()`, `corrigir_horarios_faltantes()`).
  - Registrar diagnósticos de distâncias negativas ou inconsistências em CSVs auxiliares e empacotar o GTFS processado (`*_PROC.zip`).
- **Entradas:**
  - GTFS bruto (`endereco_gtfs`, padrão `BASE_DADOS/gtfs/{ano}/{gtfs_processar}_{ano}-{mes}-{estudo}Q.zip`).
  - Arquivos Parquet de viagens válidas (GPS) em `BASE_DADOS/viagens/`.
  - Parâmetros `--config`: `BASE_DADOS`, `ano_gtfs`, `mes_gtfs`, `estudo_gtfs`, `gtfs_processar`, `endereco_gtfs`, `ano_velocidade`, `mes_velocidade`, `velocidade_padrao_kmh`.
- **Saídas:**
  - ZIP GTFS ajustado em `BASE_DADOS/gtfs/{ano}/{gtfs_processar}_{ano}-{mes}-{estudo}Q_PROC.zip`.
  - Arquivos de diagnóstico no diretório de trabalho quando houver anomalias (ex.: `shape_dist_negativos.csv`, relatórios de inconsistências).
- **Dependências:** `pandas`, `numpy`, `pyarrow`, `zipfile`; alimenta as Etapas `4`, `5` e `5.1`.
- **Execução:**
  - CLI: `python codigos_py/2_ajustar_stop_times.py --config config.json`
  - UI: `src/app.py` → Aba **2. Ajustar ST**.

### `3_desvios_nao-utilizar.py`
- **Arquivo:** [`codigos_py/3_desvios_nao-utilizar.py`](../codigos_py/3_desvios_nao-utilizar.py)
- **Categoria:** Transformação (Descontinuado)
- **Descrição:** Script legado descontinuado que aplicava ajustes de calendário de desvios no GTFS a partir de planilhas locais/Google Sheets; não deve ser utilizado em produção.
- **Responsabilidades:**
  - Ler `insumos_desvios/linhas_desvios.csv` e `insumos_desvios/descricao_desvios.csv`, filtrando desvios ativos na data atual.
  - Reescrever `calendar.txt`/`trips.txt` do GTFS com caminhos e credenciais (`credentials.json`) hardcoded.
- **Entradas:**
  - Hardcoded no código (`BASE_DADOS`, `ano_gtfs`, `mes_gtfs`, `quinzena_gtfs`, `insumos_desvios/*.csv`). **Não aceita `--config`.**
- **Saídas:**
  - Sobrescrita de arquivo ZIP GTFS via temporário.
- **Dependências:** `pandas`, `numpy`, `gspread`, `google-auth`. Não integrado ao `src/app.py`.
- **Execução:** Não recomendado (script descontinuado, mantido apenas como referência histórica).

### `4_trajetos_alternativos.py`
- **Arquivo:** [`codigos_py/4_trajetos_alternativos.py`](../codigos_py/4_trajetos_alternativos.py)
- **Categoria:** Extração
- **Descrição:** Extrai e consolida os trajetos alternativos/exceções do GTFS com suas respectivas extensões geométricas GIS para compor o anexo de alternativos da OS.
- **Responsabilidades:**
  - Filtrar `trips.txt` pelos calendários em `CALENDARIOS_ALVO` e identificar trajetos alternativos/desvios no `trip_headsign`.
  - Construir geometrias `LineString` a partir de `shapes.txt`, projetar para `EPSG:31983` e calcular a extensão real em metros.
  - Mapear sentidos (`map_sentido()`) e consolidar metadados de rota, agência e evento.
- **Entradas:**
  - ZIP GTFS (`endereco_gtfs`, padrão `BASE_DADOS/gtfs/{ano}/sppo_{ano}-{mes}-{estudo}Q_PROC.zip`).
  - Parâmetros `--config`: `BASE_DADOS`, `ano_gtfs`, `mes_gtfs`, `estudo_gtfs`, `endereco_gtfs`, `caminho_saida`, `CALENDARIOS_ALVO`.
- **Saídas:**
  - Relatório CSV em `caminho_saida` (padrão `BASE_DADOS/os/os_{ano}-{mes}-{estudo}_excep_sppo.csv`).
- **Dependências:** `pandas`, `numpy`, `geopandas`, `shapely`, `zipfile`; consome saída de `2_ajustar_stop_times.py`; sua saída pode orientar filtros em `9_filtrar_gtfs_por_lista.py`.
- **Execução:**
  - CLI: `python codigos_py/4_trajetos_alternativos.py --config config.json`
  - UI: `src/app.py` → Aba **4. Alternativos**.

---

## Grupo C — Etapa 5: Junção, Substituição e Deduplicação de GTFS

### `5_juntar_gtfs.py`
- **Arquivo:** [`codigos_py/5_juntar_gtfs.py`](../codigos_py/5_juntar_gtfs.py)
- **Categoria:** Junção
- **Descrição:** Combina os arquivos GTFS de SPPO e BRT, aplica ajustes de calendário, cores e insumos de substituição, e produz o GTFS combinado e o GTFS público.
- **Responsabilidades:**
  - Ler os ZIPs de SPPO e BRT, filtrar linhas de exclusão (`linhas_excluir`), ajustar `service_id` (`ajustar_service_id()`), remover colunas auxiliares e padronizar cores (`atualizar_cores_gtfs()`).
  - **Sobrescrever `endereco_brt`** após a filtragem/limpeza do GTFS BRT.
  - Substituir arquivos de insumos a partir de `BASE_DADOS/insumos/gtfs_combi` e `BASE_DADOS/insumos/gtfs_pub` (conforme `etapa_gtfs_rio`).
  - Executar limpeza em cascata (`clean_gtfs()`) e gravar tanto o GTFS combinado quanto o GTFS público.
- **Entradas:**
  - `endereco_sppo` (padrão `BASE_DADOS/gtfs/{ano}/{gtfs_processar}_{sufixo}_PROC.zip`) e `endereco_brt` (padrão `BASE_DADOS/gtfs/{ano}/brt_{sufixo}_PROC.zip`).
  - Pastas de insumos `BASE_DADOS/insumos/gtfs_combi/` e `BASE_DADOS/insumos/gtfs_pub/`.
  - Parâmetros `--config`: `BASE_DADOS`, `ano_gtfs`, `mes_gtfs`, `estudo_gtfs`, `gtfs_processar`, `etapa_gtfs_rio`, `endereco_sppo`, `endereco_brt`, `endereco_gtfs_combi`, `linhas_excluir`.
- **Saídas:**
  - `endereco_brt` (sobrescrito após filtragem do BRT).
  - `endereco_gtfs_combi` (padrão `BASE_DADOS/gtfs/{ano}/gtfs_combi_{ano}-{mes}-{estudo}Q.zip`).
  - `BASE_DADOS/gtfs/{ano}/gtfs_rio-de-janeiro_pub.zip`.
- **Dependências:** `pandas`, `numpy`, `zipfile`; consome saídas de `2_ajustar_stop_times.py`; alimenta Etapas `1`, `6`, `7` e `8`.
- **Execução:**
  - CLI: `python codigos_py/5_juntar_gtfs.py --config config.json`
  - UI: `src/app.py` → Aba **5. Juntar GTFS** → modo **5. Juntar SPPO + BRT**.

### `5.1_juntar_gtfs_sppo.py`
- **Arquivo:** [`codigos_py/5.1_juntar_gtfs_sppo.py`](../codigos_py/5.1_juntar_gtfs_sppo.py)
- **Categoria:** Junção
- **Descrição:** Empacota um único modal (`sppo`, `brt` ou `rio`) aplicando padronização de `service_id`, cores, substituição de insumos por etapa e limpeza em cascata.
- **Responsabilidades:**
  - Selecionar o arquivo de entrada conforme `gtfs_processar` (`sppo`, `brt` ou `rio`).
  - Aplicar `ajustar_service_id()`, `atualizar_cores_gtfs()`, substituição de arquivos por etapa (`obter_pastas_substituicao()`, `substituir_arquivos_gtfs()`) e `clean_gtfs()`.
  - Gerar os ZIPs combinado e público sem exigir junção entre dois modais.
- **Entradas:**
  - ZIP processado do modal escolhido (`endereco_sppo`, `endereco_brt` ou `rio_{sufixo}_PROC.zip`) e insumos em `BASE_DADOS/insumos/gtfs_combi/{MODAL}/{ETAPA}` e `gtfs_pub/{MODAL}/{ETAPA}`.
  - Parâmetros `--config`: `BASE_DADOS`, `ano_gtfs`, `mes_gtfs`, `estudo_gtfs`, `gtfs_processar`, `etapa_gtfs_rio`, `endereco_sppo`, `endereco_brt`, `endereco_gtfs_combi`.
- **Saídas:**
  - `endereco_gtfs_combi` (padrão `BASE_DADOS/gtfs/{ano}/gtfs_combi_{sufixo}.zip`).
  - `BASE_DADOS/gtfs/{ano}/gtfs_rio-de-janeiro_pub.zip`.
- **Dependências:** `pandas`, `numpy`, `zipfile`; alternativa ao `5_juntar_gtfs.py` quando se processa um único modal.
- **Execução:**
  - CLI: `python codigos_py/5.1_juntar_gtfs_sppo.py --config config.json`
  - UI: `src/app.py` → Aba **5. Juntar GTFS** → modo **5.1 Juntar Único (apenas SPPO ou BRT)**.

### `5.2_juntar_gtfs_simples.py`
- **Arquivo:** [`codigos_py/5.2_juntar_gtfs_simples.py`](../codigos_py/5.2_juntar_gtfs_simples.py)
- **Categoria:** Junção
- **Descrição:** Concatena diretamente as tabelas `.txt` de dois ou mais arquivos ZIP GTFS removendo linhas exatamente idênticas.
- **Responsabilidades:**
  - Ler todos os ZIPs informados em `INPUT_ZIPS`.
  - Empilhar tabelas de mesmo nome com `pd.concat()` e aplicar `drop_duplicates()`.
  - Gravar o ZIP consolidado em `OUTPUT_ZIP` sem alterar `service_id`, cores ou aplicar regras de domínio.
- **Entradas:**
  - Parâmetros `--config`: `INPUT_ZIPS` (lista de caminhos `.zip`), `OUTPUT_ZIP`.
- **Saídas:**
  - ZIP GTFS concatenado em `OUTPUT_ZIP`.
- **Dependências:** `pandas`, `zipfile`. Utilitário independente.
- **Execução:**
  - CLI: `python codigos_py/5.2_juntar_gtfs_simples.py --config config.json`
  - UI: `src/app.py` → Aba **5. Juntar GTFS** → modo **5.2 Concatenar Simples**.

### `5.3_juntar_gtfs_substituicao.py`
- **Arquivo:** [`codigos_py/5.3_juntar_gtfs_substituicao.py`](../codigos_py/5.3_juntar_gtfs_substituicao.py)
- **Categoria:** Junção
- **Descrição:** Atualiza um GTFS base (`GTFS_1_PATH`) substituindo as rotas/viagens presentes em um segundo GTFS (`GTFS_2_PATH`) e executando limpeza referencial em cascata.
- **Responsabilidades:**
  - Identificar viagens/rotas presentes em `GTFS_2_PATH` e remover registros conflitantes de `GTFS_1_PATH`.
  - Mesclar as tabelas resultantes e executar `clean_gtfs()` para eliminar órfãos em `stop_times`, `frequencies`, `routes`, `shapes`, `stops`, `calendar` e `agency`.
- **Entradas:**
  - Parâmetros `--config`: `GTFS_1_PATH` (base), `GTFS_2_PATH` (substituição/patch), `OUTPUT_ZIP`.
- **Saídas:**
  - ZIP GTFS atualizado em `OUTPUT_ZIP`.
- **Dependências:** `pandas`, `numpy`, `zipfile`. Utilitário independente (frequentemente combinado com `9_filtrar_gtfs_por_lista.py`).
- **Execução:**
  - CLI: `python codigos_py/5.3_juntar_gtfs_substituicao.py --config config.json`
  - UI: `src/app.py` → Aba **5. Juntar GTFS** → modo **5.3 Juntar c/ Substituição**.

### `5.4_eliminar_duplicatas_gtfs.py`
- **Arquivo:** [`codigos_py/5.4_eliminar_duplicatas_gtfs.py`](../codigos_py/5.4_eliminar_duplicatas_gtfs.py)
- **Categoria:** Limpeza
- **Descrição:** Detecta e elimina viagens e frequências duplicadas dentro de um ou mais arquivos GTFS preservando viagens de exceção (`EXCEP`) e mantendo integridade referencial.
- **Responsabilidades:**
  - Inspecionar viagens e bandas duplicadas, identificando viagens de exceção via `is_trip_excep()`.
  - Deduplicar tabelas (`deduplicar_gtfs()`) e remover registros órfãos em cascata (`clean_gtfs()`).
  - Suportar processamento em lote via `arquivos_alvo`; se `caminho_saida` não for informado, sobrescreve o arquivo de entrada.
- **Entradas:**
  - Parâmetros `--config`: `endereco_gtfs`, `caminho_saida` (opcional), `arquivos_alvo` (opcional, lista ou string separada por vírgula).
- **Saídas:**
  - ZIP GTFS deduplicado em `caminho_saida` (ou sobrescrevendo o arquivo original).
- **Dependências:** `pandas`, `numpy`, `zipfile`. Utilitário independente de saneamento pós-junção.
- **Execução:**
  - CLI: `python codigos_py/5.4_eliminar_duplicatas_gtfs.py --config config.json`
  - UI: `src/app.py` → Aba **5. Juntar GTFS** → modo **5.4 Eliminar Duplicatas GTFS**.

### `5.5_juntar_gtfs_unificado.py`
- **Arquivo:** [`codigos_py/5.5_juntar_gtfs_unificado.py`](../codigos_py/5.5_juntar_gtfs_unificado.py)
- **Categoria:** Junção / Empacotamento
- **Descrição:** Derivado do `5.1_juntar_gtfs_sppo.py` para processar um único GTFS `_PROC.zip` que já reúne SPPO, RIO, BRT e Executivos, sem substituir arquivos por insumos armazenados em disco.
- **Responsabilidades:**
  - Preservar `route_type == '702'` (BRT) e `route_type == '200'` (Executivos) já existentes no feed, normalizando as demais rotas para `'700'` (ou `'200'` quando `numero > 1000` ou `LECD124`/`LECD125`).
  - Padronizar `service_id` (`U`/`S`/`D` → `U_REG`/`S_REG`/`D_REG`), filtrar `trip_id_fantasma.txt` e `linhas_excluir`, limpar colunas/paradas (`APAGAR`), ordenar `shapes.txt` e executar `clean_gtfs()`.
  - Gravar `endereco_gtfs_combi` e `caminho_gtfs_pub` (removendo `EXCEP` e aplicando cores via `atualizar_cores_gtfs()`) **sem** chamar `substituir_arquivos_gtfs()`, mantendo os arquivos originais do feed (`calendar_dates.txt`, `fare_attributes.txt`, `fare_rules.txt`, `feed_info.txt`).
- **Entradas:**
  - ZIP GTFS unificado (`endereco_gtfs`, padrão `BASE_DADOS/gtfs/{ano}/{gtfs_processar}_{sufixo}_PROC.zip`).
  - Parâmetros `--config`: `BASE_DADOS`, `ano_gtfs`, `mes_gtfs`, `estudo_gtfs`, `gtfs_processar`, `endereco_gtfs`, `endereco_gtfs_combi`, `caminho_gtfs_pub`, `caminho_cores`, `linhas_excluir`.
- **Saídas:**
  - `endereco_gtfs_combi` (padrão `BASE_DADOS/gtfs/{ano}/gtfs_combi_{sufixo}.zip`).
  - `caminho_gtfs_pub` (padrão `BASE_DADOS/gtfs/{ano}/gtfs_rio-de-janeiro_pub.zip`).
- **Dependências:** `pandas`, `numpy`, `zipfile`.
- **Execução:**
  - CLI: `python codigos_py/5.5_juntar_gtfs_unificado.py --config config.json`
  - UI: `src/app.py` → Aba **5. Juntar GTFS** → modo **5.5 Processar Unificado (sem substituição)**.

---

## Grupo D — Etapas 6 a 8: Geração de Produtos (Shapes, Partidas e Extensões)

### `6_gerar_shapes.py`
- **Arquivo:** [`codigos_py/6_gerar_shapes.py`](../codigos_py/6_gerar_shapes.py)
- **Categoria:** Exportação GIS
- **Descrição:** Converte os trajetos (`shapes.txt`) e paradas (`stops.txt`) do GTFS em camadas geoespaciais Shapefile (`.shp`) e GeoPackage (`.gpkg`) enriquecidas com atributos operacionais.
- **Responsabilidades:**
  - Ordenar pontos de `shapes.txt` por `shape_pt_sequence`, detectar shapes inválidos (< 2 pontos) e exportá-los em `shapes_invalidos_com_trips.csv` quando encontrados.
  - Gerar geometrias `LineString` (trajetos) e `Point` (paradas), associando atributos de `trips`, `routes`, `agency`, `fare_rules`, `fare_attributes`, código de desvio (`get_cod_desvio()`) e tipo de rota (`get_tipo_rota()`).
  - Exportar camadas em `.shp` (com nomes de campos compatíveis com DBF) e `.gpkg` via `pyogrio`.
- **Entradas:**
  - ZIP GTFS (`endereco_gtfs_combi`, padrão `BASE_DADOS/gtfs/{ano}/gtfs_rio-de-janeiro_pub_completo.zip`).
  - Parâmetros `--config`: `BASE_DADOS`, `ano_gtfs`, `mes_gtfs`, `estudo_gtfs`, `endereco_gtfs_combi`, `pasta_shape_sppo`.
- **Saídas:**
  - Em `pasta_shape_sppo` (padrão `BASE_DADOS/shapes/{ano}/`):
    - `shapes_trajetos_{ano}-{mes}-{estudo}Q.shp` e `.gpkg`
    - `shapes_pontos_{ano}-{mes}-{estudo}Q.shp` e `.gpkg`
  - `shapes_invalidos_com_trips.csv` (se houver shapes com < 2 pontos).
- **Dependências:** `pandas`, `numpy`, `geopandas`, `shapely`, `pyogrio`, `zipfile`; consome GTFS gerado na Etapa 5.
- **Execução:**
  - CLI: `python codigos_py/6_gerar_shapes.py --config config.json`
  - UI: `src/app.py` → Aba **6. Shapes**.

### `7_lista_partidas.py`
- **Arquivo:** [`codigos_py/7_lista_partidas.py`](../codigos_py/7_lista_partidas.py)
- **Categoria:** Extração
- **Descrição:** Expande as frequências do GTFS em uma tabela completa de partidas individuais por tipo de dia (`du`, `sab`, `dom`, `pf`), incluindo extensão GIS e classificação em 14 faixas horárias.
- **Responsabilidades:**
  - Mapear tokens de `tipos_dia` (`du`, `sab`, `dom`, `pf` [ponto facultativo, `F_REG`], variantes de verão `du_ver`, `sab_ver`, `dom_ver`, `pf_ver`, ou `service_id` explícito) via `resolver_service_ids_por_tipo_dia()`, combinando o padrão legado (`U`/`S`/`D`/`F`) com os dias da semana de `calendar.txt`, excluindo `EXCEP`/`DESAT` e mantendo calendários de verão (`_VER`/`VERAO`) separados dos regulares; `get_pattern()` retorna `""` para tokens desconhecidos.
  - Excluir serviços listados em `linhas_excluir` e expandir `frequencies.txt` usando horários da primeira parada em `stop_times.txt` (compatível com `stop_sequence` iniciando em `0` ou `1`, preservando segundos herdados do ajuste por GPS da Etapa 2).
  - Calcular a extensão GIS máxima por itinerário (`LineString` em `EPSG:31983`, `astype(int)`) excluindo trips `EXCEP` e desvios (`[` no `trip_headsign`), **sem filtrar `route_type`** para cobrir também BRT (`702`).
  - Exportar arquivos individuais por tipo de dia e arquivos consolidados em CSV e Parquet.
- **Entradas:**
  - ZIP GTFS (`endereco_gtfs`) contendo `trips.txt`, `routes.txt`, `agency.txt`, `frequencies.txt`, `stop_times.txt` e `shapes.txt`.
  - Parâmetros `--config`: `BASE_DADOS`, `BASE_RESULTADOS`, `ano_gtfs`, `mes_gtfs`, `estudo_gtfs`, `endereco_gtfs`, `tipos_dia`, `linhas_excluir`.
- **Saídas:**
  - `partidas_{tipo_dia}.csv` (um por tipo de dia processado).
  - `partidas_{sufixo}.csv` e `partidas_{sufixo}.parquet` (consolidados).
- **Dependências:** `pandas`, `numpy`, `geopandas`, `shapely`, `pyarrow`, `zipfile`; consome GTFS da Etapa 5; alimenta `7.1`, `7.2` e validações externas (projeto irmão `validacao_gtfs`).
- **Execução:**
  - CLI: `python codigos_py/7_lista_partidas.py --config config.json`
  - UI: `src/app.py` → Aba **7. Partidas** → modo **7**.

### `7.1_resumo_os_domingo_du.py`
- **Arquivo:** [`codigos_py/7.1_resumo_os_domingo_du.py`](../codigos_py/7.1_resumo_os_domingo_du.py)
- **Categoria:** Transformação *(Evitar / com ressalva)*
- **Descrição:** Gera tabela pivô de 14 faixas horárias para domingo importando viagens de dia útil na janela `[hora_inicio, hora_fim)`.
- **Responsabilidades:**
  - Ler o arquivo consolidado de partidas (`.parquet` ou `.csv`) gerado pelo script `7`.
  - Selecionar viagens de `tipo_dia == 'du'` dentro de `[hora_inicio, hora_fim)` e `tipo_dia == 'dom'` fora desse intervalo.
  - Pivotar contagens nas 14 faixas padrão intercalando colunas de `Partidas` e `Quilometragem` (em branco) e exportar `.xlsx` e `.csv` (`;`, `utf-8-sig`).
- **Ressalva operacional:** Este script inclui na tabela final linhas que operam **apenas em dia útil** (sem operação no domingo) dentro da janela informada, não compatibiliza intervalos nas fronteiras de transição (`06h`/`20h`) e deixa as colunas de quilometragem vazias. Quando o requisito for manter apenas linhas operantes no domingo com compatibilização de fronteiras, utilize `7.2_combinador_partidas.py`.
- **Entradas:**
  - Arquivo consolidado de partidas (`caminho_entrada`, `.parquet` ou `.csv`).
  - Parâmetros `--config`: `caminho_entrada`, `pasta_saida`, `nome_base_saida` (padrão `tabela_domingo_ajustada`), `hora_inicio` (padrão `06:00:00`), `hora_fim` (padrão `20:00:00`).
- **Saídas:**
  - `{pasta_saida}/{nome_base_saida}.xlsx` e `{pasta_saida}/{nome_base_saida}.csv`.
- **Dependências:** `pandas`, `pyarrow`, `openpyxl`; consome saída de `7_lista_partidas.py`.
- **Execução:**
  - CLI: `python codigos_py/7.1_resumo_os_domingo_du.py --config config.json`
  - UI: `src/app.py` → Aba **7. Partidas** → modo **7.1**.

### `7.2_combinador_partidas.py`
- **Arquivo:** [`codigos_py/7.2_combinador_partidas.py`](../codigos_py/7.2_combinador_partidas.py)
- **Categoria:** Transformação
- **Descrição:** Combina partidas de domingo (`00h–06h` e `20h+`) com partidas de dia útil (`06h–20h`) exclusivamente para serviços presentes no domingo, compatibilizando intervalos nas transições.
- **Responsabilidades:**
  - Filtrar ambos os arquivos mantendo somente `trip_short_name` presentes em `caminho_dom`.
  - Agrupar por `(trip_short_name, direction_id)` (ou `trip_short_name`) e montar os três blocos horários (madrugada domingo, dia útil `06h–20h`, noite domingo).
  - Compatibilizar os intervalos de transição `05:59 -> 06:00` e `19:59 -> 20:00` evitando partidas sobrepostas ou lacunas abruptas.
- **Entradas:**
  - Dois CSVs de partidas individuais gerados pelo script `7`: `caminho_du` (padrão `quadros_du.csv`) e `caminho_dom` (padrão `quadros_dom.csv`).
  - Parâmetros `--config`: `caminho_du`, `caminho_dom`, `caminho_saida` (padrão `quadros_dom_04-10.csv`).
- **Saídas:**
  - CSV combinado em `caminho_saida`.
- **Dependências:** `pandas`; consome `partidas_du.csv` e `partidas_dom.csv` gerados por `7_lista_partidas.py`.
- **Execução:**
  - CLI: `python codigos_py/7.2_combinador_partidas.py --config config.json`
  - UI: `src/app.py` → Aba **7. Partidas** → modo **7.2**.

### `8_gerar_extensoes.py`
- **Arquivo:** [`codigos_py/8_gerar_extensoes.py`](../codigos_py/8_gerar_extensoes.py)
- **Categoria:** Extração
- **Descrição:** Calcula a extensão máxima GIS em metros por linha e sentido para serviços SPPO (`route_type` `3` ou `700`), excluindo exceções e desvios.
- **Responsabilidades:**
  - Ler `routes.txt`, `trips.txt`, `agency.txt` e `shapes.txt` do GTFS.
  - Filtrar `route_type` em `['3', '700']` e excluir trips `EXCEP` e desvios (`[` no `trip_headsign`).
  - Construir `LineString` em `EPSG:31983`, converter o comprimento para inteiro em metros (`astype(int)`) e selecionar a extensão máxima por itinerário/sentido (`map_sentido()`).
- **Entradas:**
  - ZIP GTFS (`endereco_gtfs`).
  - Parâmetros `--config`: `BASE_DADOS`, `BASE_RESULTADOS`, `ano_gtfs`, `mes_gtfs`, `estudo_gtfs`, `endereco_gtfs`.
- **Saídas:**
  - `BASE_RESULTADOS/extensoes/extensoes_{ano}-{mes}-{estudo}Q.csv`.
- **Dependências:** `pandas`, `numpy`, `geopandas`, `shapely`, `zipfile`; consome GTFS da Etapa 5.
- **Execução:**
  - CLI: `python codigos_py/8_gerar_extensoes.py --config config.json`
  - UI: `src/app.py` → Aba **8. Extensões** → modo **Padrão (Script 8)**.

### `8.1_gerar_extensoes_rio.py`
- **Arquivo:** [`codigos_py/8.1_gerar_extensoes_rio.py`](../codigos_py/8.1_gerar_extensoes_rio.py)
- **Categoria:** Extração
- **Descrição:** Variante do script `8` parametrizada por `sufixo` livre (formato `rio_YYYY-MM`) para calcular extensões máximas GIS em metros de serviços SPPO.
- **Responsabilidades:**
  - Aplicar exatamente a mesma regra de cálculo do script `8`: filtro `route_type` em `['3', '700']`, exclusão de `EXCEP` e desvios `[`, projeção `EPSG:31983` e extensão máxima em metros (`astype(int)`).
  - Permitir nomenclatura de entrada/saída baseada em `sufixo` em vez de `ano`/`mes`/`estudo`.
- **Entradas:**
  - ZIP GTFS (`endereco_gtfs`, padrão `BASE_DADOS/gtfs/{ano}/{sufixo}.zip`).
  - Parâmetros `--config`: `BASE_DADOS`, `BASE_RESULTADOS`, `ano_gtfs`, `sufixo`, `endereco_gtfs`.
- **Saídas:**
  - `BASE_RESULTADOS/extensoes/extensoes_{sufixo}.csv`.
- **Dependências:** `pandas`, `numpy`, `geopandas`, `shapely`, `zipfile`; variante de `8_gerar_extensoes.py`.
- **Execução:**
  - CLI: `python codigos_py/8.1_gerar_extensoes_rio.py --config config.json`
  - UI: `src/app.py` → Aba **8. Extensões** → modo **Formato Rio (Script 8.1)**.

---

## Grupo E — Etapas 9 e 10: Filtragem e Mesclagem por Chave Primária

### `9_filtrar_gtfs_por_lista.py`
- **Arquivo:** [`codigos_py/9_filtrar_gtfs_por_lista.py`](../codigos_py/9_filtrar_gtfs_por_lista.py)
- **Categoria:** Limpeza
- **Descrição:** Filtra um GTFS mantendo apenas as viagens que correspondem a uma tabela de filtro (Serviço, Vista, Consórcio, Sentido e Evento `[...]`) e limpa todas as tabelas associadas em cascata.
- **Responsabilidades:**
  - Fazer parsing automático de CSV (`,`) ou TSV (`\t`) fornecido via texto (`LISTA_FILTRO_RAW`) ou arquivo (`LISTA_FILTRO_PATH`) com `parse_lista_filtro()`.
  - Cruzar `trip_short_name` (Serviço), `route_long_name` (Vista), `agency_name` (Consórcio), `direction_id` (Sentido: `Ida=0`, `Volta=1`) e texto entre colchetes no `trip_headsign` (`extrair_evento()`), opcionalmente restrito a `CALENDARIOS_ALVO` (padrão `["EXCEP"]`).
  - Executar `clean_gtfs()` para remover registros órfãos em todas as demais tabelas do GTFS.
- **Entradas:**
  - ZIP GTFS (`endereco_gtfs`) e lista de filtro (`LISTA_FILTRO_RAW` ou `LISTA_FILTRO_PATH`).
  - Parâmetros `--config`: `BASE_DADOS`, `ano_gtfs`, `mes_gtfs`, `estudo_gtfs`, `endereco_gtfs`, `caminho_saida`, `CALENDARIOS_ALVO`, `LISTA_FILTRO_RAW`, `LISTA_FILTRO_PATH`.
- **Saídas:**
  - ZIP GTFS filtrado em `caminho_saida` (padrão `BASE_DADOS/gtfs/{ano}/sppo_{ano}-{mes}-{estudo}Q_FILTRADO.zip`).
- **Dependências:** `pandas`, `numpy`, `zipfile`; normalmente alimenta `10_juntar_dois_gtfs.py` ou `5.3_juntar_gtfs_substituicao.py`.
- **Execução:**
  - CLI: `python codigos_py/9_filtrar_gtfs_por_lista.py --config config.json`
  - UI: `src/app.py` → Aba **9. Filtrar**.

### `10_juntar_dois_gtfs.py`
- **Arquivo:** [`codigos_py/10_juntar_dois_gtfs.py`](../codigos_py/10_juntar_dois_gtfs.py)
- **Categoria:** Junção
- **Descrição:** Mescla dois arquivos GTFS (`GTFS_PRINCIPAL` e `GTFS_SECUNDARIO`) resolvendo conflitos tabela a tabela por chave primária com prioridade determinística ao GTFS principal.
- **Responsabilidades:**
  - Aplicar `merge_tabela()` segundo o dicionário `CHAVES_PRIMARIAS` (`agency`, `stops`, `routes`, `trips`, `stop_times`, `calendar`, `calendar_dates`, `shapes`, `frequencies`, `fare_attributes`, `fare_rules`, `feed_info`).
  - Em caso de colisão de chave primária, manter exclusivamente o registro do `GTFS_PRINCIPAL`; para `feed_info`, preservar apenas o registro do principal.
  - Executar `clean_gtfs()` ao final para assegurar consistência referencial.
- **Entradas:**
  - `GTFS_PRINCIPAL` (padrão `sppo_{sufixo}_FILTRADO.zip`) e `GTFS_SECUNDARIO` (padrão `sppo_{sufixo}_PROC.zip`).
  - Parâmetros `--config`: `BASE_DADOS`, `ano_gtfs`, `mes_gtfs`, `estudo_gtfs`, `GTFS_PRINCIPAL`, `GTFS_SECUNDARIO`, `GTFS_SAIDA`.
- **Saídas:**
  - ZIP GTFS combinado em `GTFS_SAIDA` (padrão `BASE_DADOS/gtfs/{ano}/sppo_{ano}-{mes}-{estudo}Q_COMBINADO.zip`).
- **Dependências:** `pandas`, `numpy`, `zipfile`; consome tipicamente saídas de `9_filtrar_gtfs_por_lista.py` e `2_ajustar_stop_times.py`.
- **Execução:**
  - CLI: `python codigos_py/10_juntar_dois_gtfs.py --config config.json`
  - UI: `src/app.py` → Aba **10. Juntar Dois**.

---

## Grupo F — Aplicações Streamlit, Orquestrador e Bibliotecas Puras (`src/`)

### `src/app.py`
- **Arquivo:** [`src/app.py`](../src/app.py)
- **Categoria:** Interface / Orquestração
- **Descrição:** Ponto de entrada da aplicação Streamlit principal que centraliza parâmetros globais na barra lateral e organiza o pipeline em 9 abas interativas.
- **Responsabilidades:**
  - Manter em `st.session_state` os parâmetros globais compartilhados (`BASE_DADOS`, `ano_gtfs`, `mes_gtfs`, `estudo_gtfs`, `gtfs_processar`).
  - Carregar e renderizar as 9 abas (`tab_1_extrair_qh` a `tab_10_juntar_dois`, pulando a etapa 3 descontinuada).
- **Entradas:** Parâmetros inseridos pelo usuário na barra lateral e formulários das abas.
- **Saídas:** Invocação dos scripts de `codigos_py/` via `src.runner.run_script()` e exibição dos logs na interface web.
- **Dependências:** `streamlit`, `src.tabs.tab_*`.
- **Execução:**
  - `python -m streamlit run src/app.py` (ou atalho `Iniciar_App.bat` na raiz).

### `src/runner.py`
- **Arquivo:** [`src/runner.py`](../src/runner.py)
- **Categoria:** Orquestração
- **Descrição:** Executa qualquer script do pipeline em subprocesso isolado usando o interpretador do ambiente ativo, passando a configuração por arquivo JSON temporário via `--config`.
- **Responsabilidades:**
  - Serializar o dicionário `config` em um arquivo `.json` temporário UTF-8 e removê-lo no bloco `finally`.
  - Executar `[sys.executable, script_path, "--config", config_file]` com `PYTHONIOENCODING=utf-8`.
  - Ler `stdout`/`stderr` em thread daemon sem bloquear a UI, exibindo até as últimas 500 linhas de log em tempo real sob uma barra de progresso assintótica por tempo decorrido (`_PROGRESS_CAP = 0.95`, `_PROGRESS_TAU = 90.0s`) que só chega a 100% quando `return_code == 0`.
- **Entradas:** `script_path: str`, `config: dict`, `cwd: str | None`.
- **Saídas:** Widgets Streamlit de progresso, logs e status de sucesso/erro.
- **Dependências:** `streamlit`, `subprocess`, `json`, `tempfile`, `threading`, `queue`. Importado por todas as abas em `src/tabs/tab_*.py`.
- **Execução:** Módulo interno — importado via `from src.runner import run_script`.

### `src/app_format_qh.py`
- **Arquivo:** [`src/app_format_qh.py`](../src/app_format_qh.py)
- **Categoria:** Interface
- **Descrição:** Aplicação Streamlit standalone (sucessora em Python do app Shiny `1.2 app_format_qh.R`) para inspecionar quadros horários de um GTFS, comparar com programações coladas e exportar CSV de QH.
- **Responsabilidades:**
  - Receber upload de arquivo ZIP GTFS com cache por hash (`_carregar_gtfs()`).
  - Fornecer filtros em cascata (rota, linha, vista, calendário), renderizar programação colorida por faixa horária, comparar partidas coladas contra o GTFS e exibir resumo por faixa.
  - Permitir download do quadro horário formatado em CSV.
- **Entradas:** Upload de arquivo ZIP GTFS e texto de partidas colado pelo usuário na interface.
- **Saídas:** Tabelas estilizadas na interface web e download de arquivo CSV de QH.
- **Dependências:** `streamlit`, `pandas`, `src.qh_format`.
- **Execução:**
  - `python -m streamlit run src/app_format_qh.py`

### `src/qh_format.py`
- **Arquivo:** [`src/qh_format.py`](../src/qh_format.py)
- **Categoria:** Biblioteca Pura
- **Descrição:** Reúne a lógica pura (sem dependência de Streamlit) de leitura de GTFS, parsing de partidas, derivação de faixas de headway e comparação de programações horárias.
- **Responsabilidades:**
  - Converter horários (`horario_to_seconds()`, `seconds_to_horario()`), tratar virada de meia-noite (`unwrap_midnight()`) e fazer parsing de listas de partidas (`parse_partidas()`).
  - Carregar ZIP GTFS (`load_gtfs_zip()`), filtrar viagens (`filter_trips()`), derivar bandas de frequência (`bands_from_departures()`, `qh_dataframe()`) e montar tabelas de comparação e resumo (`comparison_table()`, `summary_table()`).
- **Entradas:** Bytes/caminho de ZIP GTFS, strings de partidas e filtros de linha/calendário.
- **Saídas:** DataFrames `pandas`, listas de segundos e strings formatadas.
- **Dependências:** `pandas`, `numpy`, `zipfile`.
- **Execução:** Biblioteca importada por [`src/app_format_qh.py`](../src/app_format_qh.py) e testada em [`tests/test_qh_format.py`](../tests/test_qh_format.py) (`python -m unittest discover -s tests -t . -v`).

### `src/app_carregar_qh.py`
- **Arquivo:** [`src/app_carregar_qh.py`](../src/app_carregar_qh.py)
- **Categoria:** Interface / Carga
- **Descrição:** Aplicação Streamlit standalone para carregar CSVs de quadros horários em um GTFS base com pré-visualização das alterações e confirmação explícita antes de clonar moldes de viagem.
- **Responsabilidades:**
  - Receber upload de um ZIP GTFS base e um ou mais CSVs de QH (`horarios_*.csv`).
  - Executar `detectar_alteracoes()` para listar substituições diretas, pendências de clonagem de molde (ex.: clonar de `U_REG` quando o calendário não existe) e itens ignorados.
  - Exigir confirmação explícita do usuário para clonagem, aplicar `aplicar_quadros_gtfs()` preservando a relação 1:1 `trips`/`frequencies` e disponibilizar o ZIP resultante para download.
- **Entradas:** Upload de ZIP GTFS base e arquivos CSV de QH (formato dos scripts `1`, `1.2`, `1.3` ou `1.4`).
- **Saídas:** Preview tabular das alterações na UI e download do arquivo ZIP GTFS atualizado.
- **Dependências:** `streamlit`, `pandas`, `src.gtfs_carregar_qh`.
- **Execução:**
  - `python -m streamlit run src/app_carregar_qh.py`

### `src/gtfs_carregar_qh.py`
- **Arquivo:** [`src/gtfs_carregar_qh.py`](../src/gtfs_carregar_qh.py)
- **Categoria:** Biblioteca Pura
- **Descrição:** Biblioteca pura que implementa leitura/escrita GTFS, parsing de CSVs de QH, matching de rotas/headsigns, clonagem controlada de moldes e atualização 1:1 de `frequencies.txt`, `trips.txt` e `stop_times.txt`.
- **Responsabilidades:**
  - Ler/escrever ZIPs GTFS (`read_gtfs()`, `write_gtfs()`, `clean_gtfs()`) e fazer parsing normalizado dos CSVs (`parse_qh_csv()`, `extrair_service_id_do_nome()`).
  - Localizar `route_id` e `trip_headsign` correspondentes (`encontrar_route_id()`, `match_headsign()`) e gerar plano de alterações (`detectar_alteracoes()` → lista de `ItemPlanejado`).
  - Aplicar os novos quadros (`aplicar_quadros_gtfs()`) criando um `trip_id` exclusivo por faixa de frequência e alinhando o `departure_time` inicial em `stop_times.txt` ao `start_time` da banda (garantindo unicidade `[feed_start_date, id_viagem]` no modelo SMTR/Sigmob/dbt).
- **Entradas:** Dicionário de DataFrames GTFS, objetos `QuadroHorarioCSV` e flags de permissão de clonagem.
- **Saídas:** Lista de `ItemPlanejado` e dicionário de DataFrames GTFS atualizado (ou bytes ZIP).
- **Dependências:** `pandas`, `numpy`, `zipfile`, `dataclasses`, `unicodedata`, `uuid`.
- **Execução:** Biblioteca importada por [`codigos_py/1.5_carregar_qh_no_gtfs.py`](../codigos_py/1.5_carregar_qh_no_gtfs.py) e [`src/app_carregar_qh.py`](../src/app_carregar_qh.py), e testada em [`tests/test_gtfs_carregar_qh.py`](../tests/test_gtfs_carregar_qh.py) (`python -m unittest discover -s tests -t . -v`).

---

## Grupo G — Abas da Interface Streamlit (`src/tabs/`)

Cada módulo em `src/tabs/` expõe uma função `render()` chamada por [`src/app.py`](../src/app.py), coleta os parâmetros do formulário e invoca [`src/runner.py`](../src/runner.py) (`run_script()`).

| Módulo da Aba | Título na UI (`src/app.py`) | Modos / Opções | Scripts Executados em `codigos_py/` |
|---|---|---|---|
| [`src/tabs/tab_1_extrair_qh.py`](../src/tabs/tab_1_extrair_qh.py) | `1. Extrair QH` | `1`, `1.1`, `1.2`, `1.3`, `1.3.1`, `1.4`, `1.5` | `1_extrair_qh_especificado_no_gtfs.py`, `1.1_extrair_qh_partidas.py`, `1.2_combinar_quadros_horarios.py`, `1.3_qh_a_partir_de_partidas.py`, `1.3.1_conferir_partidas_geradas.py`, `1.4_qh_pf_a_partir_do_gtfs.py`, `1.5_carregar_qh_no_gtfs.py` |
| [`src/tabs/tab_2_ajustar_st.py`](../src/tabs/tab_2_ajustar_st.py) | `2. Ajustar ST` | Único | `2_ajustar_stop_times.py` |
| [`src/tabs/tab_4_trajetos.py`](../src/tabs/tab_4_trajetos.py) | `4. Alternativos` | Único | `4_trajetos_alternativos.py` |
| [`src/tabs/tab_5_juntar.py`](../src/tabs/tab_5_juntar.py) | `5. Juntar GTFS` | `5`, `5.1`, `5.2`, `5.3`, `5.4` | `5_juntar_gtfs.py`, `5.1_juntar_gtfs_sppo.py`, `5.2_juntar_gtfs_simples.py`, `5.3_juntar_gtfs_substituicao.py`, `5.4_eliminar_duplicatas_gtfs.py` |
| [`src/tabs/tab_6_shapes.py`](../src/tabs/tab_6_shapes.py) | `6. Shapes` | Único | `6_gerar_shapes.py` |
| [`src/tabs/tab_7_partidas.py`](../src/tabs/tab_7_partidas.py) | `7. Partidas` | `7`, `7.1`, `7.2` | `7_lista_partidas.py`, `7.1_resumo_os_domingo_du.py`, `7.2_combinador_partidas.py` |
| [`src/tabs/tab_8_extensoes.py`](../src/tabs/tab_8_extensoes.py) | `8. Extensões` | `Padrão (Script 8)`, `Formato Rio (Script 8.1)` | `8_gerar_extensoes.py`, `8.1_gerar_extensoes_rio.py` |
| [`src/tabs/tab_9_filtrar.py`](../src/tabs/tab_9_filtrar.py) | `9. Filtrar` | Único (suporta upload/colagem CSV/TSV) | `9_filtrar_gtfs_por_lista.py` |
| [`src/tabs/tab_10_juntar_dois.py`](../src/tabs/tab_10_juntar_dois.py) | `10. Juntar Dois` | Único | `10_juntar_dois_gtfs.py` |

---

## Legado R (`codigos_R/` e `1.2 app_format_qh.R`)

O repositório preserva a implementação original em R apenas para referência histórica e rastreabilidade de regras herdadas:

- **`codigos_R/`:** contém as versões anteriores em R das etapas `1` a `7` (`1. extrair_qh_especificado_no_gtfs.R` até `7. lista_partidas.R`), utilitários de conversão RDS/Parquet (`0.0` e `0.1`) e download de planilhas de desvios (`0.2`). A subpasta `codigos_R/datados/` reúne scripts obsoletos (`X.*.R`).
- **[`1.2 app_format_qh.R`](../1.2%20app_format_qh.R):** aplicação Shiny legada para formatação e comparação de quadros horários, integralmente substituída pela versão Python em [`src/app_format_qh.py`](../src/app_format_qh.py) + [`src/qh_format.py`](../src/qh_format.py).

Novas manutenções e execuções em produção devem utilizar exclusivamente os scripts Python em `codigos_py/` e as aplicações em `src/`.
