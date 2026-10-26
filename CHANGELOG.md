# Changelog — Processar GTFS

Todas as alterações notáveis neste projeto serão documentadas neste arquivo.

O formato é baseado no [Keep a Changelog](https://keepachangelog.com/pt-BR/1.0.0/).

## [Não lançado] - 2026-10-23

### Alterado
- `codigos_py/7_lista_partidas.py` — defaults do topo atualizados para o estudo corrente: `ano_gtfs` 2026 → **2030**, `estudo_gtfs` 02 → **05** e `endereco_gtfs` → **`gtfs/2030/0175_gtfs_out_26_1E_ret3.zip`** (defaults dos scripts 8/8.1 inalterados).
- Documentação da regra de extensão (7, 8 e 8.1) concluída: `README.md` (seção 7 detalha o cálculo GIS com exclusão de `EXCEP`/desvios e a não filtragem de `route_type`; seções 8 e 8.1 detalham os filtros) e `CONTEXTO.md` (sessão 2026-10-22 com diagnóstico, correção e verificação).

## [Não lançado] - 2026-10-22

### Adicionado
- Novo script `codigos_py/investigar_os190_vs_gtfs0174.py`: investigação da reclamação "351 serviços da OS não estão no GTFS 0174 (D_REG)" (testes dbt pós-carga) — cruza OS × `D_REG` por serviço/sentido/faixa horária, classifica serviços (`EM_D_REG`/`SO_EXCEP_EVENTO`/`SEM_DOMINGO_NO_GTFS`) e gera `relatorio_*_servicos.csv`, `relatorio_*_celulas.csv` e `resumo_*.txt` na raiz (gitignorados). Achado: 351 = serviços da OS com partidas de domingo; na OS 191 (utf-8-sig) todos os 364 serviços constam em `D_REG`, com 95,2% das células exatas (269 divergentes, +270 partidas).

### Alterado
- `codigos_py/7_lista_partidas.py` — cálculo de extensões agora **exclui trips `EXCEP` e desvios** (`[` no headsign) antes do `max()` por itinerário, alinhando o script 7 à regra dos scripts 8/8.1 e à lista histórica de extensões do Sigmob (ex.: 006 Ida: 8866 → **8460**). Antes o `max()` pegava o shape de exceção e inflava a extensão em até ~400 m. Merge da extensão ganhou `fillna(0).astype(int)` (grupos que só tinham EXCEP ficariam NaN — verificado: 0 rows afetados). GTFS 2030-10-03Q regenerado: 33,9% das 163.179 linhas mudaram (55.321); conferência contra a lista histórica: 49/54 exatos, todas ≤20 m (5 casos = versão anterior do GTFS, aceita pelo usuário); BRT 702 intacto (17.566 linhas, 0 nulas). Scripts 8/8.1 inalterados (já filtram EXCEP/desvio).
- `investigar_os190_vs_gtfs0174.py`: default da `formula` de `floor_mais_1` para **`floor`** — o `+1` duplicava as fronteiras entre bandas contíguas de `frequencies.txt` (fim de uma = início da próxima). Convenção alinhada ao script 7 (`while current_time < ed`, `7_lista_partidas.py:184`); relatórios da OS 190 regenerados (células exatas 1.585 → 3.321; divergências 4.053 → 1.671).
- Migração para o projeto irmão `C:\github_repositories\validacao_gtfs` (v4.0.0): o cruzamento virou o item de pipeline **`2_comparar_os_gtfs.py`** (todos os tipos de dia, entradas `OS.csv`/`GTFS.zip` sem hardcode); `validar_gtfs.py` renomeado para `1_validar_gtfs.py` e wrapper `0_` corrigido. Detalhes no `CHANGELOG.md`/`memory.md` do próprio `validacao_gtfs`.

## [Não lançado] - 2026-10-19

### Adicionado
- Novo script `codigos_py/1.4_qh_pf_a_partir_do_gtfs.py`:
  - Gera quadros de **ponto facultativo** (`F_REG`) no formato do Script 1 direto do `frequencies.txt` de um GTFS (default `0171_gtfs_out_26_1E_ret.zip` na raiz do repositório).
  - Filtros: agência MOBI-Rio (`20001`) excluída inteira; `route_type=700` apenas (exclui BRT 702 e Metrô 200); base de partidas por `mapa_base` — DU (`U_REG`) em Internorte/Intersul/Transcarioca/Santa Cruz e sábado (`S_REG`) em GTU/TUSE.
  - Agrupa bandas por `(trip_short_name, trip_headsign)`; gate antes de gravar (bandas preservadas entrada×saída, `n=ceil((end−start)/headway)≥1`, total de partidas in = out).
  - Saída própria em `qh_por_linha_pf_gtfs/{estudo}Q/`: CSVs por quadro + Excel `quadros_partidas_F_REG.xlsx` (uma aba por linha) + `relatorio_bandas.csv` (com `service_id_origem`) e `_resumo.txt`; sem aba Streamlit (CLI/`--config`).
  - Execução real no 0171: 9.519 bandas → 804 quadros, 40.744 partidas, gate OK; rotas sem bandas na base esperada (`SP766`, `899`, `SV899`, `LECD126`) registradas como aviso no `_resumo.txt`.

## [Não lançado] - 2026-10-16

### Alterado
- Script `codigos_py/7_lista_partidas.py` — novo tipo de dia **ponto facultativo** (`pf`):
  - `get_pattern`: `pf` → padrão `'F'` (casa com o `service_id` `F_REG` do GTFS); tipo desconhecido agora lança `ValueError` listando os tokens válidos (antes retornava `''` e `str.contains('')` casava com **todos** os trips).
  - Default de `tipos_dia` ampliado para `['du', 'sab', 'dom', 'pf']`; saída `partidas_pf.csv` (indivíduo) e linhas com `tipo_dia='pf'` no consolidado `partidas_{sufixo}.csv/.parquet`.
  - Default de `endereco_gtfs` trocado para o GTFS corrente `gtfs/2026/gtfs_combi_2026-10-02Q_RIO.zip` (literal, sem interpolar `ano_gtfs`).
  - Leitura do `--config` robusta a BOM (`utf-8-sig`).
- Aba `src/tabs/tab_7_partidas.py`:
  - Default do campo "GTFS Entrada" aponta para `gtfs_combi_2026-10-02Q_RIO.zip`.
  - Multiselect `tipos_dia` com opção/default `pf` (help: ponto facultativo, `service_id F_REG`).
- Verificado em pasta temporária (não toca `resultados/partidas`): reexpansão independente de `frequencies.txt` por `service_id` bate para os 4 tipos (du 2.165/U_REG, sab 2.021/S_REG, dom 1.930/D_REG, pf 2.021/F_REG), pf sem vazamento de outros serviços; guard `tipos_dia=['foo']` → exit 1. `7.1`/`7.2` filtram `== 'du'/'dom'` e ignoram `pf`.

## [Não lançado] - 2026-10-15

### Adicionado
- Novo script `codigos_py/1.3_qh_a_partir_de_partidas.py`:
  - Gera quadros de horário factíveis no formato do Script 1 a partir de uma tabela agregada de contagens de partidas por linha/sentido/faixa (sem depender do GTFS).
  - Aceita a tabela em dois formatos: **wide** (uma coluna por faixa horária, com melt automático para o formato interno) e **longo** (`Faixa` + `Partidas_Faixa`); colunas tolerantes a acento/caixa; `-`/vazio tratados como 0 (`traco_ausente`: `zerar`|`erro`).
  - Deriva o `trip_headsign` a partir da vista (`Origem - Destino`): Ida → destino, Volta → origem, Circular → literal `Circular`.
  - Por faixa com n>0, gera 1 banda com `h = (dur//n // granularidade)*granularidade` (default 300s; se `h==0`, usa o bruto `dur//n` — a contagem de partidas é sempre preservada) e `end = start + n*h` (sem vazamento da faixa).
  - Gate duro antes de gravar: reexpansão = entrada, nenhuma banda fora da faixa, headway múltiplo da granularidade, total de partidas de entrada = saída.
  - Saída: CSVs por quadro no formato idêntico ao Script 1 (`trip_id` vazio) em `qh_por_linha_partidas/`, mais `relatorio_bandas.csv` e `_resumo.txt`.
- Exportação Excel no script `1.3` (config `gerar_excel`, default ligado):
  - Um workbook `quadros_partidas_{service_id}.xlsx` com **uma aba por linha** (servico) e **um bloco por quadro** (sentido/headsign): cabeçalho `Linha` / `Trip headsign` / `Tipo dia` + tabela `Partida` | `Horário` | `Intervalo` (partidas expandidas das bandas, HH:MM:SS; primeira `Intervalo` = `--`).
  - Gate específico: total de partidas expandidas no Excel deve bater com o total de saída (aborta se divergir).
- Novo script `codigos_py/1.3.1_conferir_partidas_geradas.py`:
  - Conferência independente dos quadros gerados pelo `1.3` contra a tabela de entrada: relê a entrada (wide/longo) e os `horarios_*.csv` do disco, reexpande as bandas e compara contagens por (servico, sentido, faixa).
  - Detecta anomalias `FALTANDO`/`SOBRANDO`/`ÓRFÃO` (grupos e arquivos), `BANDA` (headway < 1, não divisível, start fora da faixa, end vaza da faixa) e divergências `CELULA` (com Δ); confere também o Excel (blocos por quadro vs expansão dos CSVs).
  - Grava `relatorio_conferencia_partidas.csv` + `_conferencia_resumo.txt` na pasta conferida; exit code 0/1 (CI-friendly). Validado em SPPO (10.766 células / 38.730 partidas / 768 blocos Excel) e RIO (574 células / 2.021 partidas / 41 blocos Excel), ambos 0 anomalias; teste negativo confirmou detecção (exit 1).

## [Não lançado] - 2026-10-06

### Removido
- Script `codigos_py/0_validar_gtfs_entrada.py`, a aba **0. Validar** da interface Streamlit (`src/tabs/tab_0_validar.py`) e a seção correspondente do `README.md`: movidos para o novo projeto independente `C:\github_repositories\validacao`.
- Módulo `validacao/` (`validar_gtfs.py`, `README.md`, `CHANGELOG.md` e dados de exemplo): mesmo destino, agora na raiz do projeto `validacao`, que passa a documentar suas próprias alterações.

## [Não lançado] - 2026-09-28

### Adicionado
- Novo script `codigos_py/7.1_resumo_os_domingo_du.py`:
  - Combina partidas de diferentes tipos de dia no formato da tabela de OS, gerando um resumo de partidas por faixa horária para Domingo a partir da base consolidada do Script 7 (`partidas.parquet` ou `partidas.csv`).
  - Importa viagens de Dia Útil (`tipo_dia == 'du'`) na janela 06:00:00 às 20:00:00 e preserva viagens de Domingo (`tipo_dia == 'dom'`) na madrugada e noite.
  - Pivota os dados pelas 14 faixas horárias operacionais padrão, intercalando colunas de Partidas e Quilometragem em branco.
  - Exporta resultados em Excel (`.xlsx`) e CSV (`utf-8-sig`, delimitador `;`).
- Novo script `codigos_py/7.2_combinador_partidas.py`:
  - Combina partidas de diferentes tipos de dia no formato do arquivo de partidas gerado pelo Script 7.
  - Gera um arquivo de partidas consolidado para Domingo: mantém a madrugada e a noite do Domingo e substitui o bloco diurno (06:00:00–20:00:00, exclusivo) pelas partidas de Dia Útil.
  - Compatibiliza o campo `intervalo` nas transições entre os blocos (05:59→06:00 e 19:59→20:00).
  - Exporta no mesmo formato do Script 7 (`trip_short_name`, `direction_id`, `departure_time`, `intervalo`, etc.).
- Novo script `codigos_py/1.2_combinar_quadros_horarios.py`:
  - Combina quadros de horários entre diferentes serviços/calendários com recorte por faixa de horário.
  - Permite gerar novos quadros de domingo (`D_REG`) aproveitando faixas de dia útil (`U_REG`) entre 06:00:00 e 20:00:00 (exclusivo).
  - Preserva faixas de madrugada e noite do serviço de destino original, com ajuste fino de bordas (*clipping*) nos limites configurados.
  - Suporta linhas sem quadro de destino prévio, gerando o quadro a partir da faixa importada.
  - Saída no formato idêntico ao script 1 (`trip_id,trip_headsign,trip_short_name,start_time,end_time,headway_secs`), salva em pasta dedicada (`qh_por_linha_combinado/...`).
  - Suporta execução via `--config <arquivo_json>` ou string JSON inline.
  - Validado com dados reais da linha 378.
- Novo script `codigos_py/1.1_extrair_qh_partidas.py`:
  - Extrai quadros de horários com partidas detalhadas de cada viagem a partir do GTFS.
  - Suporta viagens expandidas via `frequencies.txt` e viagens regulares com partidas em `stop_times.txt`.
  - Calcula a ordem de partida (`ordem_partida`: 1, 2, 3...) e o intervalo em minutos em relação à viagem anterior (`intervalo_min`).
  - Exporta arquivos CSV individuais organizados e sanitizados por linha, sentido/letreiro e calendário/serviço (`partidas_{servico}_{vista}_{calendario}.csv`).
  - Compatível com passagem de parâmetros via JSON (`--config <json>`) e execução autônoma via terminal.
  - Validado com dados reais do GTFS combinado `gtfs_combi_2026-09-01Q.zip` para 23 linhas (gerando 123 arquivos CSV e 5.969 partidas exportadas com sucesso).
