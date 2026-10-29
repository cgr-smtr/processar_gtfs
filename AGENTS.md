# AGENTS.md — Memória de sessão (Processar GTFS)

Arquivo auto-carregado em toda sessão. Mantenha atualizado e conciso.
Para histórico detalhado, ver `.opencode/memory.md`.

## Projeto
Pipeline de processamento/validação de GTFS do transporte público do Rio
(ajustar stop_times por GPS, combinar modais SPPO/BRT/Rio, gerar shapes,
partidas, extensões e GTFS filtrados). Python + `pandas`/`geopandas`; orquestrado
por aplicação Streamlit (`src/app.py`). Contém versões antigas em R (`codigos_R/`).

## Estrutura
- `src/app.py`: interface Streamlit com abas para cada etapa do pipeline.
- `src/runner.py`: executa scripts como subprocesso passando `--config <json>`;
  barra de progresso por tempo decorrido e logs em tempo real.
- `src/tabs/tab_*.py`: uma aba por etapa (QH, ajustar ST, alternativos,
  juntar, shapes, partidas, extensões, filtrar, juntar dois).
- `codigos_py/`: scripts Python do pipeline, numerados 1 a 10
  (`1_extrair_qh_especificado_no_gtfs` [faixas de frequência],
  `1.1_extrair_qh_partidas` [grade de partidas detalhadas],
  `1.2_combinar_quadros_horarios` [combina faixas entre serviços/dias por horário;
  defaults do usuário (2026-10-20): estudo 2030/02Q, janela 06:00–21:00,
  `linhas_rodar=[]` = todas as linhas],
  `1.3_qh_a_partir_de_partidas` [QH factíveis a partir de tabela de contagens
  por linha/sentido/faixa — aceita formato longo (Faixa+Partidas) ou wide
  (uma coluna por faixa, melt automático); headsign derivado da vista;
  exporta CSVs por quadro + Excel com uma aba por linha; default F_REG;
  sem aba Streamlit ainda — CLI/`--config`],
  `1.3.1_conferir_partidas_geradas` [conferência independente dos quadros do 1.3
  contra a entrada — reexpande bandas, compara (servico,sentido,faixa), confere
  Excel; relatório + exit 0/1; sem aba Streamlit],
  `1.4_qh_pf_a_partir_do_gtfs` [quadros de ponto facultativo (`F_REG`) direto do
  `frequencies.txt` de um GTFS (default `0171_gtfs_out_26_1E_ret.zip` na raiz);
  filtra `route_type=700`, exclui agência MOBI-Rio (`20001`) inteira; base DU
  (`U_REG`) p/ Internorte/Intersul/Transcarioca/Santa Cruz e sábado (`S_REG`) p/
  GTU/TUSE; saída própria `qh_por_linha_pf_gtfs/{estudo}Q/` com CSVs + Excel +
  gate; sem aba Streamlit],
  `2_ajustar_stop_times`, `4_trajetos_alternativos`, `5_juntar_gtfs` e variantes 5.1–5.4,
  `6_gerar_shapes`, `7_lista_partidas` + 7.1 + 7.2 (tipos de dia `du`/`sab`/`dom`/`pf`
  [ponto facultativo, `F_REG`]; `get_pattern` lança `ValueError` p/ token desconhecido),
  `8_gerar_extensoes` + 8.1,
  `9_filtrar_gtfs_por_lista`, `10_juntar_dois_gtfs`,
  `investigar_os190_vs_gtfs0174` [investigação OS 190/191 (Eleição 2º turno) ×
  GTFS 0174 — cruza OS×D_REG por serviço/sentido/faixa, classifica serviços e
  gera CSVs + resumo na raiz; fórmula default `floor` (bug `floor_mais_1`
  duplicava fronteiras de bandas contíguas — corrigido 2026-10-22); versão
  generalizada virou `2_comparar_os_gtfs.py` no projeto irmão `validacao_gtfs`
  (todos os tipos de dia, sem hardcode)],
  `comparar_duas_os` [compara **duas OS** "Resumo por Dia" entre si, por
  linha/sentido/tipo de dia/faixa — delta A−B, status IGUAL/A_MAIOR/B_MAIOR/
  SÓ_A/SÓ_B, sem GTFS; CSVs + resumo na raiz; defaults: PO 1ºEst Nov ×
  2ºEst Out 2026; sem aba Streamlit]).
- Projeto irmão `C:\github_repositories\validacao_gtfs` (independente, com git):
  itens numerados `0_validar_gtfs_entrada.py` (pró-pipeline), `1_validar_gtfs.py`
  (antes `validar_gtfs.py`, renomeado 2026-10-22) e `2_comparar_os_gtfs.py`
  (compara conteúdo de partidas OS × GTFS para todos os tipos de dia — entradas
  `OS.csv`/`GTFS.zip` na raiz com auto-detecção); tem `README.md`,
  `CHANGELOG.md` e `memory.md` próprios.
- `codigos_R/`: versões antigas do pipeline em R (`datados/` = obsoletos).
- `Iniciar_App.bat`: atalho de inicialização da aplicação.
- `CONTEXTO.md`, `CAPABILITIES_AND_AGENTS.md`, `plano_de_implementacao.md`: docs.
- `PLANO_1.3_qh_a_partir_de_partidas.md`: plano pronto para execução (2026-10-02) do
  script `1.3` — gerar QH no formato do script 1 a partir da lista de partidas do 7/7.2.
  Invariante: contagem de partidas por faixa preservada; headway regularizado.
  Atenção: agrupar por `(ciclo, faixa)`, nunca só por `faixa` (a faixa dá a volta em 24h).

## Convenções
- Scripts aceitam `--config <json>` e mantêm retrocompatibilidade com variáveis
  hardcoded no topo (`BASE_DADOS`, ano, mês, estudo).
- Leitura de CSVs/GTFS com `dtype=str`.
- `clean_gtfs()` = limpeza em cascata (remove registros órfãos), equivalente ao
  `gtfstools::filter_by_trip_id`.
- Projeção geográfica: EPSG:31983. Saídas em `resultados/` e `dados/`.
- CSVs de QH (scripts 1/1.2/1.3/1.4) saem com `trip_id` **vazio por design** —
  a coluna é preenchida ao subir o quadro no sistema **Sigmob** e salvar.
- "Decimais"/segundos nas partidas do script 7 são herdados do `stop_times`
  ajustado por GPS (script 2); correr `.round('min')` só se o usuário pedir.
- **Extensão de linha (7, 8, 8.1):** regra única = comprimento GIS do shape
  (LineString → EPSG:31983 → `astype(int)`), **máximo por itinerário,
  excluindo trips `EXCEP` e desvios (`[` no headsign)** — sem isso o `max()`
  pega o shape de exceção e infla a extensão (ex.: 006 Ida 8460→8866).
  A "verdade do GTFS" (`shape_dist_traveled`) fica ~21 m acima do GIS; o
  usuário validou o valor GIS (histórico Sigmob). Scripts 8/8.1 já tinham o
  filtro; o 7 recebeu em 2026-10-22. 7 NÃO filtra `route_type` na extensão
  (precisa cobrir BRT 702 das partidas); 8/8.1 filtram `[3,700]` (listagem só-SPPO).
- Docs e comentários em português. Execução: `python -m streamlit run src/app.py`.

## Memória: como usar
- Sessão começa: ler `AGENTS.md` (já carregado) e, se pedido, `.opencode/memory.md`.
- Ao terminar tarefa relevante: atualizar `AGENTS.md` (estado atual), `CHANGELOG.md` (registro de alterações relevantes), `CONTEXTO.md` (Contexto geral do projeto), `README.md` (seção do script afetado) e `.opencode/memory.md` (histórico) antes de encerrar.
- NUNCA registrar segredos/credenciais nestes arquivos.