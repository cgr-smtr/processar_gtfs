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
- `src/tabs/tab_*.py`: uma aba por etapa (validar, QH, ajustar ST, alternativos,
  juntar, shapes, partidas, extensões, filtrar, juntar dois).
- `codigos_py/`: scripts Python do pipeline, numerados 0 a 10
  (`0_validar_gtfs_entrada`, `1_extrair_qh`, `2_ajustar_stop_times`,
  `4_trajetos_alternativos`, `5_juntar_gtfs` e variantes 5.1–5.4,
  `6_gerar_shapes`, `7_lista_partidas`, `8_gerar_extensoes` + 8.1,
  `9_filtrar_gtfs_por_lista`, `10_juntar_dois_gtfs`).
- `codigos_R/`: versões antigas do pipeline em R (`datados/` = obsoletos).
- `Iniciar_App.bat`: atalho de inicialização da aplicação.
- `CONTEXTO.md`, `CAPABILITIES_AND_AGENTS.md`, `plano_de_implementacao.md`: docs.

## Convenções
- Scripts aceitam `--config <json>` e mantêm retrocompatibilidade com variáveis
  hardcoded no topo (`BASE_DADOS`, ano, mês, estudo).
- Leitura de CSVs/GTFS com `dtype=str`.
- `clean_gtfs()` = limpeza em cascata (remove registros órfãos), equivalente ao
  `gtfstools::filter_by_trip_id`.
- Projeção geográfica: EPSG:31983. Saídas em `resultados/` e `dados/`.
- Docs e comentários em português. Execução: `python -m streamlit run src/app.py`.

## Memória: como usar
- Sessão começa: ler `AGENTS.md` (já carregado) e, se pedido, `.opencode/memory.md`.
- Ao terminar tarefa relevante: atualizar `AGENTS.md` (estado atual) e
  `.opencode/memory.md` (histórico) antes de encerrar.
- NUNCA registrar segredos/credenciais nestes arquivos.