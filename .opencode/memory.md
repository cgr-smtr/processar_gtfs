# Memória de longo prazo — Processar GTFS

Registro de decisões, histórico e contexto detalhado. Complementa o `AGENTS.md`.
Preferência: adicionar entradas no topo, datadas.

---

## 2026-08-27 — Criação do banco de memória

- Criados `AGENTS.md` e `.opencode/memory.md`.
- Projeto central da SMTR: pipeline completo de processamento de GTFS do Rio de
  Janeiro (SPPO, BRT e Rio), do ajuste de horários por GPS até exportações GIS.
- Stack: Python (pandas, numpy, geopandas, shapely, pyogrio, pyarrow, loguru,
  tqdm) + Streamlit. Existe ainda um `.Rproj` com código R antigo em `codigos_R/`
  (pasta `datados/` contém versões sem uso).
- Fluxo principal: 0 validar → 2 ajustar ST → 5 juntar (ou 9/10 para
  excepcionalidades) → 6 shapes / 7 partidas / 8 extensões.
- `src/runner.py`: rodando via Streamlit, os scripts recebem `--config <json>`
  gerado temporariamente; sem o argumento usam hardcodes no topo do arquivo.
- Script `0_validar_gtfs_entrada` faz validações de padrão de `trip_headsign`
  (excepcionalidades `[...]`) e retorna exit code específico.
- Script `5_juntar_gtfs` cuida da versão pública `gtfs_rio-de-janeiro_pub.zip` e
  interna `gtfs_combi_YYYY-MM-QQ.zip`; variantes 5.1/5.2/5.3/5.4 cobrem casos
  específicos (modal único, concatenação simples, substituição de linhas,
  eliminação de duplicatas preservando EXCEP).
- Seqüência histórica de refatoração: pipeline original em R foi portado para
  Python; a interface Streamlit veio depois (com abas por script).
- Pontos de atenção: `Iniciar_App.bat` inicializa a aplicação; `stop_times.csv`
  na raiz é dado de trabalho, não código.