# processar_gtfs

Pipeline em Python para processamento, consolidação, exportação e manutenção de arquivos **GTFS** (*General Transit Feed Specification*) do transporte público do Rio de Janeiro (SMTR — Secretaria Municipal de Transportes).

O sistema resolve o ciclo operacional completo de preparação do GTFS para publicação e planejamento: recalcula tempos de parada com velocidades reais de GPS, combina modais (SPPO, BRT e Rio), remove duplicatas e registros órfãos, gera bases geoespaciais (Shapefile/GeoPackage), listas de partidas, extensões de linhas e quadros de horários (extração, combinação, geração a partir de contagens e carga de volta no GTFS).

**Tecnologias principais:** Python 3.13+, `pandas`, `numpy`, `geopandas`, `shapely`, `pyogrio`, `pyarrow`, `openpyxl` e `Streamlit`.

---

## Índice

- [Visão geral](#visão-geral)
- [Estrutura do projeto](#estrutura-do-projeto)
- [Instalação e configuração](#instalação-e-configuração)
- [Execução rápida](#execução-rápida)
- [Documentação técnica](#documentação-técnica)
- [Testes e validação](#testes-e-validação)
- [Desenvolvimento e contribuição](#desenvolvimento-e-contribuição)

---

## Visão geral

O repositório combina uma camada de orquestração visual em Streamlit com scripts Python independentes executáveis via linha de comando:

- **Interface principal ([`src/app.py`](src/app.py))**: organiza as etapas do pipeline em abas, recebe parâmetros globais na barra lateral e executa cada script como subprocesso via [`src/runner.py`](src/runner.py) com logs em tempo real.
- **Apps standalone**:
  - [`src/app_format_qh.py`](src/app_format_qh.py): compara programações previstas coladas pelo usuário com o GTFS e exporta quadros de horários no formato do Script 1 (lógica pura em [`src/qh_format.py`](src/qh_format.py)).
  - [`src/app_carregar_qh.py`](src/app_carregar_qh.py): carrega CSVs de quadros de horários em um GTFS base, substituindo frequências por sentido e calendário (lógica pura em [`src/gtfs_carregar_qh.py`](src/gtfs_carregar_qh.py)).
- **Scripts do pipeline ([`codigos_py/`](codigos_py/))**: 24 scripts numerados cobrindo quadros de horários (`1` a `1.5`), ajuste de `stop_times` por GPS (`2`), trajetos alternativos (`4`), combinação/deduplicação de GTFS (`5` a `5.5`), shapes GIS (`6`), partidas (`7` a `7.2`), extensões (`8`, `8.1`) e sub-fluxo de excepcionalidades (`9`, `10`).
- **Testes automatizados ([`tests/`](tests/))**: suíte `unittest` cobrindo formatação de quadros, carga de QH no GTFS e integração CLI do script `1.5`.

**Características arquiteturais relevantes:**

- **Contrato `--config <json>`**: scripts aceitam um arquivo JSON de configuração gerado pela interface ou passado via CLI, preservando retrocompatibilidade com variáveis padrão no topo de cada script.
- **Integridade referencial (`clean_gtfs()`)**: limpeza em cascata que remove registros órfãos entre tabelas GTFS (`stop_times`, `frequencies`, `shapes`, `stops`, `routes`, `calendar`).
- **Geoprocessamento**: extensões e geometrias calculadas em projeção métrica `EPSG:31983`.
- **Integração Sigmob / dbt e validação externa**: CSVs de QH saem com `trip_id` vazio para preenchimento no Sigmob; a carga no GTFS (`1.5` / `app_carregar_qh`) preserva estritamente o invariante 1:1 entre `trips.txt` e `frequencies.txt` exigido pelo dbt. Validações pré/pós-pipeline e comparações OS × GTFS vivem no repositório irmão `validacao_gtfs`.

---

## Estrutura do projeto

```text
processar_gtfs/
├── src/                               # Interface Streamlit, runner de subprocessos e lógica pura
│   ├── app.py                         # Aplicação principal com abas do pipeline
│   ├── runner.py                      # Executor de scripts com --config e streaming de logs
│   ├── tabs/                          # Módulos de abas (uma aba por grupo de etapas)
│   ├── app_format_qh.py               # App standalone de formatação/análise de QH
│   ├── qh_format.py                   # Lógica pura de formatação de QH
│   ├── app_carregar_qh.py             # App standalone de carga de QH no GTFS
│   └── gtfs_carregar_qh.py            # Lógica pura de carga de QH no GTFS
├── codigos_py/                        # 23 scripts Python do pipeline (etapas 1 a 10)
├── tests/                             # Suíte de testes automatizados (unittest)
├── docs/                              # Documentação técnica detalhada (Nível 2)
├── codigos_R/                         # Versões legadas do pipeline em R (referência histórica)
├── nota_tecnica_avaliacao_quadros/    # Material técnico de apoio sobre avaliação de quadros
├── Iniciar_App.bat                    # Atalho Windows para ativar .venv e iniciar src/app.py
└── requirements.txt                   # Dependências Python do projeto
```

---

## Instalação e configuração

### 1. Criar e ativar o ambiente virtual (Windows)

```powershell
# PowerShell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

```bat
:: Prompt de Comando (CMD)
python -m venv .venv
.venv\Scripts\activate.bat
```

### 2. Instalar dependências

```powershell
pip install -r requirements.txt
```

### 3. Estrutura esperada de diretórios de dados

Por padrão, o pipeline lê insumos e grava resultados sob um diretório base (`BASE_DADOS`, padrão `C:/R_SMTR`):

- `C:/R_SMTR/dados/`: arquivos GTFS de entrada, viagens GPS (Parquet/CSV), `calendario.json`, `gtfs_cores.csv` e arquivos de substituição.
- `C:/R_SMTR/resultados/`: quadros de horários extraídos, listas de partidas e extensões geradas.

Todos os parâmetros, caminhos e opções do JSON `--config` estão detalhados em [`docs/configuration.md`](docs/configuration.md).

---

## Execução rápida

### Interface principal do pipeline (Streamlit)

Pelo atalho Windows [`Iniciar_App.bat`](Iniciar_App.bat) ou diretamente no terminal (acesso em `http://localhost:8501`):

```powershell
python -m streamlit run src/app.py
```

### Aplicações standalone

```powershell
# Formatação e comparação interativa de quadros de horários
python -m streamlit run src/app_format_qh.py

# Carga de quadros de horários (CSV) em um GTFS base
python -m streamlit run src/app_carregar_qh.py
```

### Execução de etapa específica via CLI

Qualquer script do pipeline pode ser executado individualmente passando um arquivo JSON de configuração:

```powershell
python codigos_py/<script>.py --config caminho/para/config.json
```

Para a sequência completa de execução, dependências entre etapas e fluxos alternativos, consulte [`docs/pipeline.md`](docs/pipeline.md).

---

## Documentação técnica

| Documento | Descrição |
| --- | --- |
| [`docs/architecture.md`](docs/architecture.md) | Organização modular, responsabilidades de `src/` e `codigos_py/`, decisões arquiteturais e integrações externas. |
| [`docs/pipeline.md`](docs/pipeline.md) | Fluxo completo de processamento em ordem de execução, dependências entre etapas, ramificações e diagrama Mermaid. |
| [`docs/scripts_reference.md`](docs/scripts_reference.md) | Catálogo pesquisável dos 23 scripts em `codigos_py/` e módulos de suporte, com entradas, saídas e comandos. |
| [`docs/configuration.md`](docs/configuration.md) | Contrato `--config`, parâmetros por script, constantes, variáveis de ambiente e estrutura de diretórios. |
| [`docs/data_dictionary.md`](docs/data_dictionary.md) | Esquemas, formatos, convenções de nomenclatura e ciclo de vida dos arquivos de entrada, intermediários e finais. |
| [`docs/testing.md`](docs/testing.md) | Suíte `unittest`, comandos de execução, gates internos, auditoria `1.3.1` e validação externa em `validacao_gtfs`. |
| [`docs/troubleshooting.md`](docs/troubleshooting.md) | Guia de diagnóstico e solução para erros reais conhecidos do pipeline e avisos sobre scripts descontinuados. |

---

## Testes e validação

Execute a suíte automatizada a partir da raiz do repositório:

```powershell
python -m unittest discover -s tests -t . -v
```

Para auditar de forma independente os quadros gerados pelo script `1.3`:

```powershell
python codigos_py/1.3.1_conferir_partidas_geradas.py --config caminho/para/config.json
```

Detalhes sobre cobertura, gates internos e validação externa estão em [`docs/testing.md`](docs/testing.md).

---

## Desenvolvimento e contribuição

- **Invariantes obrigatórios**: preserve leitura de CSVs/GTFS com `dtype=str`, limpeza em cascata via `clean_gtfs()`, projeção `EPSG:31983`, `trip_id` vazio nos CSVs de QH para o Sigmob e unicidade 1:1 de `trip_id` entre `trips.txt` e `frequencies.txt` na carga de QH (`1.5` / `app_carregar_qh`).
- **Retrocompatibilidade**: mantenha suporte ao parâmetro `--config <json>` sem quebrar a execução isolada com variáveis padrão no topo dos scripts.
- **Qualidade e documentação**: execute `python -m unittest discover -s tests -t . -v` após alterações e mantenha atualizados [`AGENTS.md`](AGENTS.md), [`CHANGELOG.md`](CHANGELOG.md) e os documentos técnicos em [`docs/`](docs/architecture.md).
