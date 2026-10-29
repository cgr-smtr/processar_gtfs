"""Investiga OS 190 (Eleição 2º turno) x GTFS 0174 (D_REG).

Responde à reclamação "351 serviços estão na OS e não estão no GTFS (D_REG)":
cruza a tabela de OS com o GTFS por serviço (trip_short_name), sentido
(direction_id) e faixa horária de domingo, classificando cada serviço e
gerando relatórios CSV + resumo TXT na raiz do repositório.

Uso:
    python codigos_py/investigar_os190_vs_gtfs0174.py [--config <json>]
"""

import os
import re
import json
import zipfile
import argparse
from pathlib import Path

import pandas as pd

# ==============================================================================
# LEITURA DE CONFIGURAÇÃO (Linha de Comando / Streamlit / JSON)
# ==============================================================================
_parser = argparse.ArgumentParser(
    description="Cruza OS 190 (Eleição 2º turno) com GTFS 0174 (D_REG) por "
                "serviço, sentido e faixa horária de domingo."
)
_parser.add_argument('--config', type=str, default='', help='Caminho para arquivo JSON de configuração')
_args, _ = _parser.parse_known_args()
_config = {}
if _args.config:
    if os.path.isfile(_args.config):
        with open(_args.config, 'r', encoding='utf-8-sig') as _f:
            _config = json.load(_f)
    else:
        try:
            _config = json.loads(_args.config)
        except Exception:
            with open(_args.config, 'r', encoding='utf-8-sig') as _f:
                _config = json.load(_f)

_REPO_RAIZ = Path(__file__).resolve().parent.parent

# Arquivos de entrada (default: na raiz do repositório)
end_os = Path(_config.get(
    "end_os",
    next((_REPO_RAIZ / p for p in os.listdir(_REPO_RAIZ) if p.startswith("190 - OS") and p.lower().endswith(".csv")), ""),
))
end_gtfs = Path(_config.get("end_gtfs", _REPO_RAIZ / "0174_gtfs_out_26_2E_ret.zip"))

# Prefixo dos relatórios gerados na raiz
prefixo = str(_config.get("prefixo", "os190_vs_gtfs0174"))

# service_id do quadro de domingo
service_id = str(_config.get("service_id", "D_REG"))

# Convenção de contagem da grade de frequencies, alinhada ao script 7
# (7_lista_partidas.py): partidas em [start, end) com passo headway_secs,
# n = floor((end-start)/headway) — exclui end alinhado. Com bandas contíguas
# (fim de uma = início da próxima) isso evita duplicar a fronteira.
# Alternativas via config: "floor_mais_1" (inclui end) ou "ceil".
formula = str(_config.get("formula", "floor"))
if formula not in ("floor_mais_1", "floor", "ceil"):
    raise SystemExit(f"formula inválida: {formula!r} (use floor_mais_1, floor ou ceil)")

# Encoding da OS (exportações da planilha são latin-1)
encoding_os = str(_config.get("encoding_os", "latin-1"))

# Tolerância (partidas) para flagrar divergência por célula
tolerancia = int(_config.get("tolerancia", 0))

# ==============================================================================
# FUNÇÕES UTILITÁRIAS
# ==============================================================================

SENTIDO_PAR_DIR = {"ida": "0", "volta": "1", "circular": "0"}


def carregar_os(caminho: Path):
    """Lê a OS no formato tabela (uma linha por serviço×sentido) e calcula
    partidas de domingo por faixa horária."""
    df = pd.read_csv(caminho, dtype=str, encoding=encoding_os)
    df.columns = [c.strip() for c in df.columns]
    col_servico = next((c for c in df.columns if "servi" in c.lower()), None)
    col_sentido = next((c for c in df.columns if "sentido" in c.lower()), None)
    col_vista = next((c for c in df.columns if "vista" in c.lower()), None)
    if not col_servico or not col_sentido:
        raise SystemExit("Coluna 'Serviço' ou 'Sentido' não encontrada na OS")

    df["svc"] = df[col_servico].astype(str).str.strip()
    df["sentido"] = df[col_sentido].astype(str).str.strip()
    df["vista"] = df[col_vista].astype(str).str.strip() if col_vista else ""

    part_cols = [c for c in df.columns if c.lower().startswith("partidas")]

    def tipo_dia(c: str):
        m = re.search(r"-\s*(Dia .til|S.bado|Domingo|Ponto Facultativo)", c)
        return m.group(1) if m else None

    dom_cols = [c for c in part_cols if tipo_dia(c) == "Domingo"]

    def faixa_de(c: str):
        m = re.match(r"Partidas\s+(\d+)h.*?(\d+)h", c)
        return (int(m.group(1)), int(m.group(2)))

    faixas = {}
    for c in dom_cols:
        a, b = faixa_de(c)
        col = f"f_{a:02d}_{b:02d}"
        df[col] = pd.to_numeric(df[c].str.replace(",", ".", regex=False), errors="coerce").fillna(0)
        faixas[(a, b)] = col

    if not faixas:
        raise SystemExit("Nenhuma coluna de partidas de Domingo encontrada na OS")

    col_f = list(faixas.values())
    df["n_dom"] = df[col_f].sum(axis=1)
    df["dir"] = df["sentido"].str.lower().map(SENTIDO_PAR_DIR).fillna("")
    return df, faixas


def reexpandir_dominio(gtfs: dict, service_id: str, formula: str) -> pd.DataFrame:
    """Reexpande as frequencies do service_id alvo em partidas por
    (trip_short_name, direction_id, hora_do_dia)."""
    trips = gtfs["trips.txt"]
    freq = gtfs["frequencies.txt"]
    alvo = trips[trips["service_id"] == service_id]
    f = freq[freq["trip_id"].isin(set(alvo["trip_id"]))].merge(
        alvo[["trip_id", "trip_short_name", "direction_id"]], on="trip_id"
    )
    f["start"] = pd.to_timedelta(f["start_time"]).dt.total_seconds()
    f["end"] = pd.to_timedelta(f["end_time"]).dt.total_seconds()
    f["hw"] = pd.to_numeric(f["headway_secs"])

    if formula == "floor_mais_1":
        f["n"] = ((f["end"] - f["start"]) // f["hw"] + 1).astype(int)
    elif formula == "floor":
        f["n"] = ((f["end"] - f["start"]) // f["hw"]).astype(int)
    else:  # ceil
        import numpy as np
        f["n"] = np.ceil((f["end"] - f["start"]) / f["hw"]).astype(int)

    linhas = []
    for _, r in f.iterrows():
        for k in range(int(r["n"])):
            hora = ((r["start"] + k * r["hw"]) % 86400) / 3600
            linhas.append((r["trip_short_name"].strip(), str(r["direction_id"]).strip(), hora))
    return pd.DataFrame(linhas, columns=["svc", "dir", "hora"])


def carregar_gtfs(caminho: Path, arquivos) -> dict:
    z = zipfile.ZipFile(caminho)
    dados = {}
    for nome in arquivos:
        try:
            dados[nome] = pd.read_csv(z.open(nome), dtype=str)
        except KeyError:
            pass
    z.close()
    return dados


# ==============================================================================
# PRINCIPAL
# ==============================================================================

if not end_os or not Path(end_os).is_file():
    raise SystemExit(f"Arquivo de OS não encontrado: {end_os!s}")
if not Path(end_gtfs).is_file():
    raise SystemExit(f"GTFS não encontrado: {end_gtfs!s}")

print("INVESTIGAÇÃO: OS 190 (Eleição 2º turno) x GTFS 0174 (D_REG)")
print(f"  OS  : {end_os}")
print(f"  GTFS: {end_gtfs}")

gtfs = carregar_gtfs(end_gtfs, ["trips.txt", "frequencies.txt", "routes.txt", "feed_info.txt"])
trips = gtfs["trips.txt"]
os_df, faixas = carregar_os(Path(end_os))

linhas_d = set(trips.loc[trips["service_id"] == service_id, "trip_short_name"].dropna().str.strip())
linhas_excep = set(trips.loc[trips["service_id"] == "EXCEP", "trip_short_name"].dropna().str.strip())
linhas_todas = set(trips["trip_short_name"].dropna().str.strip())

dom = os_df[os_df["n_dom"] > 0].copy()
servicos_dom = sorted(dom["svc"].unique())

print(f"\nOS: {len(os_df)} linhas (serviço×sentido), {os_df['svc'].nunique()} serviços únicos")
print(f"OS com partidas de domingo: {len(servicos_dom)} serviços, {int(dom['n_dom'].sum())} partidas")
print(f"GTFS {service_id}: {len(linhas_d)} linhas (trip_short_name)")

# --- Classificação por serviço ----------------------------------------------
registros = []
for svc in servicos_dom:
    n_os = int(dom.loc[dom["svc"] == svc, "n_dom"].sum())
    vistas = sorted(set(dom.loc[dom["svc"] == svc, "vista"]) - {""})
    if svc in linhas_d:
        classe = "EM_D_REG"
    elif svc in linhas_excep:
        classe = "SO_EXCEP_EVENTO"
    else:
        classe = "SEM_DOMINGO_NO_GTFS"
    registros.append({
        "servico": svc,
        "classificacao": classe,
        "partidas_os_domingo": n_os,
        "vista_os": " | ".join(vistas),
        "services_gtfs": ",".join(sorted(trips.loc[trips["trip_short_name"] == svc, "service_id"].unique())),
    })
tab_servicos = pd.DataFrame(registros)

# --- Detalhe por (serviço, sentido, faixa) ----------------------------------
dep = reexpandir_dominio(gtfs, service_id, formula)
bounds = sorted(faixas)

def qual_faixa(h):
    for a, b in bounds:
        if a <= h < b:
            return (a, b)
    return None

dep["faixa"] = dep["hora"].apply(qual_faixa)
gtfs_f = dep.groupby(["svc", "dir", "faixa"]).size().reset_index(name="n_gtfs")

dom_com_dir = dom[dom["dir"] != ""]
os_rows = []
for _, r in dom_com_dir.iterrows():
    for (a, b), col in faixas.items():
        v = r[col]
        if v > 0:
            os_rows.append((r["svc"], r["dir"], (a, b), int(v), r["sentido"]))
os_f = pd.DataFrame(os_rows, columns=["svc", "dir", "faixa", "n_os", "sentido"])

m = os_f.merge(gtfs_f, on=["svc", "dir", "faixa"], how="left").fillna({"n_gtfs": 0})
m["n_gtfs"] = m["n_gtfs"].astype(int)
m["dif"] = m["n_os"] - m["n_gtfs"]
m["faixa"] = m["faixa"].apply(lambda t: f"{t[0]:02d}h-{t[1]:02d}h")
tab_celulas = m[["svc", "sentido", "faixa", "n_os", "n_gtfs", "dif"]].rename(columns={"svc": "servico"})

# --- Consolidação por serviço (partidas reexpandidas) -----------------------
part_por_dir = dep.groupby(["svc", "dir"]).size().reset_index(name="n_gtfs_dir")
os_por_dir = dom_com_dir.groupby(["svc", "dir"])["n_dom"].sum().reset_index().rename(columns={"n_dom": "n_os_dir"})
cmp_dir = os_por_dir.merge(part_por_dir, on=["svc", "dir"], how="outer").fillna(0)
tab_servicos = tab_servicos.merge(
    cmp_dir.groupby("svc")[["n_os_dir", "n_gtfs_dir"]].sum().reset_index().rename(
        columns={"svc": "servico", "n_os_dir": "partidas_os_por_sentido", "n_gtfs_dir": "partidas_dreg_reexpandidas"}
    ),
    on="servico", how="left",
).fillna({"partidas_os_por_sentido": 0, "partidas_dreg_reexpandidas": 0})

# --- Exportação -------------------------------------------------------------
raiz = Path(_config.get("pasta_saida", _REPO_RAIZ))
p_serv = raiz / f"relatorio_{prefixo}_servicos.csv"
p_cel = raiz / f"relatorio_{prefixo}_celulas.csv"
p_res = raiz / f"resumo_{prefixo}.txt"
tab_servicos.to_csv(p_serv, index=False, encoding="utf-8-sig")
tab_celulas.to_csv(p_cel, index=False, encoding="utf-8-sig")

contagem = tab_servicos["classificacao"].value_counts().to_dict()
ausentes = tab_servicos[tab_servicos["classificacao"] != "EM_D_REG"].sort_values(
    ["classificacao", "partidas_os_domingo"], ascending=[True, False]
)
presentes = tab_servicos[tab_servicos["classificacao"] == "EM_D_REG"]

cel_div = tab_celulas[tab_celulas["dif"].abs() > tolerancia]
cel_sem = tab_celulas[tab_celulas["n_gtfs"] == 0]
cel_exat = tab_celulas[tab_celulas["dif"] == 0]

linhas_res = []
linhas_res.append("INVESTIGACAO: OS 190 (Operacao Especial Eleicao 2o turno) x GTFS 0174 (D_REG)")
linhas_res.append(f"Formula de reexpansao de frequencies: {formula}")
linhas_res.append("")
linhas_res.append("== NUMEROS-CHAVE ==")
linhas_res.append(f"Servicos unicos na OS: {os_df['svc'].nunique()} ({len(os_df)} linhas servico x sentido)")
linhas_res.append(f"Servicos com partidas de domingo na OS: {len(servicos_dom)} "
                  f"({int(dom['n_dom'].sum())} partidas)  <-- '351' da mensagem")
linhas_res.append(f"Linhas ({service_id}) no GTFS: {len(linhas_d)}")
linhas_res.append(f"Classificacao dos {len(servicos_dom)} servicos de domingo da OS:")
for k in ("EM_D_REG", "SO_EXCEP_EVENTO", "SEM_DOMINGO_NO_GTFS"):
    linhas_res.append(f"  {k}: {contagem.get(k, 0)}")
linhas_res.append("")
linhas_res.append(f"== SERVICOS AUSENTES EM {service_id} ({len(ausentes)}) ==")
for _, r in ausentes.iterrows():
    linhas_res.append(f"  {r['servico']:10s} {r['classificacao']:20s} "
                      f"partidas_os={r['partidas_os_domingo']:4d}  vista={r['vista_os']}")
linhas_res.append("")
linhas_res.append(f"  Total de partidas OS de domingo sem cobertura em {service_id}: "
                  f"{int(ausentes['partidas_os_domingo'].sum())}")
linhas_res.append("")
linhas_res.append("== COMPARACAO POR CELULA (servico x sentido x faixa, apenas presentes em D_REG) ==")
cel_pres = tab_celulas[tab_celulas["servico"].isin(presentes["servico"])]
linhas_res.append(f"Celulas OS com partidas>0 (servicos em D_REG): {len(cel_pres)}")
linhas_res.append(f"  celulas exatas: {len(cel_pres[cel_pres['dif'] == 0])}")
linhas_res.append(f"  celulas divergentes (|dif| > {tolerancia}): {len(cel_pres[cel_pres['dif'].abs() > tolerancia])}")
pos = cel_pres[cel_pres["dif"] > tolerancia]
neg = cel_pres[cel_pres["dif"] < -tolerancia]
linhas_res.append(f"    OS > GTFS: {len(pos)} celulas (+{int(pos['dif'].sum())} partidas no OS)")
linhas_res.append(f"    GTFS > OS: {len(neg)} celulas ({int(neg['dif'].sum())} partidas no GTFS)")
linhas_res.append(f"  celulas sem nenhuma partida D_REG na mesma faixa: {len(cel_pres[cel_pres['n_gtfs'] == 0])}")
linhas_res.append("")
linhas_res.append("== CONTEXTO ==")
fi = gtfs.get("feed_info.txt")
feed_start = fi["feed_start_date"].iloc[0] if (fi is not None and not fi.empty and "feed_start_date" in fi) else "?"
linhas_res.append(f"- feed_start_date do feed_info.txt do GTFS: {feed_start}")
linhas_res.append("- A OS so preenche colunas de Domingo (Dia Util/Sabado/PF zerados): e uma OS especial de domingo.")
linhas_res.append(f"- {len(linhas_d - set(os_df['svc']))} linhas de {service_id} nao aparecem na OS "
                  "(quadro regular de domingo inclui linhas fora da OS especial).")
linhas_res.append("- '351' = contagem de servicos da OS com partidas no domingo; nao ha 351 servicos faltando.")
resumo = "\n".join(linhas_res)
p_res.write_text(resumo, encoding="utf-8")
print("\n" + resumo)
print("\nRelatórios gerados:")
print(f"  {p_serv}")
print(f"  {p_cel}")
print(f"  {p_res}")
