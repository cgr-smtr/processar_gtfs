"""Compara duas OS (formato "Resumo por Dia") por linha, sentido, tipo de dia e faixa.

Lê dois CSVs de OS exportados no formato consolidado "Resumo por Dia" (colunas
`partidas HH:MM-HH:MM - <Tipo de Dia>` intercaladas com quilometragem), derrete
as contagens para o formato longo e faz o cruzamento completo A × B por
(Servico, Sentido, TipoDia, Faixa). Delta = A − B; ausente = 0.

Status por célula:
    IGUAL   — A == B (par presente nos dois arquivos)
    A_MAIOR — A > B
    B_MAIOR — A < B
    SÓ_A    — (Serviço, Sentido) presente apenas em A
    SÓ_B    — (Serviço, Sentido) presente apenas em B

Saídas (na pasta configurada, default = raiz do repositório):
    relatorio_<prefixo>_celulas.csv   — grade completa (todas as células)
    relatorio_<prefixo>_servicos.csv  — totais por (Serviço, Sentido, TipoDia)
    resumo_<prefixo>.txt              — totais, serviços exclusivos, top deltas

Uso:
    python codigos_py/comparar_duas_os.py [--config <json|caminho>]
"""

import os
import re
import json
import argparse
from datetime import datetime
from pathlib import Path

import pandas as pd

try:
    sys_stdout = __import__("sys").stdout
    sys_stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

# ==============================================================================
# LEITURA DE CONFIGURAÇÃO (Linha de Comando / JSON)
# ==============================================================================
_parser = argparse.ArgumentParser(
    description="Compara duas OS (formato Resumo por Dia) por linha, sentido, tipo de dia e faixa."
)
_parser.add_argument('--config', type=str, default='', help='Caminho para arquivo JSON de configuração (ou JSON inline)')
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

# OS de referência (A) e de comparação (B); default = POs na raiz do repositório
end_os_a = Path(_config.get(
    "end_os_a",
    _REPO_RAIZ / "PO_1ºEst_31-10-26_Novembro - OS_todos os dias.csv",
))
end_os_b = Path(_config.get(
    "end_os_b",
    _REPO_RAIZ / "PO_2ºEst_24-10-26_Outubro_COMPLETO ret - OS_todos os dias.csv",
))

prefixo = str(_config.get("prefixo", "po1est_nov_vs_po2est_out"))
pasta_saida = Path(_config.get("pasta_saida", _REPO_RAIZ))

# Filtro opcional de tipos de dia (default: todos os encontrados no header)
tipos_dia_cfg = _config.get("tipos_dia", [])
if isinstance(tipos_dia_cfg, str):
    tipos_dia_cfg = [x.strip() for x in tipos_dia_cfg.split(',') if x.strip()]

# Encoding dos CSVs (exportações da planilha; utf-8 com BOM nos arquivos atuais)
encoding_os = str(_config.get("encoding_os", "utf-8-sig"))

# ==============================================================================
# FUNÇÕES UTILITÁRIAS
# ==============================================================================
# Colunas de partidas do formato "Resumo por Dia":
#   "partidas 06:00-09:00 - Dia Útil", "partidas 23:00-24:00 - Ponto Facultativo", ...
RE_PARTIDAS = re.compile(r"^partidas\s+(\d{2}:\d{2}-\d{2}:\d{2})\s+-\s+(.+)$", re.IGNORECASE)

ORDEM_SENTIDO = {"Ida": 0, "Volta": 1, "Circular": 2}
ORDEM_TIPO = {"Dia Útil": 0, "Sábado": 1, "Domingo": 2, "Ponto Facultativo": 3}


def log_msg(msg):
    t = datetime.now().strftime("%H:%M:%S")
    print(f"[{t}] {msg}")


def achar_coluna(colunas, termo):
    """Localiza a coluna cujo nome contém o termo (sem acento/caixa)."""
    import unicodedata
    def norm(s):
        s = unicodedata.normalize("NFKD", str(s))
        return "".join(c for c in s if not unicodedata.combining(c)).lower()
    alvo = norm(termo)
    for c in colunas:
        if alvo in norm(c):
            return c
    return None


def ler_os(caminho: Path) -> pd.DataFrame:
    """Lê a OS no formato 'Resumo por Dia' e valida a estrutura."""
    if not caminho.exists():
        raise SystemExit(f"Arquivo de OS não encontrado: {caminho}")
    try:
        df = pd.read_csv(caminho, dtype=str, encoding=encoding_os)
    except UnicodeDecodeError:
        df = pd.read_csv(caminho, dtype=str, encoding="latin-1")
    df.columns = [c.strip() for c in df.columns]
    return df


def derreter_os(df: pd.DataFrame, rotulo: str) -> pd.DataFrame:
    """Derrete as colunas 'partidas ... - Tipo' para (svc, sentido, tipo, faixa, n)."""
    col_svc = achar_coluna(df.columns, "servico")
    col_vista = achar_coluna(df.columns, "vista")
    col_cons = achar_coluna(df.columns, "consorcio")
    col_sent = achar_coluna(df.columns, "sentido")
    col_ext = achar_coluna(df.columns, "extensao")
    if not col_svc or not col_sent:
        raise SystemExit(f"[{rotulo}] Coluna 'Serviço' ou 'Sentido' não encontrada")

    id_cols = [c for c in (col_svc, col_vista, col_cons, col_sent, col_ext) if c]
    part_cols, mapa_faixa_tipo = {}, {}
    for c in df.columns:
        m = RE_PARTIDAS.match(c)
        if m:
            part_cols[c] = m.group(1)
            mapa_faixa_tipo[c] = m.group(2).strip()
    if not part_cols:
        raise SystemExit(f"[{rotulo}] Nenhuma coluna 'partidas HH:MM-HH:MM - Tipo' encontrada (formato 'Resumo por Dia')")

    longo = df.melt(id_vars=id_cols, value_vars=list(part_cols), var_name="_col", value_name="partidas")
    longo["faixa"] = longo["_col"].map(part_cols)
    longo["tipo_dia"] = longo["_col"].map(mapa_faixa_tipo)
    longo["partidas"] = pd.to_numeric(longo["partidas"].astype(str).str.strip(), errors="coerce")
    ruins = longo["partidas"].isna().sum()
    if ruins:
        raise SystemExit(f"[{rotulo}] {ruins} células de partidas não numéricas")

    longo = longo.rename(columns={
        col_svc: "servico", col_vista: "vista", col_cons: "consorcio",
        col_sent: "sentido", col_ext: "extensao",
    })
    longo["servico"] = longo["servico"].astype(str).str.strip()
    longo["sentido"] = longo["sentido"].astype(str).str.strip()
    for c in ("vista", "consorcio"):
        longo[c] = longo[c].fillna("").astype(str).str.strip()
    longo["partidas"] = longo["partidas"].astype(int)
    longo["extensao"] = pd.to_numeric(longo["extensao"], errors="coerce")

    cols_finais = ["servico", "vista", "consorcio", "sentido", "extensao", "tipo_dia", "faixa", "partidas"]
    longo = longo[cols_finais]

    # Chave (Serviço, Sentido) deve ser única na OS de origem
    pares = longo[["servico", "sentido"]].drop_duplicates()
    celulas_por_par = longo.groupby(["servico", "sentido"]).size()
    if (celulas_por_par != len(part_cols)).any():
        desvios = int((celulas_por_par != len(part_cols)).sum())
        raise SystemExit(f"[{rotulo}] {desvios} pares (Serviço, Sentido) sem o conjunto completo de faixas")
    if pares.duplicated().any():
        raise SystemExit(f"[{rotulo}] Chaves (Serviço, Sentido) duplicadas na OS")

    log_msg(f"[{rotulo}] {len(pares)} pares (Serviço, Sentido) | {len(part_cols)} células por par | "
            f"{len(mapa_faixa_tipo)} colunas de partidas ({len(set(mapa_faixa_tipo.values()))} tipos de dia)")
    return longo


# ==============================================================================
# EXECUÇÃO PRINCIPAL
# ==============================================================================
log_msg("Comparação de duas OS — partidas por linha, sentido, tipo de dia e faixa")
log_msg(f"A: {end_os_a.name}")
log_msg(f"B: {end_os_b.name}")

df_a_bruto = ler_os(end_os_a)
df_b_bruto = ler_os(end_os_b)

if list(df_a_bruto.columns) != list(df_b_bruto.columns):
    so_a = set(df_a_bruto.columns) - set(df_b_bruto.columns)
    so_b = set(df_b_bruto.columns) - set(df_a_bruto.columns)
    raise SystemExit(f"Headers das OS divergentes. Só em A: {sorted(so_a)} | só em B: {sorted(so_b)}")

log_msg(f"1. Leitura: A {len(df_a_bruto)} linhas | B {len(df_b_bruto)} linhas | {len(df_a_bruto.columns)} colunas idênticas")

long_a = derreter_os(df_a_bruto, "A")
long_b = derreter_os(df_b_bruto, "B")

total_a_entrada = int(df_a_bruto.filter(like="partidas").apply(
    lambda s: pd.to_numeric(s.astype(str).str.replace(",", ".", regex=False), errors="coerce")).sum().sum())
total_b_entrada = int(df_b_bruto.filter(like="partidas").apply(
    lambda s: pd.to_numeric(s.astype(str).str.replace(",", ".", regex=False), errors="coerce")).sum().sum())
if int(long_a["partidas"].sum()) != total_a_entrada or int(long_b["partidas"].sum()) != total_b_entrada:
    raise SystemExit(
        f"Soma do melt diverge da entrada: A {long_a['partidas'].sum():,} vs {total_a_entrada:,} | "
        f"B {long_b['partidas'].sum():,} vs {total_b_entrada:,}"
    )
log_msg(f"2. Integridade do melt: soma A = {total_a_entrada:,} | soma B = {total_b_entrada:,} (batem com os CSVs)")

# Filtro de tipos de dia (se configurado)
tipos_encontrados = sorted(set(long_a["tipo_dia"]) | set(long_b["tipo_dia"]),
                           key=lambda t: ORDEM_TIPO.get(t, 99))
if tipos_dia_cfg:
    faltando = [t for t in tipos_dia_cfg if t not in tipos_encontrados]
    if faltando:
        raise SystemExit(f"tipos_dia configurados ausentes nas OS: {faltando} (encontrados: {tipos_encontrados})")
    long_a = long_a[long_a["tipo_dia"].isin(tipos_dia_cfg)]
    long_b = long_b[long_b["tipo_dia"].isin(tipos_dia_cfg)]
    tipos_usar = list(tipos_dia_cfg)
else:
    tipos_usar = tipos_encontrados
log_msg(f"3. Tipos de dia comparados: {tipos_usar}")

# Cruzamento completo A x B por (servico, sentido, tipo_dia, faixa)
chave = ["servico", "sentido", "tipo_dia", "faixa"]
pares_a = set(zip(long_a["servico"], long_a["sentido"]))
pares_b = set(zip(long_b["servico"], long_b["sentido"]))
so_a = sorted(pares_a - pares_b)
so_b = sorted(pares_b - pares_a)
log_msg(f"4. Pares (Serviço, Sentido): A={len(pares_a)} | B={len(pares_b)} | só A={len(so_a)} | só B={len(so_b)}")

a_meta = long_a.groupby(["servico", "sentido"], as_index=False).agg(
    vista_a=("vista", "first"), consorcio_a=("consorcio", "first"), extensao_a=("extensao", "first"))
b_meta = long_b.groupby(["servico", "sentido"], as_index=False).agg(
    vista_b=("vista", "first"), consorcio_b=("consorcio", "first"), extensao_b=("extensao", "first"))

junto = long_a.merge(long_b, on=chave, how="outer", suffixes=("_a", "_b"))
junto["partidas_a"] = junto["partidas_a"].fillna(0).astype(int)
junto["partidas_b"] = junto["partidas_b"].fillna(0).astype(int)
junto["delta"] = junto["partidas_a"] - junto["partidas_b"]

set_a = set(zip(long_a["servico"], long_a["sentido"]))
set_b = set(zip(long_b["servico"], long_b["sentido"]))
junto["_par"] = list(zip(junto["servico"], junto["sentido"]))
junto["status"] = "IGUAL"
junto.loc[junto["delta"] > 0, "status"] = "A_MAIOR"
junto.loc[junto["delta"] < 0, "status"] = "B_MAIOR"
junto.loc[junto["_par"].isin(set_a - set_b), "status"] = "SÓ_A"
junto.loc[junto["_par"].isin(set_b - set_a), "status"] = "SÓ_B"

junto["_ord_svc"] = junto["servico"]
junto["_ord_sent"] = junto["sentido"].map(ORDEM_SENTIDO).fillna(9)
junto["_ord_tipo"] = junto["tipo_dia"].map(ORDEM_TIPO).fillna(9)
ordem_faixa = {f: i for i, f in enumerate(dict.fromkeys(long_a["faixa"]))}
junto["_ord_faixa"] = junto["faixa"].map(ordem_faixa).fillna(9)
junto = junto.sort_values(["_ord_svc", "_ord_sent", "_ord_tipo", "_ord_faixa"])

celulas = pd.DataFrame({
    "Serviço": junto["servico"],
    "Vista_A": junto["vista_a"].fillna(""),
    "Vista_B": junto["vista_b"].fillna(""),
    "Consórcio_A": junto["consorcio_a"].fillna(""),
    "Consórcio_B": junto["consorcio_b"].fillna(""),
    "Sentido": junto["sentido"],
    "TipoDia": junto["tipo_dia"],
    "Faixa": junto["faixa"],
    "Partidas_A": junto["partidas_a"],
    "Partidas_B": junto["partidas_b"],
    "Delta": junto["delta"],
    "Status": junto["status"],
})

# Relatório por serviço/sentido/tipo de dia
div_por_par = (celulas[celulas["Status"] != "IGUAL"]
               .groupby(["Serviço", "Sentido", "TipoDia"]).size()
               .rename("Celulas_Divergentes").reset_index())
servicos = (celulas.groupby(["Serviço", "Sentido", "TipoDia"], as_index=False)
            .agg(Total_A=("Partidas_A", "sum"), Total_B=("Partidas_B", "sum"), Delta=("Delta", "sum")))
servicos = servicos.merge(
    a_meta.rename(columns={"servico": "Serviço", "sentido": "Sentido",
                           "vista_a": "Vista_A", "consorcio_a": "Consórcio_A", "extensao_a": "Extensão_A"}),
    on=["Serviço", "Sentido"], how="left")
servicos = servicos.merge(
    b_meta.rename(columns={"servico": "Serviço", "sentido": "Sentido",
                           "vista_b": "Vista_B", "consorcio_b": "Consórcio_B", "extensao_b": "Extensão_B"}),
    on=["Serviço", "Sentido"], how="left")
servicos = servicos.merge(div_por_par, on=["Serviço", "Sentido", "TipoDia"], how="left")
servicos["Celulas_Divergentes"] = servicos["Celulas_Divergentes"].fillna(0).astype(int)
servicos["Delta_Extensão"] = servicos["Extensão_A"].fillna(0) - servicos["Extensão_B"].fillna(0)
servicos["Vista_Divergente"] = (servicos["Vista_A"] != servicos["Vista_B"]).map({True: "SIM", False: "NAO"})
servicos["_ord_sent"] = servicos["Sentido"].map(ORDEM_SENTIDO).fillna(9)
servicos["_ord_tipo"] = servicos["TipoDia"].map(ORDEM_TIPO).fillna(9)
servicos = servicos.sort_values(["Serviço", "_ord_sent", "_ord_tipo"])
servicos = servicos[[
    "Serviço", "Sentido", "TipoDia", "Vista_A", "Vista_B", "Consórcio_A", "Consórcio_B",
    "Extensão_A", "Extensão_B", "Delta_Extensão", "Total_A", "Total_B", "Delta",
    "Celulas_Divergentes", "Vista_Divergente",
]]

# Gravação
pasta_saida.mkdir(parents=True, exist_ok=True)
arq_celulas = pasta_saida / f"relatorio_{prefixo}_celulas.csv"
arq_servicos = pasta_saida / f"relatorio_{prefixo}_servicos.csv"
arq_resumo = pasta_saida / f"resumo_{prefixo}.txt"

celulas.to_csv(arq_celulas, index=False, encoding="utf-8-sig")
servicos.to_csv(arq_servicos, index=False, encoding="utf-8-sig")
log_msg(f"5. Relatórios gravados: {arq_celulas.name} ({len(celulas):,} células) | {arq_servicos.name} ({len(servicos):,} linhas)")

# Resumo TXT
linhas_resumo = []
linhas_resumo.append("RESUMO — Comparação de duas OS (partidas por linha, sentido, tipo de dia e faixa)")
linhas_resumo.append(f"Gerado em      : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
linhas_resumo.append(f"A (referência) : {end_os_a.name}")
linhas_resumo.append(f"B (comparação) : {end_os_b.name}")
linhas_resumo.append(f"Delta          : A − B (positivo = mais partidas em A)")
linhas_resumo.append("")
linhas_resumo.append("Totais gerais:")
linhas_resumo.append(f"  A: {total_a_entrada:,} partidas | B: {total_b_entrada:,} partidas | delta {total_a_entrada - total_b_entrada:+,}")
linhas_resumo.append(f"  Pares (Serviço, Sentido): A={len(pares_a)} | B={len(pares_b)} | só A={len(so_a)} | só B={len(so_b)}")
linhas_resumo.append(f"  Células comparadas: {len(celulas):,} | exatas: {int((celulas['Status'] == 'IGUAL').sum()):,} | "
                     f"divergentes: {int((celulas['Status'] != 'IGUAL').sum()):,}")
linhas_resumo.append("")
linhas_resumo.append("Por tipo de dia:")
linhas_resumo.append(f"  {'TipoDia':<20} {'células':>9} {'exatas':>9} {'A_MAIOR':>9} {'B_MAIOR':>9} {'SÓ_A':>7} {'SÓ_B':>7} "
                     f"{'part_A':>9} {'part_B':>9} {'delta':>9}")
for tipo in tipos_usar:
    sub = celulas[celulas["TipoDia"] == tipo]
    linhas_resumo.append(
        f"  {tipo:<20} {len(sub):>9,} {int((sub['Status'] == 'IGUAL').sum()):>9,} "
        f"{int((sub['Status'] == 'A_MAIOR').sum()):>9,} {int((sub['Status'] == 'B_MAIOR').sum()):>9,} "
        f"{int((sub['Status'] == 'SÓ_A').sum()):>7,} {int((sub['Status'] == 'SÓ_B').sum()):>7,} "
        f"{int(sub['Partidas_A'].sum()):>9,} {int(sub['Partidas_B'].sum()):>9,} {int(sub['Delta'].sum()):>+9,}"
    )
linhas_resumo.append("")
linhas_resumo.append(f"Serviços só em A ({len(so_a)} pares):")
for svc, sent in so_a:
    vista = a_meta.set_index(["servico", "sentido"]).loc[(svc, sent), "vista_a"]
    linhas_resumo.append(f"  {svc} {sent} — {vista}")
linhas_resumo.append(f"Serviços só em B ({len(so_b)} pares):")
for svc, sent in so_b:
    vista = b_meta.set_index(["servico", "sentido"]).loc[(svc, sent), "vista_b"]
    linhas_resumo.append(f"  {svc} {sent} — {vista}")
linhas_resumo.append("")
linhas_resumo.append("Top 15 células por |delta|:")
top = celulas.assign(_abs=celulas["Delta"].abs()).sort_values("_abs", ascending=False).head(15)
for _, r in top.iterrows():
    linhas_resumo.append(
        f"  {r['Serviço']} {r['Sentido']} {r['TipoDia']} {r['Faixa']}: "
        f"A={r['Partidas_A']} B={r['Partidas_B']} delta={r['Delta']:+d} ({r['Status']})"
    )
linhas_resumo.append("")
linhas_resumo.append("Integridade:")
linhas_resumo.append(f"  soma do melt A == soma das colunas do CSV A: OK ({total_a_entrada:,})")
linhas_resumo.append(f"  soma do melt B == soma das colunas do CSV B: OK ({total_b_entrada:,})")
n_celulas_esperado = (len(pares_a | pares_b)) * len(set(long_a['faixa'])) * len(tipos_usar)
linhas_resumo.append(f"  células == pares unidos × faixas × tipos: OK ({len(celulas):,} == {n_celulas_esperado:,})")

arq_resumo.write_text("\n".join(linhas_resumo) + "\n", encoding="utf-8-sig")
log_msg(f"6. Resumo: {arq_resumo.name}")

print("\n" + "=" * 60)
print("COMPARAÇÃO CONCLUÍDA")
print(f"  Células : {len(celulas):,} ({int((celulas['Status'] != 'IGUAL').sum()):,} divergentes)")
print(f"  Delta   : {total_a_entrada - total_b_entrada:+,} partidas (A − B)")
print(f"  Saídas  : {arq_celulas.name}, {arq_servicos.name}, {arq_resumo.name}")
print("=" * 60)
