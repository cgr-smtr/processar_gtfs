"""Lógica de formatação e comparação de quadros horários.

Este módulo é uma versão Python do app Shiny ``1.2 app_format_qh.R``.
As funções aqui não dependem de Streamlit e podem ser testadas isoladamente.
"""

from __future__ import annotations

import io
import re
import zipfile
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

# Faixas do resumo (mesmas do R: 00:00-00:59 ... 29:00-29:59).
FAIXAS_RESUMO = [
    "00:00-00:59", "01:00-01:59", "02:00-02:59", "03:00-03:59",
    "04:00-04:59", "05:00-05:59", "06:00-08:59", "09:00-11:59",
    "12:00-14:59", "15:00-17:59", "18:00-20:59", "21:00-21:59",
    "22:00-22:59", "23:00-23:59", "24:00-24:59", "25:00-25:59",
    "26:00-26:59", "27:00-27:59", "28:00-28:59", "29:00-29:59",
]
BREAKS_RESUMO = [0, 1, 2, 3, 4, 5, 6, 9, 12, 15, 18, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30]
HORAS_RESUMO = [0, 1, 2, 3, 4, 5, 6, 9, 12, 15, 18, 21, 22, 23, 24, 25, 26, 27, 28, 29]

REGEX_HORARIO = re.compile(r"^(?:[0-2]?[0-9]):[0-5][0-9]:[0-5][0-9]$")

COLUNAS_QH = [
    "trip_id",
    "trip_headsign",
    "trip_short_name",
    "start_time",
    "end_time",
    "headway_secs",
]


def horario_to_seconds(valor) -> int:
    """Converte ``HH:MM:SS``/``HH:MM`` em segundos; retorna -1 se inválido."""
    if valor is None or (not isinstance(valor, str) and pd.isna(valor)):
        return -1
    texto = str(valor).strip()
    if not texto:
        return -1
    partes = texto.split(":")
    if len(partes) < 2:
        return -1
    try:
        hora = int(partes[0])
        minuto = int(partes[1])
        segundo = int(partes[2]) if len(partes) >= 3 else 0
    except (TypeError, ValueError):
        return -1
    if hora < 0 or not (0 <= minuto <= 59) or not (0 <= segundo <= 59):
        return -1
    return hora * 3600 + minuto * 60 + segundo


def seconds_to_horario(total_seconds) -> str:
    """Converte segundos em ``HH:MM:SS``, preservando horas >= 24."""
    if total_seconds is None or pd.isna(total_seconds):
        return ""
    total = int(total_seconds)
    if total < 0:
        return ""
    hora = total // 3600
    minuto = (total % 3600) // 60
    segundo = total % 60
    return f"{hora:02d}:{minuto:02d}:{segundo:02d}"


def unwrap_midnight(seconds: Iterable[int]) -> list[int]:
    """Torna a sequência não decrescente, somando 24 h quando necessário.

    Repete o ajuste por elemento (equivalente ao loop do R) para suportar
    mais de uma virada de meia-noite.
    """
    valores = [int(s) for s in seconds]
    for i in range(1, len(valores)):
        while valores[i] < valores[i - 1]:
            valores[i] += 86400
    return valores


def parse_partidas(texto: str) -> list[int]:
    """Valida e converte a lista de horários colada pelo usuário."""
    linhas = [linha.strip() for linha in str(texto or "").splitlines()]
    linhas = [linha for linha in linhas if linha]
    if not linhas:
        raise ValueError("Nenhum horário informado.")

    invalidos = [linha for linha in linhas if not REGEX_HORARIO.match(linha)]
    if invalidos:
        raise ValueError(
            "Horário(s) em formato incorreto. Use HH:MM:SS. "
            f"Exemplos inválidos: {', '.join(invalidos[:5])}"
        )

    segundos = [horario_to_seconds(linha) for linha in linhas]
    return unwrap_midnight(segundos)


def _ler_csv_zip(z: zipfile.ZipFile, nome: str) -> pd.DataFrame | None:
    if nome not in z.namelist():
        return None
    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            with z.open(nome) as arquivo:
                return pd.read_csv(arquivo, dtype=str, encoding=encoding)
        except UnicodeDecodeError:
            continue
    raise ValueError(f"Não foi possível ler {nome} do GTFS.")


def load_gtfs_zip(source) -> dict[str, pd.DataFrame]:
    """Lê routes/trips/frequencies de um ZIP GTFS (bytes, caminho ou file-like)."""
    if isinstance(source, (bytes, bytearray)):
        arquivo = io.BytesIO(source)
        fechar = True
    elif hasattr(source, "read"):
        arquivo = source
        fechar = False
    else:
        arquivo = Path(source)
        fechar = True

    with zipfile.ZipFile(arquivo) as z:
        routes = _ler_csv_zip(z, "routes.txt")
        trips = _ler_csv_zip(z, "trips.txt")
        frequencies = _ler_csv_zip(z, "frequencies.txt")

    if fechar and hasattr(arquivo, "close"):
        arquivo.close()

    if routes is None or trips is None:
        raise ValueError("O GTFS precisa conter routes.txt e trips.txt.")

    return {
        "routes": routes,
        "trips": trips,
        "frequencies": frequencies if frequencies is not None else pd.DataFrame(),
    }


def route_choices(routes: pd.DataFrame) -> list[str]:
    """Lista de opções ``short_name - route_id`` ordenada pelo nome da linha."""
    if routes is None or routes.empty or "route_id" not in routes.columns:
        return []

    df = routes.copy()
    if "route_short_name" not in df.columns:
        df["route_short_name"] = ""
    nomes = df["route_short_name"].fillna("").astype(str).str.strip()
    ids = df["route_id"].fillna("").astype(str).str.strip()
    df["_label"] = nomes + " - " + ids
    df["_ordem"] = nomes.str.lower()
    df = df[ids != ""].sort_values(["_ordem", "route_id"], kind="mergesort")
    return list(dict.fromkeys(df["_label"].tolist()))


def route_id_from_label(label: str) -> str:
    """Extrai o ``route_id`` da opção ``short_name - route_id``."""
    texto = str(label)
    if " - " in texto:
        return texto.split(" - ", 1)[1]
    return texto


def trip_short_name_from_label(label: str) -> str:
    """Extrai o ``trip_short_name`` (parte antes de `` - ``) da opção."""
    return re.sub(r"\s*-\s*.*$", "", str(label))


def unique_nonempty(valores: Iterable) -> list[str]:
    """Valores únicos não vazios, preservando a ordem de aparição."""
    vistos: set[str] = set()
    resultado: list[str] = []
    for valor in valores:
        if valor is None or (not isinstance(valor, str) and pd.isna(valor)):
            continue
        texto = str(valor).strip()
        if not texto or texto in vistos:
            continue
        vistos.add(texto)
        resultado.append(texto)
    return resultado


def filter_trips(
    trips: pd.DataFrame,
    route_id: str,
    direction_id: str | None = None,
    service_id: str | None = None,
    trip_headsign: str | None = None,
) -> pd.DataFrame:
    """Filtra viagens pelos campos do formulário."""
    if trips is None or trips.empty:
        return pd.DataFrame(columns=trips.columns if trips is not None else [])

    mask = trips["route_id"].astype(str) == str(route_id)
    if direction_id is not None and "direction_id" in trips.columns:
        mask &= trips["direction_id"].astype(str) == str(direction_id)
    if service_id is not None and "service_id" in trips.columns:
        mask &= trips["service_id"].astype(str) == str(service_id)
    if trip_headsign is not None and "trip_headsign" in trips.columns:
        mask &= trips["trip_headsign"].astype(str) == str(trip_headsign)
    return trips.loc[mask].copy()


def get_departure_seconds(gtfs: dict[str, pd.DataFrame], trips: pd.DataFrame) -> list[int]:
    """Expande ``frequencies`` das viagens selecionadas em partidas (segundos)."""
    frequencies = gtfs.get("frequencies")
    if frequencies is None or frequencies.empty or trips is None or trips.empty:
        return []
    if "trip_id" not in frequencies.columns or "trip_id" not in trips.columns:
        return []

    trip_ids = set(trips["trip_id"].dropna().astype(str))
    freq = frequencies[frequencies["trip_id"].astype(str).isin(trip_ids)]
    if freq.empty:
        return []

    partidas: set[int] = set()
    for _, linha in freq.iterrows():
        inicio = horario_to_seconds(linha.get("start_time"))
        fim = horario_to_seconds(linha.get("end_time"))
        try:
            headway = int(float(linha.get("headway_secs")))
        except (TypeError, ValueError):
            continue
        if inicio < 0 or fim < 0 or headway <= 0 or inicio >= fim:
            continue
        ultima = fim - headway
        if ultima < inicio:
            continue
        for segundo in range(inicio, ultima + 1, headway):
            partidas.add(segundo)

    return sorted(partidas)


def schedule_rows(seconds: Iterable[int]) -> pd.DataFrame:
    """Tabela ordenada de partidas com intervalo em minutos."""
    valores = sorted({int(s) for s in seconds})
    df = pd.DataFrame({"departure_seconds": valores})
    if df.empty:
        df["departure_time"] = pd.Series(dtype=str)
        df["hora"] = pd.Series(dtype=int)
        df["intervalo_min"] = pd.Series(dtype=float)
        return df

    df["departure_time"] = df["departure_seconds"].map(seconds_to_horario)
    df["hora"] = df["departure_seconds"] // 3600
    df["intervalo_min"] = df["departure_seconds"].diff() / 60.0
    return df


def schedule_display(seconds: Iterable[int], casas_decimais: int = 2) -> pd.DataFrame:
    """DataFrame de exibição: Partida | Horário | Intervalo (+ hora interna)."""
    df = schedule_rows(seconds)
    if df.empty:
        return pd.DataFrame(columns=["Partida", "Horário", "Intervalo", "_hora"])

    intervalos = [
        "-" if pd.isna(valor) else round(float(valor), casas_decimais)
        for valor in df["intervalo_min"]
    ]
    return pd.DataFrame({
        "Partida": range(1, len(df) + 1),
        "Horário": df["departure_time"].tolist(),
        "Intervalo": intervalos,
        "_hora": df["hora"].astype(int).tolist(),
    })


def bands_from_departures(seconds: Iterable[int]) -> pd.DataFrame:
    """Agrupa partidas consecutivas de mesmo headway em bandas (formato QH).

    O último headway é herdado da penúltima partida, como no R. A banda
    resultante segue o formato ``start_time, end_time, headway_secs`` usado
    pelo ``frequencies.txt``.
    """
    valores = sorted({int(s) for s in seconds})
    if len(valores) < 2:
        raise ValueError("São necessárias pelo menos 2 partidas para gerar o QH.")

    headways = [valores[i + 1] - valores[i] for i in range(len(valores) - 1)]
    headways.append(headways[-1])

    bandas: list[dict] = []
    inicio_banda = valores[0]
    headway_banda = headways[0]
    ultima_partida = valores[0]

    for i in range(1, len(valores)):
        if headways[i] == headway_banda:
            ultima_partida = valores[i]
        else:
            bandas.append({
                "start_seconds": inicio_banda,
                "end_seconds": ultima_partida + headway_banda,
                "headway_secs": headway_banda,
            })
            inicio_banda = valores[i]
            headway_banda = headways[i]
            ultima_partida = valores[i]

    bandas.append({
        "start_seconds": inicio_banda,
        "end_seconds": ultima_partida + headway_banda,
        "headway_secs": headway_banda,
    })

    df = pd.DataFrame(bandas)
    df["start_time"] = df["start_seconds"].map(seconds_to_horario)
    df["end_time"] = df["end_seconds"].map(seconds_to_horario)
    df["n_partidas"] = ((df["end_seconds"] - df["start_seconds"]) // df["headway_secs"]).astype(int)
    return df


def qh_dataframe(
    bands: pd.DataFrame,
    trip_short_name: str,
    trip_headsign: str,
) -> pd.DataFrame:
    """Monta o CSV de QH no formato do Script 1 (``trip_id`` vazio)."""
    if bands is None or bands.empty:
        return pd.DataFrame(columns=COLUNAS_QH)

    df = bands.sort_values("start_seconds").copy()
    return pd.DataFrame({
        "trip_id": "",
        "trip_headsign": trip_headsign,
        "trip_short_name": trip_short_name,
        "start_time": df["start_time"].tolist(),
        "end_time": df["end_time"].tolist(),
        "headway_secs": df["headway_secs"].astype(int).tolist(),
    })[COLUNAS_QH]


def round_to_minute(seconds) -> int:
    """Arredonda segundos para o minuto mais próximo (half-even, como o R)."""
    return int(np.rint(np.asarray(seconds, dtype=float) / 60.0) * 60)


def _nearest_time(valor: float, candidatos: np.ndarray) -> float | None:
    if len(candidatos) == 0:
        return None
    indice = int(np.argmin(np.abs(candidatos - valor)))
    return float(candidatos[indice])


def comparison_table(gtfs_seconds: Iterable[int], user_seconds: Iterable[int]) -> pd.DataFrame:
    """Tabela de comparação GTFS x proposta (mesma lógica do R)."""
    colunas = [
        "Horário GTFS", "Horário Usuário",
        "Intervalo GTFS", "Intervalo Usuário",
        "Comparação", "_hora_gtfs", "_hora_user",
    ]
    gtfs = sorted({round_to_minute(s) for s in gtfs_seconds})
    user = [round_to_minute(s) for s in user_seconds]
    if not user:
        return pd.DataFrame(columns=colunas)

    gtfs_df = pd.DataFrame({"time_gtfs": gtfs})
    gtfs_df["interval_gtfs"] = gtfs_df["time_gtfs"].diff() / 60.0

    user_df = pd.DataFrame({"time_user": user})
    user_df["interval_user"] = user_df["time_user"].diff() / 60.0
    if gtfs:
        candidatos = np.asarray(gtfs, dtype=float)
        user_df["nearest_gtfs_time"] = [_nearest_time(u, candidatos) for u in user]
    else:
        user_df["nearest_gtfs_time"] = np.nan

    combined = gtfs_df.merge(
        user_df,
        left_on="time_gtfs",
        right_on="nearest_gtfs_time",
        how="left",
    )
    combined = combined.drop(columns=["nearest_gtfs_time"], errors="ignore")

    # Usuário sem par GTFS entra como linha própria (time_gtfs = time_user).
    tempos_usuario = set(combined["time_user"].dropna().astype(float).tolist())
    sem_par = user_df[~user_df["time_user"].astype(float).isin(tempos_usuario)].copy()
    if not sem_par.empty:
        sem_par["time_gtfs"] = sem_par["time_user"]
        sem_par["interval_gtfs"] = np.nan
        sem_par = sem_par.drop(columns=["nearest_gtfs_time"], errors="ignore")
        combined = pd.concat([combined, sem_par], ignore_index=True, sort=False)

    combined["_diff"] = (combined["time_gtfs"] - combined["time_user"]).abs()

    if len(gtfs_df) <= len(user_df):
        min_diff = combined.groupby("time_gtfs")["_diff"].transform("min")
        duplicado = combined.groupby("time_gtfs")["_diff"].transform(
            lambda serie: serie.duplicated(keep="first")
        ).fillna(False).astype(bool)
        manter = (combined["_diff"] == min_diff) & (~duplicado)
        combined.loc[(~manter) & combined["_diff"].notna(), "time_gtfs"] = np.nan
    else:
        min_diff_user = combined.groupby("time_user")["_diff"].transform("min")
        duplicado_user = combined.groupby("time_user")["_diff"].transform(
            lambda serie: serie.duplicated(keep="first")
        ).fillna(False).astype(bool)
        manter_user = (combined["_diff"] == min_diff_user) & (~duplicado_user)
        combined.loc[(~manter_user) & combined["_diff"].notna(), "time_user"] = np.nan

        min_diff_gtfs = combined.groupby("time_gtfs")["_diff"].transform("min")
        duplicado_gtfs = combined.groupby("time_gtfs")["_diff"].transform(
            lambda serie: serie.duplicated(keep="first")
        ).fillna(False).astype(bool)
        manter_gtfs = (combined["_diff"] == min_diff_gtfs) & (~duplicado_gtfs)
        combined.loc[(~manter_gtfs) & combined["_diff"].notna(), "time_gtfs"] = np.nan

    combined = combined.sort_values("time_gtfs", na_position="last", kind="mergesort")

    def _fmt(valor) -> str:
        return "-" if pd.isna(valor) else seconds_to_horario(int(valor))

    resultado = pd.DataFrame({
        "Horário GTFS": combined["time_gtfs"].map(_fmt),
        "Horário Usuário": combined["time_user"].map(_fmt),
        "Intervalo GTFS": [
            "-" if pd.isna(iv) or pd.isna(tv) else round(float(iv), 1)
            for iv, tv in zip(combined["interval_gtfs"], combined["time_gtfs"])
        ],
        "Intervalo Usuário": [
            "-" if pd.isna(iv) else round(float(iv), 1)
            for iv in combined["interval_user"]
        ],
        "Comparação": [
            "-" if (
                pd.isna(iv) or pd.isna(tv) or pd.isna(iu) or pd.isna(tu)
            ) else (
                "↑" if iu > iv else ("↓" if iu < iv else "=")
            )
            for iv, tv, iu, tu in zip(
                combined["interval_gtfs"],
                combined["time_gtfs"],
                combined["interval_user"],
                combined["time_user"],
            )
        ],
    })
    resultado["_hora_gtfs"] = combined["time_gtfs"].map(
        lambda valor: None if pd.isna(valor) else int(valor // 3600)
    )
    resultado["_hora_user"] = combined["time_user"].map(
        lambda valor: None if pd.isna(valor) else int(valor // 3600)
    )
    return resultado


def summary_table(gtfs_seconds: Iterable[int], user_seconds: Iterable[int]) -> pd.DataFrame:
    """Contagem de partidas por faixa horária (14 faixas + 24h-29h)."""
    def _contar(seconds: Iterable[int]) -> np.ndarray:
        valores = list(seconds)
        if not valores:
            return np.zeros(len(FAIXAS_RESUMO), dtype=int)
        horas = np.floor(np.asarray(valores, dtype=float) / 3600.0)
        categorias = pd.cut(
            horas,
            bins=BREAKS_RESUMO,
            labels=FAIXAS_RESUMO,
            right=False,
        )
        contagens = pd.Series(categorias).value_counts().reindex(FAIXAS_RESUMO, fill_value=0)
        return contagens.to_numpy(dtype=int)

    contagem_gtfs = _contar(gtfs_seconds)
    contagem_user = _contar(user_seconds)

    df = pd.DataFrame({
        "Faixa": FAIXAS_RESUMO,
        "Partidas GTFS": contagem_gtfs,
        "Partidas Propostas": contagem_user,
        "_hora": HORAS_RESUMO,
    })
    total = pd.DataFrame([{
        "Faixa": "Total",
        "Partidas GTFS": int(contagem_gtfs.sum()),
        "Partidas Propostas": int(contagem_user.sum()),
        "_hora": None,
    }])
    return pd.concat([df, total], ignore_index=True)


def get_time_color(hora) -> str:
    """Cor de fundo da faixa horária (mesma paleta do R)."""
    if hora is None or pd.isna(hora):
        return "#FFFFFF"
    h = int(hora)
    if h == 0:
        return "#B3E5FC"
    if h == 1:
        return "#80DEEA"
    if h == 2:
        return "#80CBC4"
    if h == 3:
        return "#A5D6A7"
    if h == 4:
        return "#C8E6C9"
    if h == 5:
        return "#E6EE9C"
    if 6 <= h < 9:
        return "#FFF59D"
    if 9 <= h < 12:
        return "#FFE082"
    if 12 <= h < 15:
        return "#FFCC80"
    if 15 <= h < 18:
        return "#FFAB91"
    if 18 <= h < 21:
        return "#F48FB1"
    if h == 21:
        return "#CE93D8"
    if h == 22:
        return "#B39DDB"
    if h == 23:
        return "#9FA8DA"
    if h == 24:
        return "#E0E0E0"
    if h == 25:
        return "#CFD8DC"
    if h == 26:
        return "#D7CCC8"
    if h == 27:
        return "#DCEDC8"
    if h == 28:
        return "#FFF9C4"
    if h == 29:
        return "#FFECB3"
    return "#FFFFFF"


def sanitize_filename(nome: str) -> str:
    """Remove caracteres inválidos para nomes de arquivo no Windows."""
    texto = re.sub(r'[\\/*?:"<>|]', "_", str(nome or ""))
    texto = re.sub(r"\s+", " ", texto).strip(". ")
    return texto or "SEM_NOME"
