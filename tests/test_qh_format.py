import io
import sys
import unittest
import zipfile
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.qh_format import (  # noqa: E402
    bands_from_departures,
    comparison_table,
    get_departure_seconds,
    horario_to_seconds,
    load_gtfs_zip,
    parse_partidas,
    qh_dataframe,
    route_choices,
    schedule_display,
    seconds_to_horario,
    summary_table,
    unwrap_midnight,
)


def _gtfs_zip() -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        z.writestr(
            "routes.txt",
            "route_id,route_short_name,route_long_name\n"
            "R1,371,Linha 371\n"
            "R2,624,Linha 624\n",
        )
        z.writestr(
            "trips.txt",
            "route_id,service_id,trip_id,trip_headsign,direction_id\n"
            "R1,U_REG,T1,Destino A,0\n"
            "R1,U_REG,T2,Destino A,0\n"
            "R2,U_REG,T3,Destino B,1\n",
        )
        z.writestr(
            "frequencies.txt",
            "trip_id,start_time,end_time,headway_secs\n"
            "T1,06:00:00,07:00:00,600\n"
            "T2,06:00:00,07:00:00,600\n",
        )
    return buffer.getvalue()


class TestTempo(unittest.TestCase):
    def test_horario_to_seconds(self):
        self.assertEqual(horario_to_seconds("06:00:00"), 21600)
        self.assertEqual(horario_to_seconds("29:59:59"), 107999)
        self.assertEqual(horario_to_seconds("6:00"), 21600)
        self.assertEqual(horario_to_seconds("invalido"), -1)
        self.assertEqual(horario_to_seconds("06:60:00"), -1)
        self.assertEqual(horario_to_seconds(None), -1)

    def test_seconds_to_horario(self):
        self.assertEqual(seconds_to_horario(21600), "06:00:00")
        self.assertEqual(seconds_to_horario(90000), "25:00:00")
        self.assertEqual(seconds_to_horario(0), "00:00:00")

    def test_unwrap_midnight(self):
        self.assertEqual(unwrap_midnight([82800, 3600]), [82800, 90000])
        self.assertEqual(
            unwrap_midnight([82800, 3600, 82800, 3600]),
            [82800, 90000, 169200, 176400],
        )

    def test_parse_partidas(self):
        texto = "\n 06:00:00 \n\n06:10:00\n"
        self.assertEqual(parse_partidas(texto), [21600, 22200])
        self.assertEqual(
            parse_partidas("23:50:00\n00:10:00"),
            [85800, 87000],
        )

    def test_parse_partidas_invalidas(self):
        with self.assertRaises(ValueError):
            parse_partidas("")
        with self.assertRaises(ValueError):
            parse_partidas("06:00\n06:10:00")
        with self.assertRaises(ValueError):
            parse_partidas("30:00:00")


class TestGtfs(unittest.TestCase):
    def test_load_gtfs_zip(self):
        gtfs = load_gtfs_zip(_gtfs_zip())
        self.assertEqual(set(gtfs), {"routes", "trips", "frequencies"})
        self.assertEqual(gtfs["routes"].shape[0], 2)
        self.assertEqual(
            route_choices(gtfs["routes"]),
            ["371 - R1", "624 - R2"],
        )

    def test_expand_frequencies(self):
        gtfs = load_gtfs_zip(_gtfs_zip())
        trips = gtfs["trips"][gtfs["trips"]["trip_id"] == "T1"]
        self.assertEqual(
            get_departure_seconds(gtfs, trips),
            [21600, 22200, 22800, 23400, 24000, 24600],
        )

    def test_expand_frequencies_sem_tabela(self):
        gtfs = load_gtfs_zip(_gtfs_zip())
        gtfs["frequencies"] = pd.DataFrame()
        trips = gtfs["trips"][gtfs["trips"]["trip_id"] == "T1"]
        self.assertEqual(get_departure_seconds(gtfs, trips), [])


class TestBandas(unittest.TestCase):
    def test_bandas_headways_consecutivos(self):
        bandas = bands_from_departures([0, 600, 1200, 2400])
        self.assertEqual(
            bandas[["start_seconds", "end_seconds", "headway_secs"]].values.tolist(),
            [[0, 1200, 600], [1200, 3600, 1200]],
        )
        self.assertEqual(bandas["n_partidas"].tolist(), [2, 2])

    def test_bandas_tres_grupos(self):
        bandas = bands_from_departures([0, 600, 1800, 2400])
        self.assertEqual(
            bandas[["start_seconds", "end_seconds", "headway_secs"]].values.tolist(),
            [[0, 600, 600], [600, 1800, 1200], [1800, 3000, 600]],
        )

    def test_bandas_uma_partida(self):
        with self.assertRaises(ValueError):
            bands_from_departures([21600])

    def test_qh_dataframe(self):
        bandas = bands_from_departures([0, 600, 1200])
        qh = qh_dataframe(bandas, "371", "Destino A")
        self.assertEqual(
            list(qh.columns),
            ["trip_id", "trip_headsign", "trip_short_name", "start_time", "end_time", "headway_secs"],
        )
        self.assertEqual(qh.loc[0, "trip_id"], "")
        self.assertEqual(qh.loc[0, "start_time"], "00:00:00")
        self.assertEqual(qh.loc[0, "end_time"], "00:30:00")
        self.assertEqual(int(qh.loc[0, "headway_secs"]), 600)


class TestComparacaoResumo(unittest.TestCase):
    def test_schedule_display(self):
        tabela = schedule_display([0, 600, 1200])
        self.assertEqual(tabela["Partida"].tolist(), [1, 2, 3])
        self.assertEqual(tabela["Horário"].tolist(), ["00:00:00", "00:10:00", "00:20:00"])
        self.assertEqual(tabela["Intervalo"].tolist(), ["-", 10.0, 10.0])

    def test_comparacao_igual(self):
        tabela = comparison_table([0, 600, 1200], [0, 600, 1200])
        self.assertEqual(tabela["Comparação"].tolist(), ["-", "=", "="])
        self.assertEqual(tabela["Horário GTFS"].tolist(), ["00:00:00", "00:10:00", "00:20:00"])

    def test_comparacao_setas(self):
        tabela = comparison_table([0, 600, 1200], [0, 900, 1800])
        self.assertEqual(tabela["Comparação"].tolist(), ["-", "↑", "↑"])

    def test_comparacao_usuario_sem_par(self):
        tabela = comparison_table([0, 600, 1200], [0, 600])
        self.assertEqual(tabela["Horário GTFS"].tolist(), ["00:00:00", "00:10:00", "00:20:00"])
        self.assertEqual(tabela["Horário Usuário"].tolist(), ["00:00:00", "00:10:00", "-"])
        self.assertEqual(tabela["Comparação"].tolist(), ["-", "=", "-"])

    def test_resumo(self):
        resumo = summary_table([0, 3600, 7200], [0, 86400])
        self.assertEqual(int(resumo.loc[0, "Partidas GTFS"]), 1)
        self.assertEqual(int(resumo.loc[0, "Partidas Propostas"]), 1)
        linha_24 = resumo[resumo["Faixa"] == "24:00-24:59"]
        self.assertEqual(int(linha_24["Partidas Propostas"].iloc[0]), 1)
        self.assertEqual(int(resumo["Partidas GTFS"].iloc[-1]), 3)
        self.assertEqual(int(resumo["Partidas Propostas"].iloc[-1]), 2)


if __name__ == "__main__":
    unittest.main()
