import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path

import pandas as pd


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "codigos_py" / "7_lista_partidas.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("lista_partidas", SCRIPT_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class TestListaPartidasCalendarios(unittest.TestCase):
    def test_resolver_service_ids_com_calendar_txt_e_legado(self):
        mod = _load_module()
        df_trips = pd.DataFrame([
            {"trip_id": "T1", "service_id": "U_REG"},
            {"trip_id": "T2", "service_id": "S_REG"},
            {"trip_id": "T3", "service_id": "D_REG"},
            {"trip_id": "T4", "service_id": "F_REG"},
            {"trip_id": "T5", "service_id": "SEG_SEX"},
            {"trip_id": "T6", "service_id": "SAB_BRT"},
            {"trip_id": "T7", "service_id": "DOM_BRT"},
            {"trip_id": "T8", "service_id": "EXCEP"},
            {"trip_id": "T9", "service_id": "U_DESAT_1"},
            {"trip_id": "T10", "service_id": "PF_EXEC"},
        ])
        df_calendar = pd.DataFrame([
            {"service_id": "U_REG", "monday": "1", "tuesday": "1", "wednesday": "1", "thursday": "1", "friday": "1", "saturday": "0", "sunday": "0"},
            {"service_id": "F_REG", "monday": "1", "tuesday": "1", "wednesday": "1", "thursday": "1", "friday": "1", "saturday": "0", "sunday": "0"},
            {"service_id": "SEG_SEX", "monday": "1", "tuesday": "1", "wednesday": "1", "thursday": "1", "friday": "1", "saturday": "0", "sunday": "0"},
            {"service_id": "SAB_BRT", "monday": "0", "tuesday": "0", "wednesday": "0", "thursday": "0", "friday": "0", "saturday": "1", "sunday": "0"},
            {"service_id": "DOM_BRT", "monday": "0", "tuesday": "0", "wednesday": "0", "thursday": "0", "friday": "0", "saturday": "0", "sunday": "1"},
            {"service_id": "EXCEP", "monday": "1", "tuesday": "0", "wednesday": "0", "thursday": "0", "friday": "0", "saturday": "0", "sunday": "0"},
        ])

        self.assertEqual(mod.resolver_service_ids_por_tipo_dia("du", df_trips, df_calendar), {"U_REG", "SEG_SEX"})
        self.assertEqual(mod.resolver_service_ids_por_tipo_dia("sab", df_trips, df_calendar), {"S_REG", "SAB_BRT"})
        self.assertEqual(mod.resolver_service_ids_por_tipo_dia("dom", df_trips, df_calendar), {"D_REG", "DOM_BRT"})
        self.assertEqual(mod.resolver_service_ids_por_tipo_dia("pf", df_trips, df_calendar), {"F_REG", "PF_EXEC"})
        self.assertEqual(mod.resolver_service_ids_por_tipo_dia("desconhecido", df_trips, df_calendar), set())

    def test_resolver_calendarios_de_verao_separados_dos_regulares(self):
        mod = _load_module()
        df_trips = pd.DataFrame([
            {"trip_id": "T1", "service_id": "U_REG"},
            {"trip_id": "T2", "service_id": "U_VER"},
            {"trip_id": "T3", "service_id": "S_VER"},
            {"trip_id": "T4", "service_id": "D_VER"},
            {"trip_id": "T5", "service_id": "SEG_SEX_VERAO"},
            {"trip_id": "T6", "service_id": "SAB_REG"},
        ])
        df_calendar = pd.DataFrame([
            {"service_id": "U_REG", "monday": "1", "tuesday": "1", "wednesday": "1", "thursday": "1", "friday": "1", "saturday": "0", "sunday": "0"},
            {"service_id": "U_VER", "monday": "1", "tuesday": "1", "wednesday": "1", "thursday": "1", "friday": "1", "saturday": "0", "sunday": "0"},
            {"service_id": "S_VER", "monday": "0", "tuesday": "0", "wednesday": "0", "thursday": "0", "friday": "0", "saturday": "1", "sunday": "0"},
            {"service_id": "D_VER", "monday": "0", "tuesday": "0", "wednesday": "0", "thursday": "0", "friday": "0", "saturday": "0", "sunday": "1"},
            {"service_id": "SEG_SEX_VERAO", "monday": "1", "tuesday": "1", "wednesday": "1", "thursday": "1", "friday": "1", "saturday": "0", "sunday": "0"},
            {"service_id": "SAB_REG", "monday": "0", "tuesday": "0", "wednesday": "0", "thursday": "0", "friday": "0", "saturday": "1", "sunday": "0"},
        ])

        self.assertEqual(mod.resolver_service_ids_por_tipo_dia("du", df_trips, df_calendar), {"U_REG"})
        self.assertEqual(mod.resolver_service_ids_por_tipo_dia("sab", df_trips, df_calendar), {"SAB_REG"})
        self.assertEqual(mod.resolver_service_ids_por_tipo_dia("dom", df_trips, df_calendar), set())

        self.assertEqual(mod.resolver_service_ids_por_tipo_dia("du_ver", df_trips, df_calendar), {"U_VER", "SEG_SEX_VERAO"})
        self.assertEqual(mod.resolver_service_ids_por_tipo_dia("sab_ver", df_trips, df_calendar), {"S_VER"})
        self.assertEqual(mod.resolver_service_ids_por_tipo_dia("dom_ver", df_trips, df_calendar), {"D_VER"})

    def test_cli_processa_novos_calendarios_de_gtfs_unificado(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            resultados = base / "resultados"
            gtfs_zip = base / "gtfs_unificado.zip"

            tables = {
                "agency": pd.DataFrame([{"agency_id": "20001", "agency_name": "MOBI-Rio"}]),
                "routes": pd.DataFrame([
                    {"route_id": "R1", "agency_id": "20001", "route_short_name": "50", "route_long_name": "BRT 50", "route_type": "702"},
                ]),
                "calendar": pd.DataFrame([
                    {"service_id": "SEG_SEX", "monday": "1", "tuesday": "1", "wednesday": "1", "thursday": "1", "friday": "1", "saturday": "0", "sunday": "0"},
                    {"service_id": "SAB_BRT", "monday": "0", "tuesday": "0", "wednesday": "0", "thursday": "0", "friday": "0", "saturday": "1", "sunday": "0"},
                ]),
                "trips": pd.DataFrame([
                    {"route_id": "R1", "service_id": "SEG_SEX", "trip_id": "T_DU", "trip_short_name": "50", "trip_headsign": "Alvorada", "direction_id": "0", "shape_id": "S1"},
                    {"route_id": "R1", "service_id": "SAB_BRT", "trip_id": "T_SAB", "trip_short_name": "50", "trip_headsign": "Alvorada", "direction_id": "0", "shape_id": "S1"},
                ]),
                "stop_times": pd.DataFrame([
                    {"trip_id": "T_DU", "arrival_time": "06:00:00", "departure_time": "06:00:00", "stop_id": "P1", "stop_sequence": "1"},
                    {"trip_id": "T_SAB", "arrival_time": "08:00:00", "departure_time": "08:00:00", "stop_id": "P1", "stop_sequence": "1"},
                ]),
            }

            with zipfile.ZipFile(gtfs_zip, "w") as zf:
                for k, df in tables.items():
                    zf.writestr(f"{k}.txt", df.to_csv(index=False))

            cfg = base / "config.json"
            cfg.write_text(
                json.dumps({
                    "BASE_DADOS": str(base),
                    "BASE_RESULTADOS": str(resultados),
                    "ano_gtfs": "2028",
                    "mes_gtfs": "11",
                    "estudo_gtfs": "05",
                    "endereco_gtfs": str(gtfs_zip),
                    "tipos_dia": ["du", "sab"],
                }),
                encoding="utf-8",
            )

            res = subprocess.run(
                [sys.executable, str(SCRIPT_PATH), "--config", str(cfg)],
                capture_output=True,
                text=True,
            )
            self.assertEqual(res.returncode, 0, msg=res.stderr or res.stdout)

            du_csv = pd.read_csv(resultados / "partidas" / "partidas_du.csv", dtype=str)
            sab_csv = pd.read_csv(resultados / "partidas" / "partidas_sab.csv", dtype=str)

            self.assertEqual(du_csv["departure_time"].tolist(), ["06:00:00"])
            self.assertEqual(sab_csv["departure_time"].tolist(), ["08:00:00"])


if __name__ == "__main__":
    unittest.main()
