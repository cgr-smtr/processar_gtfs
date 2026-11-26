import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path

import pandas as pd


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "codigos_py" / "5.5_juntar_gtfs_unificado.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("juntar_gtfs_unificado", SCRIPT_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _read_zip_tables(zip_path: Path) -> dict[str, pd.DataFrame]:
    tables = {}
    with zipfile.ZipFile(zip_path, "r") as zf:
        for name in zf.namelist():
            if name.endswith(".txt"):
                with zf.open(name) as f:
                    tables[name[:-4]] = pd.read_csv(f, dtype=str)
    return tables


class TestJuntarGtfsUnificado(unittest.TestCase):
    def test_classificar_route_types_preserva_brt_e_executivo(self):
        mod = _load_module()
        df_routes = pd.DataFrame([
            {"route_id": "R_BRT", "route_short_name": "50", "route_type": "702"},
            {"route_id": "R_EXEC_NUM", "route_short_name": "2335", "route_type": "3"},
            {"route_id": "R_EXEC_LECD", "route_short_name": "LECD124", "route_type": ""},
            {"route_id": "R_EXEC_TAG", "route_short_name": "EXEC1", "route_type": "200"},
            {"route_id": "R_SPPO", "route_short_name": "474", "route_type": "3"},
            {"route_id": "R_RIO", "route_short_name": "309", "route_type": "700"},
        ])

        out = mod.classificar_route_types(df_routes).set_index("route_id")

        self.assertEqual(out.loc["R_BRT", "route_type"], "702")
        self.assertEqual(out.loc["R_EXEC_NUM", "route_type"], "200")
        self.assertEqual(out.loc["R_EXEC_LECD", "route_type"], "200")
        self.assertEqual(out.loc["R_EXEC_TAG", "route_type"], "200")
        self.assertEqual(out.loc["R_SPPO", "route_type"], "700")
        self.assertEqual(out.loc["R_RIO", "route_type"], "700")

    def test_cli_nao_substitui_arquivos_em_disco_e_preserva_modais(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            insumos = base / "insumos"
            insumos.mkdir(parents=True)

            # Arquivo de cores obrigatório
            pd.DataFrame([
                {"route_short_name": "474", "route_color": "AA1122", "route_text_color": "FFFFFF"},
                {"route_short_name": "50", "route_color": "999999", "route_text_color": "000000"},
            ]).to_csv(insumos / "gtfs_cores.csv", index=False)

            # Mesmo se existirem pastas de substituição em disco, o 5.5 NÃO deve usá-las
            (insumos / "gtfs_combi").mkdir(parents=True)
            (insumos / "gtfs_pub").mkdir(parents=True)
            pd.DataFrame([{"feed_publisher_name": "DISCO_COMBI"}]).to_csv(
                insumos / "gtfs_combi" / "feed_info.txt", index=False
            )
            pd.DataFrame([{"feed_publisher_name": "DISCO_PUB"}]).to_csv(
                insumos / "gtfs_pub" / "feed_info.txt", index=False
            )

            # GTFS _PROC.zip unificado (SPPO + RIO + BRT + Executivo + EXCEP)
            input_zip = base / "rio_2028-11-05Q_PROC.zip"
            combi_zip = base / "gtfs_combi_2028-11-05Q.zip"
            pub_zip = base / "gtfs_rio-de-janeiro_pub.zip"

            tables_in = {
                "agency": pd.DataFrame([
                    {"agency_id": "20001", "agency_name": "MOBI-Rio"},
                    {"agency_id": "22002", "agency_name": "Intersul"},
                ]),
                "routes": pd.DataFrame([
                    {"route_id": "R_BRT", "agency_id": "20001", "route_short_name": "50", "route_type": "702", "route_color": "00AA55", "route_text_color": "FFFFFF"},
                    {"route_id": "R_SPPO", "agency_id": "22002", "route_short_name": "474", "route_type": "3", "route_color": "111111", "route_text_color": "FFFFFF"},
                    {"route_id": "R_EXEC", "agency_id": "22002", "route_short_name": "2335", "route_type": "200", "route_color": "111111", "route_text_color": "FFFFFF"},
                ]),
                "calendar": pd.DataFrame([
                    {"service_id": "U", "monday": "1"},
                    {"service_id": "EXCEP", "monday": "0"},
                ]),
                "calendar_dates": pd.DataFrame([
                    {"service_id": "U", "date": "20281101", "exception_type": "1"},
                ]),
                "trips": pd.DataFrame([
                    {"route_id": "R_BRT", "service_id": "U", "trip_id": "T_BRT", "trip_short_name": "50", "trip_headsign": "Alvorada", "shape_id": "S_BRT"},
                    {"route_id": "R_SPPO", "service_id": "U", "trip_id": "T_SPPO", "trip_short_name": "474", "trip_headsign": "Copacabana", "shape_id": "S_SPPO"},
                    {"route_id": "R_EXEC", "service_id": "U", "trip_id": "T_EXEC", "trip_short_name": "2335", "trip_headsign": "Castelo", "shape_id": "S_EXEC"},
                    {"route_id": "R_SPPO", "service_id": "EXCEP", "trip_id": "T_EXCEP", "trip_short_name": "474", "trip_headsign": "[DESVIO] Copacabana", "shape_id": "S_SPPO"},
                ]),
                "stop_times": pd.DataFrame([
                    {"trip_id": "T_BRT", "arrival_time": "06:00:00", "departure_time": "06:00:00", "stop_id": "P1", "stop_sequence": "0", "shape_dist_traveled": "0"},
                    {"trip_id": "T_SPPO", "arrival_time": "06:10:00", "departure_time": "06:10:00", "stop_id": "P2", "stop_sequence": "0", "shape_dist_traveled": "0"},
                    {"trip_id": "T_EXEC", "arrival_time": "06:20:00", "departure_time": "06:20:00", "stop_id": "P3", "stop_sequence": "0", "shape_dist_traveled": "0"},
                    {"trip_id": "T_EXCEP", "arrival_time": "07:00:00", "departure_time": "07:00:00", "stop_id": "P2", "stop_sequence": "0", "shape_dist_traveled": "0"},
                ]),
                "stops": pd.DataFrame([
                    {"stop_id": "P1", "stop_name": "Terminal Alvorada"},
                    {"stop_id": "P2", "stop_name": "Ponto 2"},
                    {"stop_id": "P3", "stop_name": "Ponto 3"},
                ]),
                "shapes": pd.DataFrame([
                    {"shape_id": "S_BRT", "shape_pt_lat": "-22.9", "shape_pt_lon": "-43.3", "shape_pt_sequence": "1"},
                    {"shape_id": "S_BRT", "shape_pt_lat": "-22.91", "shape_pt_lon": "-43.31", "shape_pt_sequence": "2"},
                    {"shape_id": "S_SPPO", "shape_pt_lat": "-22.9", "shape_pt_lon": "-43.2", "shape_pt_sequence": "1"},
                    {"shape_id": "S_SPPO", "shape_pt_lat": "-22.91", "shape_pt_lon": "-43.21", "shape_pt_sequence": "2"},
                    {"shape_id": "S_EXEC", "shape_pt_lat": "-22.9", "shape_pt_lon": "-43.1", "shape_pt_sequence": "1"},
                    {"shape_id": "S_EXEC", "shape_pt_lat": "-22.91", "shape_pt_lon": "-43.11", "shape_pt_sequence": "2"},
                ]),
                "fare_attributes": pd.DataFrame([
                    {"fare_id": "F1", "price": "4.70", "currency_type": "USD"},
                ]),
                "feed_info": pd.DataFrame([
                    {"feed_publisher_name": "ORIGINAL_DO_PROC", "feed_version": "2028-11-05Q"},
                ]),
            }

            with zipfile.ZipFile(input_zip, "w") as zf:
                for k, df in tables_in.items():
                    zf.writestr(f"{k}.txt", df.to_csv(index=False))

            cfg_path = base / "config.json"
            cfg_path.write_text(
                json.dumps({
                    "BASE_DADOS": str(base),
                    "ano_gtfs": "2028",
                    "mes_gtfs": "11",
                    "estudo_gtfs": "05",
                    "endereco_gtfs": str(input_zip),
                    "endereco_gtfs_combi": str(combi_zip),
                    "caminho_gtfs_pub": str(pub_zip),
                    "caminho_cores": str(insumos / "gtfs_cores.csv"),
                }),
                encoding="utf-8",
            )

            res = subprocess.run(
                [sys.executable, str(SCRIPT_PATH), "--config", str(cfg_path)],
                capture_output=True,
                text=True,
            )
            self.assertEqual(res.returncode, 0, msg=res.stderr or res.stdout)

            combi = _read_zip_tables(combi_zip)
            pub = _read_zip_tables(pub_zip)

            # Combi preserva EXCEP, ajusta service_id e não substitui feed_info do disco
            self.assertIn("T_EXCEP", set(combi["trips"]["trip_id"]))
            self.assertIn("U_REG", set(combi["trips"]["service_id"]))
            self.assertEqual(combi["feed_info"].iloc[0]["feed_publisher_name"], "ORIGINAL_DO_PROC")
            self.assertEqual(combi["fare_attributes"].iloc[0]["currency_type"], "BRL")

            # Pub remove EXCEP, preserva cores BRT (702), aplica cores 700 e 200, e não substitui feed_info
            self.assertNotIn("T_EXCEP", set(pub["trips"]["trip_id"]))
            self.assertEqual(pub["feed_info"].iloc[0]["feed_publisher_name"], "ORIGINAL_DO_PROC")

            pub_routes = pub["routes"].set_index("route_id")
            self.assertEqual(pub_routes.loc["R_BRT", "route_type"], "702")
            self.assertEqual(pub_routes.loc["R_BRT", "route_color"], "00AA55")
            self.assertEqual(pub_routes.loc["R_SPPO", "route_type"], "700")
            self.assertEqual(pub_routes.loc["R_SPPO", "route_color"], "AA1122")
            self.assertEqual(pub_routes.loc["R_EXEC", "route_type"], "200")
            self.assertEqual(pub_routes.loc["R_EXEC", "route_color"], "030478")


if __name__ == "__main__":
    unittest.main()
