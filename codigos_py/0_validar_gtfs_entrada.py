import pandas as pd
import zipfile
import re
import unicodedata
from pathlib import Path
import sys
from datetime import datetime

import argparse
import json

# ==============================================================================
# LEITURA DE CONFIGURAÇÃO (Streamlit)
# ==============================================================================
_parser = argparse.ArgumentParser()
_parser.add_argument('--config', type=str, default='', help='Caminho para arquivo JSON de configuração')
_args, _ = _parser.parse_known_args()
_config = {}
if _args.config:
    with open(_args.config, 'r', encoding='utf-8') as _f:
        _config = json.load(_f)

# ==============================================================================
# CONFIGURAÇÕES
# ==============================================================================
BASE_DADOS = Path(_config.get("BASE_DADOS", "C:/R_SMTR/dados"))
PASTA_RESULTADOS = Path(_config.get("PASTA_RESULTADOS", "C:/R_SMTR/resultados/validacoes_snapshot"))

ano_gtfs = _config.get("ano_gtfs", "2027")
mes_gtfs = _config.get("mes_gtfs", "08")
estudo_gtfs = _config.get("estudo_gtfs", "04")
gtfs_processar = _config.get("gtfs_processar", "sppo")  # "brt" ou "sppo" ou "rio"

# Arquivo GTFS de entrada (pode ser _PROC.zip ou original)
# Ajuste conforme necessário
endereco_gtfs = Path(_config.get("endereco_gtfs", BASE_DADOS / f"gtfs/{ano_gtfs}/{gtfs_processar}_{ano_gtfs}-{mes_gtfs}-{estudo_gtfs}Q.zip"))

# ==============================================================================
# FUNÇÕES AUXILIARES
# ==============================================================================
def log_msg(msg):
    print(f"[{pd.Timestamp.now().strftime('%H:%M:%S')}] {msg}")

def remover_acentos(texto):
    """Remove acentos de uma string."""
    if pd.isna(texto):
        return ""
    return unicodedata.normalize('NFKD', str(texto)).encode('ASCII', 'ignore').decode('ASCII')

def validar_padrao_excepcionalidade(trip_headsign):
    """
    Valida se o texto entre colchetes em trip_headsign segue o padrão:
    - Sem acentos
    - Espaços substituídos por _
    - Sem letras maiúsculas
    - Apenas caracteres a-z, 0-9, _
    """
    if pd.isna(trip_headsign):
        return True, None

    texto = str(trip_headsign)

    # Extrair texto entre colchetes
    matches = re.findall(r'\[([^\]]+)\]', texto)
    if not matches:
        return True, None  # Não é uma trip de excepcionalidade

    erros = []
    for match in matches:
        # Verificar se tem acentos
        if remover_acentos(match) != match:
            erros.append(f"Acentos: '{match}'")

        # Verificar se tem espaços (devem ser _)
        if ' ' in match:
            erros.append(f"Espacos (devem ser _): '{match}'")

        # Verificar se tem maiúsculas
        if match != match.lower():
            erros.append(f"Maiusculas: '{match}'")

        # Verificar caracteres especiais além de _ e alfanuméricos
        if not re.match(r'^[a-z0-9_]+$', match):
            erros.append(f"Caracteres invalidos (a-z, 0-9, _): '{match}'")

    return len(erros) == 0, erros

def carregar_gtfs(zip_path):
    """Carrega arquivos GTFS essenciais do ZIP."""
    log_msg(f"Lendo GTFS: {zip_path}")
    gtfs_data = {}
    with zipfile.ZipFile(zip_path, 'r') as z:
        for fname in ['routes.txt', 'trips.txt', 'stop_times.txt', 'calendar.txt', 'calendar_dates.txt', 'shapes.txt']:
            if fname in z.namelist():
                with z.open(fname) as f:
                    try:
                        gtfs_data[fname.split('.')[0]] = pd.read_csv(f, dtype=str)
                    except pd.errors.EmptyDataError:
                        gtfs_data[fname.split('.')[0]] = pd.DataFrame()
            else:
                log_msg(f"Aviso: Arquivo {fname} nao encontrado no GTFS")
                gtfs_data[fname.split('.')[0]] = pd.DataFrame()
    return gtfs_data

def gerar_sugestao_correcao(trip_headsign):
    """
    Gera sugestão de correção para o trip_headsign fora do padrão.
    """
    matches = re.findall(r'\[([^\]]+)\]', str(trip_headsign))
    if not matches:
        return trip_headsign

    sugestoes = []
    for match in matches:
        # Aplicar correções: remover acentos, substituir espaços por _, lowercase
        corrigido = remover_acentos(match)
        corrigido = corrigido.replace(' ', '_')
        corrigido = corrigido.lower()
        # Remover caracteres não permitidos
        corrigido = re.sub(r'[^a-z0-9_]', '', corrigido)
        sugestoes.append(f"[{corrigido}]")

    # Substituir no headsign original
    resultado = str(trip_headsign)
    for orig, sug in zip(matches, sugestoes):
        resultado = resultado.replace(f"[{orig}]", sug)
    return resultado

# ==============================================================================
# VALIDAÇÕES PRINCIPAIS
# ==============================================================================
def validar_routes_com_trips(routes_df, trips_df):
    """
    Validação 1: Garantir que existam trips para cada route (via route_id).
    Retorna: (sucesso, lista_erros, dataframe_detalhado)
    """
    log_msg("=" * 60)
    log_msg("VALIDACAO 1: Routes com trips associadas")
    log_msg("=" * 60)

    if routes_df.empty:
        log_msg("AVISO: routes.txt esta vazio!")
        return False, [], pd.DataFrame()

    if trips_df.empty:
        log_msg("AVISO: trips.txt esta vazio!")
        return False, [], pd.DataFrame()

    routes_com_trips = set(trips_df['route_id'].dropna().unique())
    routes_sem_trips = routes_df[~routes_df['route_id'].isin(routes_com_trips)].copy()

    erros = []
    detalhes = []

    if len(routes_sem_trips) > 0:
        for _, row in routes_sem_trips.iterrows():
            route_id = row['route_id']
            route_short_name = row.get('route_short_name', 'N/A')
            route_long_name = row.get('route_long_name', 'N/A')
            route_type = row.get('route_type', 'N/A')

            erro = f"Route sem trips: route_id={route_id}, short_name={route_short_name}, long_name={route_long_name}"
            erros.append(erro)

            detalhes.append({
                'validacao': 'Routes sem trips',
                'status': 'FALHA',
                'route_id': route_id,
                'route_short_name': route_short_name,
                'route_long_name': route_long_name,
                'route_type': route_type,
                'trip_id': '',
                'service_id': '',
                'trip_headsign': '',
                'problema': 'Route sem nenhuma trip associada',
                'sugestao': 'Verificar se a route deve existir ou adicionar trips no trips.txt'
            })

        log_msg(f"FALHA: {len(erros)} route(s) sem trips associadas")
        for erro in erros[:10]:  # Mostrar apenas as primeiras 10 no console
            log_msg(f"  - {erro}")
        if len(erros) > 10:
            log_msg(f"  ... e mais {len(erros) - 10} routes (veja CSV completo)")
        return False, erros, pd.DataFrame(detalhes)
    else:
        log_msg(f"OK: Todos os {len(routes_df)} routes possuem pelo menos uma trip")
        # Adicionar info das routes OK
        for _, row in routes_df.iterrows():
            n_trips = len(trips_df[trips_df['route_id'] == row['route_id']])
            detalhes.append({
                'validacao': 'Routes sem trips',
                'status': 'OK',
                'route_id': row['route_id'],
                'route_short_name': row.get('route_short_name', 'N/A'),
                'route_long_name': row.get('route_long_name', 'N/A'),
                'route_type': row.get('route_type', 'N/A'),
                'trip_id': '',
                'service_id': '',
                'trip_headsign': '',
                'problema': '',
                'sugestao': f'{n_trips} trip(s) associada(s)'
            })
        return True, [], pd.DataFrame(detalhes)

def validar_trips_excepcionalidade(trips_df):
    """
    Validação 2: Garantir que trips de excepcionalidade (com [] no trip_headsign)
    sigam o padrão da equipe: sem acentos, espaços = _, sem maiúsculas.
    Retorna: (sucesso, lista_erros, dataframe_detalhado)
    """
    log_msg("=" * 60)
    log_msg("VALIDACAO 2: Padrao de trip_headsign em trips de excepcionalidade")
    log_msg("=" * 60)

    if trips_df.empty:
        log_msg("AVISO: trips.txt esta vazio!")
        return False, [], pd.DataFrame()

    if 'trip_headsign' not in trips_df.columns:
        log_msg("AVISO: Coluna trip_headsign nao encontrada em trips.txt")
        return False, [], pd.DataFrame()

    # Filtrar trips com colchetes no trip_headsign
    mask_excep = trips_df['trip_headsign'].notna() & trips_df['trip_headsign'].str.contains(r'\[', regex=True)
    trips_excep = trips_df[mask_excep].copy()

    log_msg(f"Total de trips com '[' no trip_headsign: {len(trips_excep)}")

    if trips_excep.empty:
        log_msg("OK: Nenhuma trip de excepcionalidade encontrada")
        return True, [], pd.DataFrame()

    erros = []
    detalhes = []

    for _, row in trips_excep.iterrows():
        trip_id = row['trip_id']
        trip_headsign = row['trip_headsign']
        route_id = row.get('route_id', 'N/A')
        service_id = row.get('service_id', 'N/A')
        direction_id = row.get('direction_id', 'N/A')
        trip_short_name = row.get('trip_short_name', 'N/A')

        valido, erros_trip = validar_padrao_excepcionalidade(trip_headsign)
        sugestao = gerar_sugestao_correcao(trip_headsign)

        if not valido:
            for erro in erros_trip:
                erros.append(f"trip_id={trip_id}, route_id={route_id}, headsign='{trip_headsign}' -> {erro}")

            detalhes.append({
                'validacao': 'Padrao excepcionalidade',
                'status': 'FALHA',
                'route_id': route_id,
                'route_short_name': '',
                'route_long_name': '',
                'route_type': '',
                'trip_id': trip_id,
                'service_id': service_id,
                'trip_headsign': trip_headsign,
                'problema': ' | '.join(erros_trip),
                'sugestao': f"Corrigir para: {sugestao}"
            })
        else:
            detalhes.append({
                'validacao': 'Padrao excepcionalidade',
                'status': 'OK',
                'route_id': route_id,
                'route_short_name': '',
                'route_long_name': '',
                'route_type': '',
                'trip_id': trip_id,
                'service_id': service_id,
                'trip_headsign': trip_headsign,
                'problema': '',
                'sugestao': 'Padrao correto'
            })

    if erros:
        log_msg(f"FALHA: {len(erros)} problema(s) em {trips_excep['trip_id'].nunique()} trip(s) de excepcionalidade")
        for erro in erros[:10]:
            log_msg(f"  - {erro}")
        if len(erros) > 10:
            log_msg(f"  ... e mais {len(erros) - 10} problemas (veja CSV completo)")
        return False, erros, pd.DataFrame(detalhes)
    else:
        log_msg(f"OK: Todas as {len(trips_excep)} trips de excepcionalidade seguem o padrao")
        return True, [], pd.DataFrame(detalhes)

def validar_trips_sem_route_id(trips_df):
    """
    Validação extra: Verificar trips sem route_id.
    """
    log_msg("-" * 60)
    log_msg("VALIDACAO EXTRA: Trips sem route_id")
    log_msg("-" * 60)

    if trips_df.empty:
        return True, []

    trips_sem_route = trips_df[trips_df['route_id'].isna() | (trips_df['route_id'] == "")]

    if len(trips_sem_route) > 0:
        log_msg(f"AVISO: {len(trips_sem_route)} trip(s) sem route_id")
        for _, row in trips_sem_route.head(10).iterrows():
            log_msg(f"  - trip_id={row['trip_id']}, service_id={row.get('service_id', 'N/A')}")
        return True, []
    else:
        log_msg("OK: Todas as trips possuem route_id preenchido")
        return True, []

def validar_service_ids(trips_df):
    """
    Validação extra: Verificar service_ids usados.
    """
    log_msg("-" * 60)
    log_msg("VALIDACAO EXTRA: Service IDs utilizados")
    log_msg("-" * 60)

    if trips_df.empty:
        return True, []

    service_ids = trips_df['service_id'].dropna().unique()
    log_msg(f"Service IDs encontrados: {sorted(service_ids)}")

    # Contar trips por service_id
    for sid in sorted(service_ids):
        count = len(trips_df[trips_df['service_id'] == sid])
        log_msg(f"  - {sid}: {count} trip(s)")
    return True, []

# ==============================================================================
# EXPORTAÇÃO DE RESULTADOS
# ==============================================================================
def exportar_relatorio_csv(detalhes_todas_validacoes, pasta_saida, nome_arquivo_base):
    """
    Exporta o relatório detalhado para CSV.
    """
    pasta_saida.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    arquivo_csv = pasta_saida / f"{nome_arquivo_base}_{timestamp}.csv"

    if not detalhes_todas_validacoes.empty:
        detalhes_todas_validacoes.to_csv(arquivo_csv, index=False, encoding='utf-8-sig')
        log_msg(f"Relatorio CSV salvo em: {arquivo_csv}")
    else:
        # Criar CSV vazio com cabeçalho
        df_vazio = pd.DataFrame(columns=[
            'validacao', 'status', 'route_id', 'route_short_name', 'route_long_name',
            'route_type', 'trip_id', 'service_id', 'trip_headsign', 'problema', 'sugestao'
        ])
        df_vazio.to_csv(arquivo_csv, index=False, encoding='utf-8-sig')
        log_msg(f"Relatorio CSV vazio salvo em: {arquivo_csv}")

    return arquivo_csv

def imprimir_resumo_consolidado(detalhes_df):
    """
    Imprime um resumo consolidado e legível no console.
    """
    print("\n" + "=" * 80)
    print("RESUMO CONSOLIDADO DE VALIDACOES")
    print("=" * 80)

    if detalhes_df.empty:
        print("Nenhum dado para exibir.")
        return

    # Resumo por validação
    for validacao in detalhes_df['validacao'].unique():
        df_val = detalhes_df[detalhes_df['validacao'] == validacao]
        total = len(df_val)
        ok = len(df_val[df_val['status'] == 'OK'])
        falha = len(df_val[df_val['status'] == 'FALHA'])
        aviso = len(df_val[df_val['status'] == 'AVISO'])

        status_icon = "OK" if falha == 0 else "FALHA"
        print(f"\n  {status_icon} {validacao}")
        print(f"     Total: {total} | OK: {ok} | FALHA: {falha} | AVISO: {aviso}")

        # Mostrar detalhes das falhas
        if falha > 0:
            falhas = df_val[df_val['status'] == 'FALHA']
            for _, row in falhas.head(5).iterrows():
                ident = row['trip_id'] if row['trip_id'] else row['route_id']
                print(f"     - {ident}: {row['problema']}")
                if row['sugestao']:
                    print(f"       -> Sugestao: {row['sugestao']}")
            if len(falhas) > 5:
                print(f"     ... e mais {len(falhas) - 5} problema(s) (veja CSV)")

    print("\n" + "=" * 80)

# ==============================================================================
# EXECUÇÃO PRINCIPAL
# ==============================================================================
if __name__ == "__main__":
    print("\n" + "=" * 80)
    print("              VALIDACAO DE GTFS DE ENTRADA - CODIGO 2")
    print("=" * 80)
    print(f"Arquivo GTFS: {endereco_gtfs}")
    print(f"Pasta resultados: {PASTA_RESULTADOS}")
    print("=" * 80 + "\n")

    if not endereco_gtfs.exists():
        log_msg(f"ERRO: Arquivo GTFS nao encontrado: {endereco_gtfs}")
        sys.exit(1)

    # Carregar GTFS
    gtfs = carregar_gtfs(endereco_gtfs)

    routes_df = gtfs.get('routes', pd.DataFrame())
    trips_df = gtfs.get('trips', pd.DataFrame())

    log_msg(f"Routes carregadas: {len(routes_df)}")
    log_msg(f"Trips carregadas: {len(trips_df)}")
    print()

    # Executar validações e coletar detalhes
    todos_detalhes = []

    # Validação 1: Routes com trips
    ok, erros, detalhes = validar_routes_com_trips(routes_df, trips_df)
    if not detalhes.empty:
        todos_detalhes.append(detalhes)
    print()

    # Validação 2: Padrão de excepcionalidade
    ok, erros, detalhes = validar_trips_excepcionalidade(trips_df)
    if not detalhes.empty:
        todos_detalhes.append(detalhes)
    print()

    # Validações extras (apenas avisos no console)
    validar_trips_sem_route_id(trips_df)
    print()
    validar_service_ids(trips_df)
    print()

    # Consolidar todos os detalhes
    if todos_detalhes:
        detalhes_consolidado = pd.concat(todos_detalhes, ignore_index=True)
    else:
        detalhes_consolidado = pd.DataFrame()

    # Imprimir resumo consolidado
    imprimir_resumo_consolidado(detalhes_consolidado)

    # Exportar CSV
    arquivo_csv = exportar_relatorio_csv(
        detalhes_consolidado,
        PASTA_RESULTADOS,
        f"validacao_gtfs_entrada_{gtfs_processar}_{ano_gtfs}{mes_gtfs}{estudo_gtfs}Q"
    )

    # Resumo final
    total_falhas = len(detalhes_consolidado[detalhes_consolidado['status'] == 'FALHA']) if not detalhes_consolidado.empty else 0
    total_ok = len(detalhes_consolidado[detalhes_consolidado['status'] == 'OK']) if not detalhes_consolidado.empty else 0

    print(f"\nRESULTADO FINAL:")
    print(f"  Validacoes OK: {total_ok}")
    print(f"  Problemas (FALHA): {total_falhas}")
    print(f"  Relatorio completo: {arquivo_csv}")
    print("=" * 80)

    if total_falhas == 0:
        log_msg("SUCESSO: Todas as validacoes criticas passaram!")
        sys.exit(0)
    else:
        log_msg(f"FALHA: {total_falhas} problema(s) critico(s) encontrado(s)!")
        sys.exit(1)