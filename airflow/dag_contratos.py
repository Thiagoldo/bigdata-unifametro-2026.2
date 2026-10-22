"""
DAG de ETL dos contratos do Ceara Transparente -> PostgreSQL.

Pipeline (3 tasks encadeadas):
  1. coletar   -> baixa os contratos de 2025 e 2026 da API publica
  2. preparar  -> seleciona as colunas esperadas e trata a data de assinatura
  3. salvar    -> insere no PostgreSQL (tabela 'contratos')

Como o periodo (2025-2026) tem dezenas de milhares de registros, os dados
passam de uma task para a outra por arquivos temporarios (JSONL) dentro do
container, e o XCom carrega apenas o caminho do arquivo.
"""

import json
import os
from datetime import timedelta

import pandas as pd
import pendulum
import psycopg2
import psycopg2.extras
import requests
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

# ---------------------------------------------------------------------------
# Configuracao
# ---------------------------------------------------------------------------
API_URL = "https://api-dados-abertos.cearatransparente.ce.gov.br/transparencia/contratos/contratos"

# Periodo coletado (query params data_assinatura_inicio / data_assinatura_fim).
DATA_ASSINATURA_INICIO = "01/01/2025"
DATA_ASSINATURA_FIM = "31/12/2026"

# Arquivos temporarios usados para trocar dados entre as tasks (dentro do
# container do Airflow). O XCom so carrega o caminho, nao os registros.
ARQUIVO_BRUTO = "/tmp/contratos_bruto.jsonl"
ARQUIVO_PREPARADO = "/tmp/contratos_preparado.jsonl"

# Conexao com o Postgres.
# host = nome do container do Postgres (os dois containers na mesma rede Docker)
# port = porta INTERNA do container (5432), nao a mapeada no host (5433)
PG_HOST = "db"
PG_PORT = 5432
PG_USER = "airflow"
PG_PASSWORD = "1234"
PG_DATABASE = "airflow"
PG_TABLE = "contratos"

TIMEZONE = "America/Fortaleza"

# Colunas que queremos manter do retorno da API (na ordem de insercao).
COLUNAS_ESPERADAS = [
    "id",
    "cod_concedente",
    "cod_financiador",
    "cod_gestora",
    "cod_orgao",
    "cod_secretaria",
    "descricao_modalidade",
    "descricao_objeto",
    "descricao_tipo",
    "descricao_url",
    "data_assinatura",
    "data_processamento",
    "data_termino",
    "flg_tipo",
    "isn_parte_destino",
    "isn_sic",
    "num_spu",
    "valor_contrato",
    "isn_modalidade",
    "isn_entidade",
    "tipo_objeto",
    "num_spu_licitacao",
    "descricao_justificativa",
    "valor_can_rstpg",
    "data_publicacao_portal",
    "descricao_url_pltrb",
    "descricao_url_ddisp",
    "descricao_url_inexg",
    "cod_plano_trabalho",
    "num_certidao",
    "descriaco_edital",
    "cpf_cnpj_financiador",
    "num_contrato",
    "valor_original_concedente",
    "valor_original_contrapartida",
    "valor_atualizado_concedente",
    "valor_atualizado_contrapartida",
    "created_at",
    "updated_at",
    "plain_num_contrato",
    "calculated_valor_aditivo",
    "calculated_valor_ajuste",
    "calculated_valor_empenhado",
    "calculated_valor_pago",
    "contract_type",
    "infringement_status",
    "cod_financiador_including_zeroes",
    "accountability_status",
    "plain_cpf_cnpj_financiador",
    "descricao_situacao",
    "data_publicacao_doe",
    "descricao_nome_credor",
    "isn_parte_origem",
    "data_auditoria",
    "data_termino_original",
    "data_inicio",
    "data_rescisao",
    "confidential",
    "gestor_contrato",
]


# ---------------------------------------------------------------------------
# Funcoes auxiliares
# ---------------------------------------------------------------------------
def converter_data_assinatura(datas: pd.Series) -> pd.Series:
    """Converte a coluna de data de assinatura para datetime no fuso local.

    Tenta varios formatos possiveis; o que nao casar vira NaT (nulo).
    """
    datas_texto = datas.astype(str).str.strip()
    convertida = pd.Series(pd.NaT, index=datas.index, dtype="datetime64[ns, UTC]")
    formatos = [
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%d %H:%M:%S%z",
        "%Y-%m-%d",
        "%d/%m/%Y",
    ]
    for formato in formatos:
        pendentes = convertida.isna()
        if not pendentes.any():
            break
        convertida.loc[pendentes] = pd.to_datetime(
            datas_texto.loc[pendentes], format=formato, errors="coerce", utc=True
        )
    return convertida.dt.tz_convert(TIMEZONE)


def ler_jsonl(caminho: str, tamanho_lote: int = 5000):
    """Le um arquivo JSONL (um registro por linha) em lotes de listas."""
    lote: list[dict] = []
    with open(caminho, encoding="utf-8") as arquivo:
        for linha in arquivo:
            linha = linha.strip()
            if not linha:
                continue
            lote.append(json.loads(linha))
            if len(lote) >= tamanho_lote:
                yield lote
                lote = []
    if lote:
        yield lote


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------
def coletar(**context) -> str:
    """Baixa os contratos de 2025-2026 e grava os registros brutos em JSONL."""
    session = requests.Session()
    try:
        total_registros = 0
        pagina = 1
        with open(ARQUIVO_BRUTO, "w", encoding="utf-8") as saida:
            while True:
                params = {
                    "page": pagina,
                    "data_assinatura_inicio": DATA_ASSINATURA_INICIO,
                    "data_assinatura_fim": DATA_ASSINATURA_FIM,
                }
                resposta = session.get(API_URL, params=params, timeout=60)
                resposta.raise_for_status()
                payload = resposta.json()

                # Dados da pagina atual (ajuste a chave conforme o retorno da API).
                dados_pagina = payload.get("data", [])
                for registro in dados_pagina:
                    saida.write(json.dumps(registro, ensure_ascii=False) + "\n")
                total_registros += len(dados_pagina)

                total_paginas = int(payload.get("sumary", {}).get("total_pages", 1))
                if pagina % 50 == 0 or pagina >= total_paginas:
                    print(f"Pagina {pagina}/{total_paginas} ({total_registros} registros).")
                if pagina >= total_paginas:
                    break
                pagina += 1

        print(
            f"Coletados {total_registros} contratos "
            f"({DATA_ASSINATURA_INICIO} a {DATA_ASSINATURA_FIM})."
        )
        return ARQUIVO_BRUTO
    finally:
        session.close()


def preparar(**context) -> dict:
    """Seleciona as colunas esperadas e trata a data de assinatura."""
    ti = context["ti"]
    caminho_bruto = ti.xcom_pull(task_ids="coletar")

    if not caminho_bruto or not os.path.exists(caminho_bruto):
        print("Nenhum registro para preparar.")
        return {}

    total = 0
    with open(ARQUIVO_PREPARADO, "w", encoding="utf-8") as saida:
        for lote in ler_jsonl(caminho_bruto):
            df = pd.DataFrame(lote)
            colunas = [c for c in COLUNAS_ESPERADAS if c in df.columns]
            df = df[colunas].copy()

            # Converte a data e descarta linhas com data invalida.
            df["data_assinatura_dt"] = converter_data_assinatura(df["data_assinatura"])
            df = df.dropna(subset=["data_assinatura_dt"]).drop(columns=["data_assinatura_dt"])

            # to_json/loads normaliza os tipos do pandas (NaN -> null, etc.).
            registros = json.loads(df.to_json(orient="records"))
            for registro in registros:
                saida.write(json.dumps(registro, ensure_ascii=False) + "\n")
            total += len(registros)

    print(f"Preparados {total} contratos.")
    return {"arquivo": ARQUIVO_PREPARADO, "total": total}


def salvar(**context) -> None:
    """Insere/atualiza os contratos preparados no PostgreSQL (chave: id)."""
    ti = context["ti"]
    info = ti.xcom_pull(task_ids="preparar") or {}
    caminho = info.get("arquivo")

    if not caminho or not os.path.exists(caminho):
        print("Nenhum registro para salvar.")
        return

    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASSWORD,
        dbname=PG_DATABASE,
    )
    total = 0
    try:
        with conn:
            with conn.cursor() as cur:
                insert_sql = None
                for lote in ler_jsonl(caminho):
                    colunas = list(lote[0].keys())
                    if insert_sql is None:
                        cols_sql = ", ".join(f'"{c}"' for c in colunas)
                        placeholders = ", ".join(["%s"] * len(colunas))
                        # Contratos sao atualizados na origem (updated_at), entao
                        # refletimos as mudancas ao inves de ignorar duplicados.
                        cols_update = ", ".join(
                            f'"{c}" = EXCLUDED."{c}"' for c in colunas if c != "id"
                        )
                        insert_sql = (
                            f"INSERT INTO {PG_TABLE} ({cols_sql}) VALUES ({placeholders}) "
                            f'ON CONFLICT ("id") DO UPDATE SET {cols_update}'
                        )
                    valores = [[r.get(c) for c in colunas] for r in lote]
                    psycopg2.extras.execute_batch(cur, insert_sql, valores, page_size=500)
                    total += len(valores)
        print(f"Salvos {total} contratos na tabela {PG_TABLE}.")
    finally:
        conn.close()

    # Limpa os arquivos temporarios da execucao.
    for arquivo in (caminho, ARQUIVO_BRUTO):
        try:
            os.remove(arquivo)
        except OSError:
            pass


# ---------------------------------------------------------------------------
# Definicao da DAG
# ---------------------------------------------------------------------------
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="api_contratos_ceara_postgresql",
    schedule="0 19 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz=TIMEZONE),
    catchup=False,
    default_args=default_args,
    tags=["ceara", "contratos", "postgresql"],
) as dag:

    t1 = PythonOperator(task_id="coletar", python_callable=coletar)
    t2 = PythonOperator(task_id="preparar", python_callable=preparar)
    t3 = PythonOperator(task_id="salvar", python_callable=salvar)

    t1 >> t2 >> t3  # pipeline sequencial: coletar -> preparar -> salvar
