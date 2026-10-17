"""
DAG de ETL dos convenios do Ceara Transparente -> PostgreSQL.

Pipeline (3 tasks encadeadas):
  1. coletar   -> baixa os convenios do mes da API publica
  2. preparar  -> seleciona as colunas esperadas e trata a data de assinatura
  3. salvar    -> insere no PostgreSQL (tabela 'convenios')

Os dados passam de uma task para a outra via XCom (return / xcom_pull).
"""

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
API_URL = "https://api-dados-abertos.cearatransparente.ce.gov.br/transparencia/contratos/convenios"

# Conexao com o Postgres.
# host = nome do container do Postgres (os dois containers na mesma rede Docker)
# port = porta INTERNA do container (5432), nao a mapeada no host (5433)
PG_HOST = "db"
PG_PORT = 5432
PG_USER = "airflow"
PG_PASSWORD = "1234"
PG_DATABASE = "airflow"
PG_TABLE = "convenios"

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


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------
def coletar(**context) -> list[dict]:
    """Baixa os convenios do mes da execucao e retorna a lista de registros."""
    # Periodo = mes da data logica da execucao.
    logical_date = context["logical_date"].in_timezone(TIMEZONE)
    data_inicio = logical_date.start_of("month").format("DD/MM/YYYY")
    data_fim = logical_date.end_of("month").format("DD/MM/YYYY")

    session = requests.Session()
    try:
        registros: list[dict] = []
        pagina = 1
        while True:
            params = {
                "page": pagina,
                "data_assinatura_inicio": data_inicio,
                "data_assinatura_fim": data_fim,
            }
            resposta = session.get(API_URL, params=params, timeout=60)
            resposta.raise_for_status()
            payload = resposta.json()

            # Dados da pagina atual (ajuste a chave conforme o retorno da API).
            dados_pagina = payload.get("data", [])
            registros.extend(dados_pagina)

            total_paginas = int(payload.get("sumary", {}).get("total_pages", 1))
            if pagina >= total_paginas:
                break
            pagina += 1

        print(f"Coletados {len(registros)} convenios ({data_inicio} a {data_fim}).")
        return registros
    finally:
        session.close()


def preparar(**context) -> list[dict]:
    """Seleciona as colunas esperadas e trata a data de assinatura."""
    ti = context["ti"]
    registros = ti.xcom_pull(task_ids="coletar")

    if not registros:
        print("Nenhum registro para preparar.")
        return []

    df = pd.DataFrame(registros)
    colunas = [c for c in COLUNAS_ESPERADAS if c in df.columns]
    df = df[colunas].copy()

    # Converte a data e descarta linhas com data invalida.
    df["data_assinatura_dt"] = converter_data_assinatura(df["data_assinatura"])
    df = df.dropna(subset=["data_assinatura_dt"]).drop(columns=["data_assinatura_dt"])

    print(f"Preparados {len(df)} convenios.")
    return df.to_dict(orient="records")


def salvar(**context) -> None:
    """Insere os convenios preparados no PostgreSQL (ignora duplicados por id)."""
    ti = context["ti"]
    registros = ti.xcom_pull(task_ids="preparar")

    if not registros:
        print("Nenhum registro para salvar.")
        return

    colunas = list(registros[0].keys())
    cols_sql = ", ".join(f'"{c}"' for c in colunas)
    placeholders = ", ".join(["%s"] * len(colunas))
    insert_sql = (
        f"INSERT INTO {PG_TABLE} ({cols_sql}) VALUES ({placeholders}) "
        f'ON CONFLICT ("id") DO NOTHING'
    )
    valores = [[r.get(c) for c in colunas] for r in registros]

    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASSWORD,
        dbname=PG_DATABASE,
    )
    try:
        with conn:
            with conn.cursor() as cur:
                psycopg2.extras.execute_batch(cur, insert_sql, valores, page_size=500)
        print(f"Salvos {len(valores)} convenios na tabela {PG_TABLE}.")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Definicao da DAG
# ---------------------------------------------------------------------------
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="api_convenios_ceara_postgresql",
    schedule="0 18 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz=TIMEZONE),
    catchup=False,
    default_args=default_args,
    tags=["ceara", "convenios", "postgresql"],
) as dag:

    t1 = PythonOperator(task_id="coletar", python_callable=coletar)
    t2 = PythonOperator(task_id="preparar", python_callable=preparar)
    t3 = PythonOperator(task_id="salvar", python_callable=salvar)

    t1 >> t2 >> t3  # pipeline sequencial: coletar -> preparar -> salvar
