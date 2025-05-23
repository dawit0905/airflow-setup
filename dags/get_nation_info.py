from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from pandas import Timestamp

import pandas as pd
import logging
from airflow.utils.dates import days_ago
import requests


def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()


@task
def extract(url: str):
    logging.info("extract started")
    response = requests.get(url)
    response.raise_for_status()
    logging.info("load done")
    return response.json()

@task
def transform(data):
    logging.info("transform started")
    transformed = []
    for item in data:
        # 안전하게 .get()을 쓰거나 KeyError 예외 처리 가능
        try:
            official_name = item['name']['official']
            population = item.get('population', None)
            area = item.get('area', None)

            transformed.append({
                'country': official_name,
                'population': population,
                'area': area
            })
        except KeyError:
            # 필수 키가 없으면 건너뛰기
            continue

    logging.info("transform done")
    return transformed

def _create_table(cur, schema, table, drop_first):
    if drop_first:
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table};")
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table} (
        country VARCHAR,
        population int,
        area int)
    """)


@task
def load(schema, table, records):
    logging.info("load started")
    cur = get_Redshift_connection()
    try:
        cur.execute("BEGIN;")
        # 원본 테이블이 없으면 생성 - 테이블이 처음 한번 만들어질 때 필요한 코드
        _create_table(cur, schema, table, True)

        insert_sql = f"""
            INSERT INTO {schema}.{table} (country, population, area)
            VALUES (%s, %s, %s);
        """

        for r in records:
            cur.execute(insert_sql, (
                r['country'],
                r['population'],
                r['area'],
            ))
            print(insert_sql, (r['country'], r['population'], r['area']))

        cur.execute("COMMIT;")   # cur.execute("END;")
    except Exception as error:
        print(error)
        cur.execute("ROLLBACK;")
        raise
    logging.info("load done")


with DAG(
    dag_id = 'get_nation_info',
    start_date = days_ago(1),
    catchup=False,
    tags=['API'],
    schedule_interval = '30 6 * * 6'
) as dag:
    schema = 'dawit0905'   ## 자신의 스키마로 변경
    table = 'nation_info'

    url = 'https://restcountries.com/v3/all'
    result = transform(extract(url))
    load(schema, table, result)
