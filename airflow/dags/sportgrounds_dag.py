import logging
import csv
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow import DAG
from sportgounds import SportgroundsDataOperator
from data_path import temp_data_path
from const import DICT_DISTRICRS


DEFAULT_ARGS = {
    "owner": "t-manakova",
    "poke_interval": 600,
    "retries": 3,
    "start_date": days_ago(1),
}

with DAG(
    dag_id="sportgrounds_data_load",
    schedule_interval="@weekly",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["sportgrounds_data_load"],
) as dag:
    get_sportgrounds = SportgroundsDataOperator(
        task_id="get_sportgrounds_data",
        per_page=100,
    )

    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="conn_postgres",
        sql="""
              CREATE TABLE IF NOT EXISTS apartment.apartment.sportgrounds_info (
              id INT NOT NULL,
              district_id INT NOT NULL,
              name VARCHAR NOT NULL,
              categories VARCHAR NOT NULL,
              season VARCHAR NOT NULL,
              address VARCHAR NOT NULL,
              lat double precision NOT NULL,
              lon double precision NOT NULL);
            """,
    )

    def sportgrounds_to_table_func(**kwargs):
        pg_hook = PostgresHook(postgres_conn_id="conn_postgres")
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("TRUNCATE TABLE apartment.apartment.sportgrounds_info;")
        conn.commit()
        with open(temp_data_path("sportgrounds.csv"), newline="") as csvfile:
            sportgrounds = csv.DictReader(csvfile)
            for sportgrounds in sportgrounds:
                cursor.execute(
                    f"""
                                INSERT INTO apartment.apartment.sportgrounds_info(id, district_id,
                                                                             name, categories,
                                                                             season, address,
                                                                             lat, lon)
                                VALUES (
                                     {sportgrounds["id"]},
                                     {DICT_DISTRICRS.get(sportgrounds["district"], 0)},
                                    "{sportgrounds["name"]}",
                                    "{sportgrounds["categories"]}",
                                    "{sportgrounds["season"]}",
                                    "{sportgrounds["address"]}",
                                     {sportgrounds["lat"]},
                                     {sportgrounds["lon"]}
                                );
                                """
                )
                conn.commit()
            cursor.close()
            conn.close()

    sportgrounds_to_table = PythonOperator(
        task_id="sportgrounds_to_table_func", python_callable=sportgrounds_to_table_func
    )

    def table_to_logs_func(**kwargs):
        pg_hook = PostgresHook(postgres_conn_id="conn_postgres")
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM apartment.apartment.sportgrounds_info LIMIT 1;")
        query_res = cursor.fetchall()
        logging.info("-----------------------------------------------")
        logging.info("Дата исполнения: " + kwargs["ds"])
        logging.info(query_res)
        logging.info("-----------------------------------------------")
        cursor.close()
        conn.close()

    table_to_logs = PythonOperator(
        task_id="table_to_logs", python_callable=table_to_logs_func
    )

    delete_temp_csv = BashOperator(
        task_id="delete_temp_csv",
        bash_command=f"rm {temp_data_path('sportgrounds.csv')}",
    )

    (
        get_sportgrounds >> create_table
        >> sportgrounds_to_table
        >> table_to_logs
        >> delete_temp_csv
    )
