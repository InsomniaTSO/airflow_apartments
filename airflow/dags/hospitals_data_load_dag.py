import logging
import csv
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow import DAG
from hospitals_data_load import HospitalsDataOperator
from data_path import temp_data_path
from const import DICT_DISTRICRS


DEFAULT_ARGS = {
    "owner": "t-manakova",
    "poke_interval": 600,
    "retries": 3,
    "start_date": days_ago(1),
}

with DAG(
    dag_id="hospitals_data_load",
    schedule_interval="@weekly",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["hospitals_data_load"],
) as dag:
    get_hospitals = HospitalsDataOperator(task_id="get_hospitals_data", per_page=100)

    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="conn_postgres",
        sql="""
              CREATE TABLE IF NOT EXISTS apartment.apartment.hospitals_info (
              id INT NOT NULL,
              district_id INT NOT NULL,
              name VARCHAR NOT NULL,
              type VARCHAR NOT NULL,
              address VARCHAR NOT NULL,
              lat double precision NOT NULL,
              lon double precision NOT NULL);
            """,
    )

    def hospitals_to_table_func(**kwargs):
        pg_hook = PostgresHook(postgres_conn_id="conn_postgres")
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("TRUNCATE TABLE apartment.apartment.hospitals_info;")
        conn.commit()
        with open(temp_data_path("hospitals.csv"), newline="") as csvfile:
            hospitals = csv.DictReader(csvfile)
            for hospital in hospitals:
                cursor.execute(
                    f"""
                                INSERT INTO apartment.apartment.hospitals_info(id, district_id, name,
                                                                               type, address, lat, lon)
                                VALUES (
                                     {hospital["id"]},
                                     {DICT_DISTRICRS.get(hospital["district"], 0)},
                                    "{hospital["name"]}",
                                    "{hospital["type"]}",
                                    "{hospital["address"]}",
                                     {hospital["lat"]},
                                     {hospital["lon"]}
                                );
                                """
                )
                conn.commit()
            cursor.close()
            conn.close()

    hospitals_to_table = PythonOperator(
        task_id="hospitals_to_table_func", 
        python_callable=hospitals_to_table_func
    )

    def table_to_logs_func(**kwargs):
        pg_hook = PostgresHook(postgres_conn_id="conn_postgres")
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM apartment.apartment.hospitals_info LIMIT 1;")
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
        bash_command=f"rm {temp_data_path('hospitals.csv')}"
    )

    get_hospitals >> create_table >> hospitals_to_table >> table_to_logs >> delete_temp_csv
