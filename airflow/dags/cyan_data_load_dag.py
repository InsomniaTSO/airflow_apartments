import logging
import csv
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from apartment_data_load import get_apartment_from_cyan
from data_path import temp_data_path
from const import DICT_DISTRICRS
from airflow import DAG


DEFAULT_ARGS = {
    "owner": "t-manakova",
    "poke_interval": 600,
    "retries": 3,
    "start_date": days_ago(1),
}

with DAG(
    dag_id="cyan_data_load",
    schedule_interval="00 10 * * *",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["cyan_data_load"],
) as dag:
    get_apartments = PythonOperator(
        task_id="get_apartments_data", python_callable=get_apartment_from_cyan
    )

    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="conn_postgres",
        sql="""
              CREATE TABLE IF NOT EXISTS apartment.apartment.apartments_info (
              id SERIAL PRIMARY KEY,
              date DATE NOT NULL,
              district_id INT NOT NULL,
              metro VARCHAR NOT NULL,
              address VARCHAR NOT NULL,
              floor INT NOT NULL,
              floors INT NOT NULL,
              rooms VARCHAR NOT NULL,
              area DECIMAL NOT NULL,
              price INT NOT NULL,
              link VARCHAR NOT NULL,
              lat double precision NOT NULL,
              lon double precision NOT NULL);
              """,
    )

    def apartments_to_table_func(**kwargs):
        pg_hook = PostgresHook(postgres_conn_id="conn_postgres")
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        counter = 0
        with open(temp_data_path("apartments.csv"), newline="") as csvfile:
            apartments = csv.DictReader(csvfile)
            for apartment in apartments:
                cursor.execute(
                    f"""
                                INSERT INTO apartment.apartment.apartments_info(date, floor, floors, rooms,
                                                                            area, price, link, district_id,
                                                                            metro, address, lat, lon)
                                VALUES (
                                    CURRENT_DATE,
                                    {apartment["floor"]},
                                    {apartment["floors"]},
                                    "{apartment["rooms"]}",
                                    {apartment["area"]},
                                    {apartment["int_price"]},
                                    "{apartment["link"]}",
                                    {DICT_DISTRICRS.get(apartment["district"], 0)},
                                    "{apartment["metro"]}",
                                    "{apartment["address"]}",
                                    {apartment["lat"]},
                                    {apartment["lon"]}
                                );
                                """
                )
                counter += 1
                conn.commit()
            cursor.close()
            conn.close()
        return counter

    apartments_to_table = PythonOperator(
        task_id="apartments_to_table_func",
        python_callable=apartments_to_table_func
    )

    def table_to_logs_func(**kwargs):
        numbers_row = kwargs["ti"].xcom_pull(task_ids="apartments_to_table_func")
        pg_hook = PostgresHook(postgres_conn_id="conn_postgres")
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT link FROM apartment.apartment.apartments_info;")
        query_res = cursor.fetchall()
        logging.info("-----------------------------------------------")
        logging.info("Дата исполнения: " + kwargs["ds"])
        logging.info("Загружено: " + str(numbers_row) + " строк")
        logging.info(query_res)
        logging.info("-----------------------------------------------")
        cursor.close()
        conn.close()

    table_to_logs = PythonOperator(
        task_id="table_to_logs", python_callable=table_to_logs_func
    )

    delete_temp_csv = BashOperator(
        task_id="delete_temp_csv",
        bash_command=f"rm {temp_data_path('apartments.csv')}"
    )

    (
        get_apartments
        >> create_table
        >> apartments_to_table
        >> table_to_logs
        >> delete_temp_csv
    )
