import logging
import csv
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow import DAG
from environment_objects_data_load import ObjectsDataOperator
from data_path import temp_data_path
from const import DICT_DISTRICRS


DEFAULT_ARGS = {
    "owner": "t-manakova",
    "poke_interval": 600,
    "retries": 3,
    "start_date": days_ago(1)
}

with DAG(
    dag_id="objects_data_load",
    schedule_interval="@weekly",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["objects_data_load"]
) as dag:

    get_objects = ObjectsDataOperator(
        task_id="get_objects_data",
        per_page=100,
    )

    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="conn_postgres",
        sql="""
              CREATE TABLE IF NOT EXISTS apartment.apartment.objects_info (
              id INT NOT NULL,
              district_id INT NOT NULL,
              type_id INT NOT NULL,
              type_name VARCHAR NOT NULL);
            """
    )

    def objects_to_table_func(**kwargs):
        pg_hook = PostgresHook(postgres_conn_id="conn_postgres")
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("TRUNCATE TABLE apartment.apartment.objects_info;")
        conn.commit()
        with open(temp_data_path("objects.csv"), newline="") as csvfile:
            objects = csv.DictReader(csvfile)
            for objects in objects:
                cursor.execute(f"""
                                INSERT INTO apartment.apartment.objects_info(id, district_id,
                                                                             type_id, type_name)
                                VALUES (
                                     {objects["id"]},
                                     {DICT_DISTRICRS.get(objects["district"], 0)},
                                    "{objects["type_id"]}",
                                    "{objects["type_name"]}"
                                );
                                """
                            )
                conn.commit()
            cursor.close()
            conn.close()

    objects_to_table = PythonOperator(
        task_id="objects_to_table_func",
        python_callable=objects_to_table_func)

    def table_to_logs_func(**kwargs):
        pg_hook = PostgresHook(postgres_conn_id="conn_postgres")
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM apartment.apartment.objects_info LIMIT 1;")
        query_res = cursor.fetchall()
        logging.info("-----------------------------------------------")
        logging.info("Дата исполнения: " + kwargs["ds"])
        logging.info(query_res)
        logging.info("-----------------------------------------------")
        cursor.close()
        conn.close()

    table_to_logs = PythonOperator(
        task_id="table_to_logs",
        python_callable=table_to_logs_func)

    delete_temp_csv = BashOperator(
        task_id="delete_temp_csv",
        bash_command=f"rm {temp_data_path('objects.csv')}"
    )

    get_objects >> create_table >> objects_to_table >> table_to_logs >> delete_temp_csv