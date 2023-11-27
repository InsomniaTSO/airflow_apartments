import logging
from datetime import timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago
from airflow import DAG
from data_path import csv_data_path


DEFAULT_ARGS = {
    "owner": "t-manakova",
    "poke_interval": 600,
    "retries": 3,
    "start_date": days_ago(1)
}

with DAG(
    dag_id="export_apartment_to_csv",
    schedule_interval="10 10 * * *",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["export_apartment_to_csv"]
) as dag:

    wait_for_loading_data = ExternalTaskSensor(
        task_id="wait_for_loading_data",
        external_dag_id="cyan_data_load",
        external_task_id="delete_temp_csv",
        execution_delta=timedelta(minutes=10)
    )

    def apart_extract(copy_sql):
        pg_hook = PostgresHook.get_hook("conn_postgres")
        logging.info("Exporting query to file")
        pg_hook.copy_expert(copy_sql, filename=csv_data_path("apartments.csv"))

    export_last_apartment = PythonOperator(
        task_id="export_last_apartment",
        python_callable=apart_extract,
        op_kwargs={"copy_sql":
                   """
                    COPY (SELECT *
                          FROM apartment.apartment.apartments_info ai
                          WHERE ai.date >= CURRENT_DATE - INTERVAL "10 DAYS"
                          AND ai.date <= CURRENT_DATE)
                    TO STDOUT
                    WITH CSV HEADER;
                    """}
        )

    def infra_extract(copy_sql):
        pg_hook = PostgresHook.get_hook("conn_postgres")
        logging.info("Exporting query to file")
        pg_hook.copy_expert(copy_sql, filename=csv_data_path("infra.csv"))

    export_infra = PythonOperator(
        task_id="export_infra",
        python_callable=infra_extract,
        op_kwargs={"copy_sql":
                   """
                    COPY (SELECT * FROM apartment.apartment.infra_view iv)
                    TO STDOUT
                    WITH CSV HEADER;
                    """}
        )

    def logs_func(**kwargs):
        logging.info("-----------------------------------------------")
        logging.info("Дата исполнения: " + kwargs["ds"])
        logging.info("-----------------------------------------------")

    to_logs = PythonOperator(
        task_id="to_logs",
        python_callable=logs_func)

wait_for_loading_data >> export_last_apartment >> export_infra >> to_logs


with DAG(
    dag_id="export_to_csv",
    schedule_interval="@weekly",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["export_to_csv"]
) as dag:

    def hosp_extract(copy_sql):
        pg_hook = PostgresHook.get_hook("conn_postgres")
        logging.info("Exporting query to file")
        pg_hook.copy_expert(copy_sql, filename=csv_data_path("hospitals.csv"))

    export_hospitals = PythonOperator(
        task_id="export_export_hospitals",
        python_callable=hosp_extract,
        op_kwargs={"copy_sql":
                   """
                    COPY (SELECT * FROM apartment.apartment.hospitals_info)
                    TO STDOUT
                    WITH CSV HEADER;
                   """}
    )

    def kind_extract(copy_sql):
        pg_hook = PostgresHook.get_hook("conn_postgres")
        logging.info("Exporting query to file")
        pg_hook.copy_expert(copy_sql,
                            filename=csv_data_path("kindergarten.csv"))

    export_kindergarten = PythonOperator(
        task_id="export_kindergarten",
        python_callable=kind_extract,
        op_kwargs={"copy_sql":
                   """
                    COPY (SELECT * FROM apartment.apartment.kindergarten_info)
                    TO STDOUT
                    WITH CSV HEADER;
                   """}
    )

    def mfc_extract(copy_sql):
        pg_hook = PostgresHook.get_hook("conn_postgres")
        logging.info("Exporting query to file")
        pg_hook.copy_expert(copy_sql,
                            filename=csv_data_path("mfc.csv"))

    export_mfc = PythonOperator(
        task_id="export_mfc",
        python_callable=mfc_extract,
        op_kwargs={"copy_sql":
                   """
                    COPY (SELECT * FROM apartment.apartment.mfc_info)
                    TO STDOUT
                    WITH CSV HEADER;
                   """}
    )

    def schools_extract(copy_sql):
        pg_hook = PostgresHook.get_hook("conn_postgres")
        logging.info("Exporting query to file")
        pg_hook.copy_expert(copy_sql,
                            filename=csv_data_path("schools.csv"))

    export_schools = PythonOperator(
        task_id="export_schools",
        python_callable=schools_extract,
        op_kwargs={"copy_sql":
                   """
                    COPY (SELECT * FROM apartment.apartment.schools_info)
                    TO STDOUT
                    WITH CSV HEADER;
                   """}
    )

    def objects_extract(copy_sql):
        pg_hook = PostgresHook.get_hook("conn_postgres")
        logging.info("Exporting query to file")
        pg_hook.copy_expert(copy_sql,
                            filename=csv_data_path("objects.csv"))

    export_objects = PythonOperator(
        task_id="export_objects",
        python_callable=objects_extract,
        op_kwargs={"copy_sql":
                   """
                    COPY (SELECT * FROM apartment.apartment.objects_info)
                    TO STDOUT
                    WITH CSV HEADER;
                   """}
    )

    def infra_count_extract(copy_sql):
        pg_hook = PostgresHook.get_hook("conn_postgres")
        logging.info("Exporting query to file")
        pg_hook.copy_expert(copy_sql,
                            filename=csv_data_path("infra_count.csv"))

    export_infra_count = PythonOperator(
        task_id="export_infra_count",
        python_callable=infra_count_extract,
        op_kwargs={"copy_sql":
                   """
                    COPY (SELECT * FROM apartment.apartment.infra_count_view)
                    TO STDOUT
                    WITH CSV HEADER;
                   """}
    )

    def logs_func(**kwargs):
        logging.info("-----------------------------------------------")
        logging.info("Дата исполнения: " + kwargs["ds"])
        logging.info("-----------------------------------------------")

    to_logs = PythonOperator(
        task_id="to_logs",
        python_callable=logs_func)

    export_hospitals >> export_kindergarten >> export_schools >> export_mfc >> export_objects >> to_logs
