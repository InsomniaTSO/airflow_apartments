import logging
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from airflow import DAG

DEFAULT_ARGS = {
    "owner": "t-manakova",
    "poke_interval": 600,
    "retries": 3,
    "start_date": days_ago(1),
}

with DAG(
    dag_id="refresh_view",
    schedule_interval="30 10 * * *",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["refresh_view"],
) as dag:
    refresh_view = PostgresOperator(
        task_id="refresh_view",
        postgres_conn_id="conn_postgres",
        sql="""
              refresh materialized view mv_apartments_for_days;
              refresh materialized view mv_city_districts;
            """,
    )

    def logs_func(**kwargs):
        logging.info("-----------------------------------------------")
        logging.info("Дата исполнения: " + kwargs["ds"])
        logging.info("Материализованные представления обновлены.")
        logging.info("-----------------------------------------------")

    to_logs = PythonOperator(task_id="to_logs", python_callable=logs_func)

    refresh_view >> to_logs
