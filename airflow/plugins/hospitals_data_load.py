import logging
import math
import csv
from airflow.models import BaseOperator
from airflow.providers.http.hooks.http import HttpHook
from data_path import temp_data_path

conn_url = "https://spb-classif.gate.petersburg.ru"
location_url = "/api/v2/datasets/149/versions/latest/data/327/"
fieldnames_list = ("id", "district", "name", "type", "address", "lat", "lon")


class HospitalsDataHook(HttpHook):
    """
    Interact with API
    """

    def __init__(self, http_conn_id: str, **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = "GET"

    def get_page_count(self, per_page: int) -> int:
        page_count = math.ceil(
            self.run(f"{location_url}?page=1&per_page=10").json()["count"]/per_page
        )
        return page_count

    def get_page(self, page: int, per_page: int) -> list:
        return self.run(f"{location_url}?page={page}&per_page={per_page}").json()[
            "results"
        ]


class HospitalsDataOperator(BaseOperator):
    """
    Select important data
    """

    def __init__(self, endpoint_url: str = "", per_page: int = 100, **kwargs):
        super().__init__(**kwargs)
        self.endpoint_url = endpoint_url
        self.per_page = per_page

    def get_cleen_data(self, result_json: list) -> int:
        for result in result_json:
            try:
                lat = result.get("coordinates")[0]
                lon = result.get("coordinates")[1]
            except TypeError:
                lat = 0
                lon = 0
            hospital = {
                "id": result.get("number"),
                "district": result.get("district"),
                "name": result.get("name"),
                "type": result.get("type"),
                "address": result.get("address"),
                "lat": lat,
                "lon": lon,
            }
            with open(
                temp_data_path("hospitals.csv"), "a",
                encoding="utf-8", newline=""
            ) as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames_list)
                writer.writerow(hospital)
        return len(result_json)

    def execute(self, context):
        count = 0
        hook = HospitalsDataHook("hospital_data")
        page_count = hook.get_page_count(self.per_page)
        with open(
            temp_data_path("hospitals.csv"), "w", newline="", encoding="utf-8"
        ) as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames_list)
            writer.writeheader()
        for page in range(1, page_count + 1):
            page_result = hook.get_page(page, self.per_page)
            count_per_page = self.get_cleen_data(page_result)
            count += count_per_page
        logging.info("-----------------------------------------------")
        logging.info(f"Загружено {count} результатов")
        logging.info("-----------------------------------------------")
