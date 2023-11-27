import logging
import math
import csv
from airflow.models import BaseOperator
from airflow.providers.http.hooks.http import HttpHook
from data_path import temp_data_path
from const import OBJECT_TYPES

conn_url = "https://yazzh.gate.petersburg.ru"
location_url = "/ogs/object/"
fieldnames_list = ("id", "district", "type_id", "type_name")


class ObjectsDataHook(HttpHook):
    """
    Interact with API
    """

    def __init__(self, http_conn_id: str, **kwargs):
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = "GET"

    def get_page_count(self, per_page: int, type_id: int) -> int:
        page_count = math.ceil(
            self.run(f"{location_url}?type={type_id}&page=1&Count=10").json()[
                "totalCount"
            ]
            / per_page
        )
        return page_count

    def get_page(self, page: int, per_page: int, type_id: int) -> list:
        return self.run(
            f"{location_url}?type={type_id}&page={page}&Count={per_page}"
        ).json()["objects"]


class ObjectsDataOperator(BaseOperator):
    """
    Select important data
    """

    def __init__(self, endpoint_url: str = "",
                 per_page: int = 100, **kwargs):
        super().__init__(**kwargs)
        self.endpoint_url = endpoint_url
        self.per_page = per_page

    def get_cleen_data(self, result_json: list, type_id: int) -> int:
        for result in result_json:
            object = {
                "id": result.get("id"),
                "district": result.get("districts"),
                "type_id": type_id,
                "type_name": result.get("type_name"),
            }
            with open(
                temp_data_path("objects.csv"), "a", encoding="utf-8", newline=""
            ) as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames_list)
                writer.writerow(object)
        return len(result_json)

    def execute(self, context):
        count = 0
        hook = ObjectsDataHook("objects_data")
        with open(
            temp_data_path("objects.csv"), "w", newline="", encoding="utf-8"
        ) as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames_list)
            writer.writeheader()
        for type_id in OBJECT_TYPES.keys():
            page_count = hook.get_page_count(self.per_page, type_id)
            for page in range(1, page_count + 1):
                page_result = hook.get_page(page, self.per_page, type_id)
                count_per_page = self.get_cleen_data(page_result, type_id)
                count += count_per_page
            logging.info("-----------------------------------------------")
            logging.info(f"Загружено {count} объектов {OBJECT_TYPES[type_id]}")
            logging.info("-----------------------------------------------")
