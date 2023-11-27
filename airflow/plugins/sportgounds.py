import logging
import math
import csv
from airflow.models import BaseOperator
from airflow.providers.http.hooks.http import HttpHook
from data_path import temp_data_path

conn_url = "https://yazzh.gate.petersburg.ru"
location_url = "/sportgrounds/"
fieldnames_list = (
    "id",
    "district",
    "name",
    "categories",
    "season",
    "address",
    "lat",
    "lon",
)
seasons = ["Зима", "Лето"]


class SportgroundsDataHook(HttpHook):
    """
    Interact with API
    """

    def __init__(self, http_conn_id: str, **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = "GET"

    def get_page_count(self, per_page: int, season: str) -> int:
        page_count = math.ceil(
            self.run(f"{location_url}?season={season}&page=1&count=10").json()["count"]
            / per_page
        )
        return page_count

    def get_page(self, page: int, per_page: int, season: str) -> list:
        return self.run(
            f"{location_url}?season={season}&page={page}&count={per_page}"
        ).json()["data"]


class SportgroundsDataOperator(BaseOperator):
    """
    Select important data
    """

    def __init__(self, endpoint_url: str = "", per_page: int = 100, **kwargs):
        super().__init__(**kwargs)
        self.endpoint_url = endpoint_url
        self.per_page = per_page

    def get_cleen_data(self, result_json: list, season: str) -> int:
        for place in result_json:
            result = place.get("place")
            try:
                lat = result.get("coordinates")[0]
                lon = result.get("coordinates")[1]
            except (TypeError, IndexError):
                lat = 0
                lon = 0
            sportground = {
                "id": result.get("id"),
                "district": result.get("district").split()[0],
                "name": result.get("name"),
                "categories": result.get("categories"),
                "season": season,
                "address": result.get("address"),
                "lat": lat,
                "lon": lon,
            }
            with open(
                temp_data_path("sportgrounds.csv"), "a",
                encoding="utf-8", newline=""
            ) as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames_list)
                writer.writerow(sportground)
        return len(result_json)

    def execute(self, context):
        count = 0
        hook = SportgroundsDataHook("objects_data")
        with open(
            temp_data_path("sportgrounds.csv"), "w",
            newline="", encoding="utf-8"
        ) as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames_list)
            writer.writeheader()
        for season in seasons:
            page_count = hook.get_page_count(self.per_page, season)
            for page in range(1, page_count + 1):
                page_result = hook.get_page(page, self.per_page, season)
                count_per_page = self.get_cleen_data(page_result, season)
                count += count_per_page
            logging.info("-----------------------------------------------")
            logging.info(f"Загружено {count} объектов {season}")
            logging.info("-----------------------------------------------")
