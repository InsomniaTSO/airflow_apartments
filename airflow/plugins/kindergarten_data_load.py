import logging
import csv
from airflow.models import BaseOperator
from airflow.providers.http.hooks.http import HttpHook
from data_path import temp_data_path
from parser_geolocation import GeoDataHook

conn_url = "https://edu-prc.gate.petersburg.ru"
location_url = "/preschool_educational_organizations"
fieldnames_list = ("id", "district", "name", "address", "lat", "lon")


class KindergartenDataHook(HttpHook):
    """
    Interact with API
    """
    def __init__(self, http_conn_id: str, **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = "GET"

    def get_list(self) -> list:
        return self.run(f"{location_url}").json()


class KindergartenDataOperator(BaseOperator):
    """
    Select important data
    """
    def __init__(self, endpoint_url: str = "", **kwargs):
        super().__init__(**kwargs)
        self.endpoint_url = endpoint_url

    def get_cleen_data(self, result_json: list) -> int:
        for result in result_json:
            geo_hook = GeoDataHook("geo_data")
            lat, lon = geo_hook.get_coordinates(result.get("address"))
            kindergarten = {
                "id": result.get("id"),
                "district": result.get("district"),
                "name": result.get("abbreviated_name"),
                "address": result.get("address"),
                "lat": lat,
                "lon": lon,
            }
            with open(temp_data_path("kindergartens.csv"), "a", encoding="utf-8",
                      newline="") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames_list)
                writer.writerow(kindergarten)
        return len(result_json)

    def execute(self, context):
        hook = KindergartenDataHook("kind_data")
        kindergarten_data = hook.get_list()
        with open(temp_data_path("kindergartens.csv"), "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames_list)
            writer.writeheader()
        count = self.get_cleen_data(kindergarten_data)
        logging.info("-----------------------------------------------")
        logging.info(f"Загружено {count} результатов")
        logging.info("-----------------------------------------------")
