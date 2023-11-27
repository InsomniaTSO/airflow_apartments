import logging
import csv
from airflow.models import BaseOperator
from airflow.providers.http.hooks.http import HttpHook
from data_path import temp_data_path
from parser_geolocation import GeoDataHook

conn_url = "https://obr.gate.petersburg.ru"
location_url = "/educational_organizations"
fieldnames_list = ("id", "district", "name", "address", "lat", "lon")


class SchoolDataHook(HttpHook):
    """
    Interact with API
    """

    def __init__(self, http_conn_id: str, **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = "GET"

    def get_list(self) -> list:
        return self.run(f"{location_url}").json()


class SchoolDataOperator(BaseOperator):
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
            school = {
                "id": result.get("id"),
                "district": result.get("district"),
                "name": result.get("abbreviated_name"),
                "address": result.get("address"),
                "lat": lat,
                "lon": lon,
            }
            with open(
                temp_data_path("schools.csv"), "a",
                encoding="utf-8", newline=""
            ) as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames_list)
                writer.writerow(school)
        return len(result_json)

    def execute(self, context) -> list:
        hook = SchoolDataHook("obr_data")
        school_data = hook.get_list()
        with open(
            temp_data_path("schools.csv"), "w", newline="", encoding="utf-8"
        ) as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames_list)
            writer.writeheader()
        count = self.get_cleen_data(school_data)
        logging.info("-----------------------------------------------")
        logging.info(f"Загружено {count} результатов")
        logging.info("-----------------------------------------------")
