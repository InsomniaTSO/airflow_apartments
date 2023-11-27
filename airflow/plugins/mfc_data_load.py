import logging
import re
import csv
from airflow.models import BaseOperator
from airflow.providers.http.hooks.http import HttpHook
from data_path import temp_data_path
from const import DICT_DISTRICRS

conn_url = "https://yazzh.gate.petersburg.ru/mfc/all/"
fieldnames_list = ("id", "district_id", "name", "active",
                   "address", "lat", "lon")


class MFCDataHook(HttpHook):
    """
    Interact with API
    """

    def __init__(self, http_conn_id: str, **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = "GET"

    def get_list(self) -> list:
        return self.run().json()


class MFCDataOperator(BaseOperator):
    """
    Select important data
    """

    def __init__(self, endpoint_url: str = "", per_page: int = 100, **kwargs):
        super().__init__(**kwargs)
        self.endpoint_url = endpoint_url
        self.per_page = per_page

    def get_cleen_data(self, result_json: list) -> int:
        counter = 0
        for result in result_json:
            if re.match(r"МФЦ\s+\w+\s+района\s+\w+.+",
                        result.get("name")) is None:
                continue
            try:
                lat = result.get("coordinates")[0]
                lon = result.get("coordinates")[1]
            except TypeError:
                lat = 0
                lon = 0
            name = result.get("name")
            district = DICT_DISTRICRS.get(
                list(filter(
                        lambda x: (
                            name.split()[1][:5] in x,
                            list(DICT_DISTRICRS.keys()),
                        )
                    )
                )[0],
                0,
            )
            mfc = {
                "id": result.get("id"),
                "district_id": district,
                "name": result.get("name"),
                "active": result.get("active"),
                "address": result.get("address"),
                "lat": lat,
                "lon": lon,
            }
            with open(
                temp_data_path("mfc.csv"), "a", encoding="utf-8", newline=""
            ) as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames_list)
                writer.writerow(mfc)
            counter += 1
        return counter

    def execute(self, context):
        hook = MFCDataHook("mfc_data")
        mfc_data = hook.get_list()["data"]
        with open(
            temp_data_path("mfc.csv"), "w", newline="", encoding="utf-8"
        ) as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames_list)
            writer.writeheader()
        count = self.get_cleen_data(mfc_data)
        logging.info("-----------------------------------------------")
        logging.info(f"Загружено {count} результатов")
        logging.info("-----------------------------------------------")
