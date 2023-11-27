from airflow.providers.http.hooks.http import HttpHook

conn_url = "https://geocode.gate.petersburg.ru"
geo_url = "/parse/eas?street="


class GeoDataHook(HttpHook):
    """
    Interact with geocode API
    """
    def __init__(self, http_conn_id: str, **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = "GET"

    def get_coordinates(self, address: str) -> list:
        result = self.run(f"{geo_url}{address}").json()
        lat, lon = 0, 0
        if result.get("error") is None:
            lat, lon = (result.get("Latitude"),
                        result.get("Longitude"))
            return (lat, lon)
        else:
            return (lat, lon)
