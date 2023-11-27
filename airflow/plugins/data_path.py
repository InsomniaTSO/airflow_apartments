from pathlib import Path


def temp_data_path(name: str) -> Path:
    csv_path = Path.joinpath(Path(__file__).parents[2], name)
    return csv_path


def csv_data_path(name: str) -> Path:
    csv_path = Path.joinpath(Path(__file__).parents[2], "csv_export", name)
    return csv_path
