import csv
import random
import time
import logging
import cfscrape
from bs4 import BeautifulSoup
from apartment_data_transform import Apartment
from data_path import temp_data_path

conn_url = "https://spb.cian.ru/cat.php?deal_type=rent&engine_version=2&offer_type=flat"
settings = "&region=2&room1=1&room2=1&room3=1&room9=1&type=4"
fieldsnames = (
    "info",
    "price",
    "location",
    "district",
    "floor",
    "floors",
    "rooms",
    "area",
    "int_price",
    "link",
    "metro",
    "address",
    "lat",
    "lon",
)


def get_apartment_from_cyan() -> None:
    """Get data from website page by page and
    save it to csv-file"""
    scraper = cfscrape.create_scraper(delay=10)
    page = 1
    current_page = 1
    with open(
        temp_data_path("apartments.csv"), "w", newline="", encoding="utf-8"
    ) as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldsnames)
        writer.writeheader()
    while page < 100:
        response = scraper.get(conn_url + f"&p={page}" + settings)
        if response.status_code == 200 and (
            "https://spb.cian.ru/captcha" not in response.url
        ):
            soup = BeautifulSoup(response.text, "html.parser")
            flat_list = soup.find_all(
                "div", class_="_93444fe79c--container--kZeLu _93444fe79c--link--DqDOy"
            )
            current_page = soup.find(
                "button",
                class_="_93444fe79c--button--Cp1dl _93444fe79c--button--IqIpq _93444fe79c--M--T3GjF _93444fe79c--button--dh5GL",
            ).text
            if current_page == "Назад":
                current_page = 1
            else:
                current_page = int(current_page)
            if current_page == page:
                for flat in flat_list:
                    title = flat.find(
                        "div", class_="_93444fe79c--row--kEHOK"
                    ).select_one("span[data-mark=OfferTitle]")
                    subtitle = flat.find(
                        "div", class_="_93444fe79c--row--kEHOK"
                    ).select_one("span[data-mark=OfferSubtitle]")
                    if subtitle is None:
                        info = title.text
                    else:
                        info = subtitle.text
                    price = (
                        flat.find("div", class_="_93444fe79c--container--aWzpE")
                        .select_one("span[class]")
                        .text
                    )
                    link = flat.find("a", class_="_93444fe79c--link--eoxce").get("href")
                    location = flat.find(
                        "div", class_="_93444fe79c--labels--L8WyJ"
                    ).text
                    apartment = Apartment(info, price, link, location).__dict__
                    with open(
                        temp_data_path("apartments.csv"),
                        "a",
                        encoding="utf-8",
                        newline="",
                    ) as csvfile:
                        writer = csv.DictWriter(csvfile, fieldnames=fieldsnames)
                        writer.writerow(apartment)
            else:
                break
        else:
            logging.error(f"Ошибка доступа к сайту {response.status_code}")
            break
        time_sleep = random.uniform(1, 3)
        time.sleep(time_sleep)
        page += 1
    return
