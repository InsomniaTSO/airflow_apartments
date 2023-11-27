from parser_geolocation import GeoDataHook


class Apartment(object):
    """Transform data from webpage"""
    def __init__(self, info, price, link, location):
        self.info = info
        self.price = price
        self.location = location
        self.district = self.get_district()
        self.floor = self.get_floor()
        self.floors = self.get_floors()
        self.rooms = self.get_rooms()
        self.area = self.get_area()
        self.int_price = self.get_int_price()
        self.link = link
        self.metro = self.get_metro()
        self.address = self.get_address()
        self.lat, self.lon = self.get_coordinates()

    def get_district(self):
        data = self.location.split(", ")
        district = " "
        for item in data:
            if "р-н " in item:
                district = " ".join(item.split()[1:])
        return district

    def get_floor(self):
        try:
            data = self.info.split(", ")[-1].split()[0]
            data = int(data.split("/")[0])
        except (IndexError, ValueError):
            data = 0
        return data

    def get_floors(self):
        try:
            data = self.info.split(", ")[-1].split()[0]
            data = int(data.split("/")[1])
        except (IndexError, ValueError):
            data = 0
        return data

    def get_rooms(self):
        try:
            data = self.info.split(", ")[0].split()[0]
        except (IndexError, ValueError):
            data = " "
        return data

    def get_area(self):
        try:
            data = self.info.split(", ")[1]
            data = float(data.split()[0].replace(",", "."))
        except (IndexError, ValueError):
            data = " "
        return data

    def get_int_price(self):
        try:
            int_price = int("".join(self.price.split()[:-1]))
        except (IndexError, ValueError):
            int_price = 0
        return int_price

    def get_metro(self):
        data = self.location.split(", ")
        metro = " "
        for item in data:
            if "м. " in item:
                metro = " ".join(item.split()[1:])
        return metro

    def get_address(self):
        try:
            building = self.location.split(", ")[-1]
            street = self.location.split(", ")[-2]
            building_new = ""
            for i in list(building):
                if i.isdigit() or i in ["-", "/"]:
                    building_new += i
                else:
                    building_new += " "
                    building_new += i
            address = street + ", " + building_new
        except (IndexError, ValueError):
            address = " "
        return address

    def get_coordinates(self):
        geo_hook = GeoDataHook("geo_data")
        lat, lon = geo_hook.get_coordinates(self.address)
        return lat, lon
