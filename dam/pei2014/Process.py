import os
import sys
import pprint
import hashlib
import base64
import yaml

__folder__ = os.path.dirname(__file__)

def hash(s):
    hashed = hashlib.md5(s.encode())
    digest = hashed.digest()
    encoded = base64.b64encode(digest, b"-_")[:-2]
    encoded = encoded.decode("ascii")

    return encoded

def denull(d):
    for key, value in list(d.items()):
        if value is None:
            del d[key]

    return d

class Processor:
    def __init__(self):
        self.folder = os.path.join(__folder__, "pyd")
        self.dorder = []
        self.d = {}

    def run(self):
        with open(os.path.join(self.folder, "pei.search.events.pyd")) as fin:
            records = eval(fin.read())

        for record in records:
            self.cook_one(record)

        yaml.dump([ self.d[client_id] for client_id in self.dorder ], sys.stdout)

    def cook_one(self, record):
        record_id = record["id"]

        item = {
            "type": "Item",
            "client_id": hash(f"item-{record_id}"),
            "name": record.get("title"),
            "description": record.get("content"),
            "url": record.get("url"),
            "email": record.get("email"),
            "phone": record.get("phone"),
        }
        self.add(item)

        location = {
            "type": "Location",
            "client_id": hash(f"location-{record_id}"),
            "street": record.get("street_address"),
            "locality": record.get("locality"),
            "region": record.get("region_state"),
            "country": record.get("country_name"),
            "latitude": record.get("lat"),
            "longitude": record.get("lon"),
        }
        self.add(location)

        event_start = record.get("event_start")
        event_end = record.get("event_end")
        offer = {
            "type": "Offer",
            "client_id": hash(f"offer-{record_id}-{event_start}-{event_end}"),
            "name": record["title"],
            "date_start": event_start,
            "date_end": event_end,
            "location": {
                "type": "Location",
                "client_id": location["client_id"],
            },
            "item": {
                "type": "Item",
                "client_id": item["client_id"],
            },
        }
        self.add(offer)

    def add(self, item):
        client_id = item["client_id"]
        if client_id in self.d:
            return

        self.dorder.append(client_id)
        self.d[client_id] = denull(item)
        
        """
 'guid': '4823920bcebfd000fe4a0cbeba4f53f0',
 'id': 20037603,
 'phone': '902-629-1864',
 'photos': ['https://www.tourismpei.com/search/assets/images/common/37603-1630413734.jpg'],
 'theme': ['Events'],
        """


if __name__ == '__main__':
    p = Processor()
    p.run()
