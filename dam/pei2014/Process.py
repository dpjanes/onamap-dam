import os
import re
import sys
import pprint
import hashlib
import base64
import datetime

import pytz
import yaml
from markdownify import markdownify

__folder__ = os.path.dirname(__file__)

TZ_LOCAL = pytz.timezone("America/Halifax")
TZ_UTC = pytz.timezone("UTC")

def normalize_date(s_local, eod=False):
    if s_local is None:
        return 

    if eod:
        s_local += "T23:59:59"

    dt_local = TZ_LOCAL.localize(datetime.datetime.fromisoformat(s_local))
    dt_utc = dt_local.astimezone(TZ_UTC)
    s_utc = dt_utc.isoformat()
    s_utc = re.sub("[+].*$", "Z", s_utc)

    return s_utc

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

def scrub_content(text):
    if text is None:
        return

    text = markdownify(text)
    text = re.sub("#### Region.*$", "", text, flags=re.DOTALL|re.MULTILINE)
    text = re.sub("\n+", "\n", text, flags=re.DOTALL|re.MULTILINE)
    text = text.strip()

    return text

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

        images = []
        for photo in record.get("photos", []):
            image = {
                "type": "Image",
                "client_id": hash(photo),
                "url": photo,
            }
            self.add(image)
            images.append({
                "type": "Image",
                "client_id": image["client_id"],
            })

        item = {
            "type": "Item",
            "client_id": hash(f"item-{record_id}"),
            "name": record.get("title"),
            "description": scrub_content(record.get("content")),
            "url": record.get("url"),
            "email": record.get("email"),
            "phone": record.get("phone"),
            "images": images or None,
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

        event_start = normalize_date(record.get("event_start"))
        event_end = normalize_date(record.get("event_end"), eod=True)
        offer = {
            "type": "Offer",
            "client_id": hash(f"offer-{record_id}-{event_start}-{event_end}"),
            "name": record["title"],
            "period": {
                "start": event_start,
                "end": event_end,
                "all_day": True,
            },
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

if __name__ == '__main__':
    p = Processor()
    p.run()
