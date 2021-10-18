import os
import re
import sys
import pprint
import hashlib
import base64
import datetime
import argparse

import pytz
import yaml
from markdownify import markdownify

__folder__ = os.path.dirname(__file__)

TZ = "America/Halifax"
TZ_LOCAL = pytz.timezone(TZ)
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
    def __init__(self, n=None):
        self.folder = os.path.join(__folder__, "pyd")
        self.dorder = []
        self.d = {}
        self.n = n

    def run(self):
        with open(os.path.join(self.folder, "pei.search.events.pyd")) as fin:
            records = eval(fin.read())

        if self.n is not None:
            records = records[:self.n]

        for record in records:
            self.cook_one(record)

        yaml.dump([ self.d[identifier] for identifier in self.dorder ], sys.stdout)

    def cook_one(self, record):
        record_id = record["id"]

        images = []
        for photo in record.get("photos", []):
            image = {
                "type": "Image",
                "identifier": hash(photo),
                "url": photo,
            }
            self.add(image)
            images.append({
                "type": "Image",
                "identifier": image["identifier"],
            })

        item = {
            "type": "Item",
            "identifier": hash(f"item-{record_id}"),
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
            "identifier": hash(f"location-{record_id}"),
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
            "identifier": hash(f"offer-{record_id}-{event_start}-{event_end}"),
            "name": record["title"],
            "period": {
                "tz": TZ,
                "start": event_start,
                "end": event_end,
                "all_day": True,
            },
            "location": {
                "type": "Location",
                "identifier": location["identifier"],
            },
            "item": {
                "type": "Item",
                "identifier": item["identifier"],
            },
        }
        self.add(offer)

    def add(self, item):
        identifier = item["identifier"]
        if identifier in self.d:
            return

        self.dorder.append(identifier)
        self.d[identifier] = denull(item)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='parse event data')
    parser.add_argument('--n', help='max number of records', default=None, type=int)
    args = parser.parse_args()

    p = Processor(n=args.n)
    p.run()
