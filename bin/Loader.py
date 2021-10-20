import os
import re
import sys
import pprint
import hashlib
import base64
import datetime
import argparse

import yaml

__folder__ = os.path.dirname(__file__)

class Loader:
    def __init__(self, destination, filename):
        self.filename_in = filename

        self.folder_db = os.path.expanduser(f"~/.onamap/dam/{destination}")
        if not os.path.isdir(self.folder_db):
            os.makedirs(self.folder_db)

        self.filename_db = os.path.join(self.folder_db, "events.yaml")
        self.db = {
            "records": [],
        }

    def run(self):
        self._load_existing()
        self._load_new()
        self._load_new()

    def _load_existing(self):
        try:
            with open(self.filename_db, "rb") as fin:
                self.db = yaml.safe_load(fin)
        except IOError:
            pass

    def _load_new(self):
        with open(self.filename_in, "rb") as fin:
            records = yaml.safe_load(fin)
            for record in records:
                try: del record["id"]
                except KeyError: pass

        old_identifiers = set([ record["identifier"] for record in self.db["records"] ])
        new_identifiers = set([ record["identifier"] for record in records ])

        del_identifiers = old_identifiers - new_identifiers
        print("deleted", del_identifiers)

    def _process(self):
        for record in self.db["records"]:
            if "id" in record:
                continue

            actions.dispatch({
                "action": "Ensure",
                "subject": "record",
            })


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='load event data')
    parser.add_argument('--destination', help='name of destination', required=True)
    parser.add_argument('filename', help='file to load')
    args = parser.parse_args()

    p = Loader(destination=args.destination, filename=args.filename)
    p.run()

