import os
import re
import sys
import pprint
import hashlib
import base64
import datetime
import argparse
import ulid

import yaml

__folder__ = os.path.dirname(__file__)

class API:
    def __init__(self):
        self.d = {}

    def ObjectEnsure(self, in_record):
        in_identifier = in_record["identifier"]
        ex_record = self.d.get(in_identifier)
        if not ex_record:
            ex_record = dict(in_record)
            ex_record["id"] = str(ulid.ULID())
            self.d[in_identifier] = ex_record

        return {
            "object": ex_record,
        }


api = API()

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
        ## existing records
        try:
            with open(self.filename_db, "rb") as fin:
                self.db = yaml.safe_load(fin)
        except IOError:
            pass

        ex_records = self.db["records"]
        ex_recordd = {}
        for ex_record in ex_records:
            ex_recordd[ex_record["identifier"]] = ex_record

        ex_identifiers = set([ record["identifier"] for record in ex_records ])

        ## new records
        with open(self.filename_in, "rb") as fin:
            in_records = yaml.safe_load(fin)
            for in_record in in_records:
                try: del in_record["id"]
                except KeyError: pass

        in_identifiers = set([ record["identifier"] for record in in_records ])

        ## update the database
        ## with onamap.actions.api() as api:
        if True:
            for in_record in in_records:
                in_identifier = in_record["identifier"]

                ex_record = ex_recordd.get(in_identifier)
                if ex_record:
                    in_record["id"] = ex_record["id"]

                if not ex_record or in_record != ex_record:
                    response = api.ObjectEnsure(in_record)
                    response_record = response["object"]

                    in_record["id"] = response_record["id"]

        pprint.pprint(api.d)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='load event data')
    parser.add_argument('--destination', help='name of destination', required=True)
    parser.add_argument('filename', help='file to load')
    args = parser.parse_args()

    p = Loader(destination=args.destination, filename=args.filename)
    p.run()

