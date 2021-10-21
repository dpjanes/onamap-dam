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
        del in_record["identifier"]

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

        in_records = in_records
        in_identifiers = set([ record["identifier"] for record in in_records ])

        def cook(d, depth=0):
            if isinstance(d, dict):
                ## print("COOK", d)

                identifier = d.get("identifier")
                if identifier:
                    ex_record = ex_recordd.get(identifier)
                    if depth and not ex_record:
                        pprint.pprint(("XXX", identifier, ex_recordd, ex_record))
                        print("FAILED", d)
                        sys.exit(1)
                        
                    
                    if ex_record:
                        d["id"] = ex_record["id"]
                        del d["identifier"]

                for key, value in d.items():
                    cook(value, depth=depth+1)
            elif isinstance(d, list):
                for value in d:
                    cook(value, depth=depth+1)

        ## update the database
        ## with onamap.actions.api() as api:
        if True:
            for in_record in in_records:
                print("---")
                in_identifier = in_record["identifier"]

                ex_record = ex_recordd.get(in_identifier)
                ## print("lookup", in_identifier, (ex_record or {}).get("id"))
                if ex_record:
                    in_record["id"] = ex_record["id"]

                if not ex_record or in_record != ex_record:
                    cook(in_record)

                    response = api.ObjectEnsure(in_record)
                    response_record = response["object"]
                    ## print("RR", response_record)

                    in_record["id"] = response_record["id"]
                    ex_recordd[in_identifier] = response_record

                    pprint.pprint(response_record)

        ## pprint.pprint(api.d)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='load event data')
    parser.add_argument('--destination', help='name of destination', required=True)
    parser.add_argument('filename', help='file to load')
    args = parser.parse_args()

    p = Loader(destination=args.destination, filename=args.filename)
    p.run()

