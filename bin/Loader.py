#
#   bin/Loader.py
#
#   David Janes
#   Onamap
#   2021-10-20
#
#   Load a Destination's data into the servers
#

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

import logging
logger = logging.getLogger(__name__)

__folder__ = os.path.dirname(__file__)

class Loader:
    def __init__(self, destination, filename, context, user):
        self.filename_in = filename
        self.context = context
        self.user = user

        self.folder_db = os.path.expanduser(f"~/.onamap/dam/{destination}")
        if not os.path.isdir(self.folder_db):
            os.makedirs(self.folder_db)

        self.cache = context.get("media.cache", expanduser=True)

        self.filename_db = os.path.join(self.folder_db, "events.yaml")
        self.db = {
            "records": [],
        }

    def _cook(self, d, depth=0):
        ## print("COOK", d)
        if isinstance(d, dict):
            identifier = d.get("identifier")
            if identifier:
                ex_record = self.ex_recordd.get(identifier)
                if depth and not ex_record:
                    pprint.pprint(("XXX", identifier, self.ex_recordd, ex_record))
                    print("FAILED", d)
                    sys.exit(1)
                    
                if ex_record:
                    d["id"] = ex_record["id"]
                    del d["identifier"]

            for key, value in d.items():
                self._cook(value, depth=depth+1)
        elif isinstance(d, list):
            for value in d:
                self._cook(value, depth=depth+1)

    def run(self):
        L = "Loader.run"

        import oms_actions

        self.db_start()

        ## new records
        with open(self.filename_in, "rb") as fin:
            in_records = yaml.safe_load(fin)
            for in_record in in_records:
                try: del in_record["id"]
                except KeyError: pass

        in_records = in_records
        in_identifiers = set([ record["identifier"] for record in in_records ])

        ## update the database
        for in_record in in_records:
            print("---")
            in_identifier = in_record["identifier"]

            ex_record = self.ex_recordd.get(in_identifier)
            if ex_record:
                in_record["id"] = ex_record["id"]

            if ex_record and ex_record == in_record:
                print(f"{L}: unchanged: {in_record['id']}")
                continue

            if ex_record:
                print(f"{L}: changed: {in_record['id']}")
                print(ex_record)
                print(in_record)
                sys.exit(1)
            else:
                print(f"{L}: new")

            self._cook(in_record)

            deletes = []
            if (in_record["type"] == "Image") and ("url" in in_record) and ("data" not in in_record):
                image = oms_helpers.load_url(in_record["url"], cache=self.cache)
                if image and "data" in image:
                    in_record["data"] = image["data"]
                    deletes.append("data")

            ## pprint.pprint(in_record)
            response = oms_actions.ObjectEnsure(self.context, self.user, in_record)
            response_record = response["object"]
            ## pprint.pprint(response_record)

            for key in deletes:
                try: del in_record[key]
                except KeyError: pass

            in_record["id"] = response_record["id"]
            self.ex_recordd[in_identifier] = response_record

        ## pprint.pprint(api.d)
        self.db["records"] = in_records

        self.db_end()

    def db_start(self):
        ## existing records
        try:
            with open(self.filename_db, "rb") as fin:
                self.db = yaml.safe_load(fin)
        except IOError:
            pass

        self.db = self.db or {}
        self.db.setdefault("records", [])

        self.ex_records = self.db["records"]
        self.ex_recordd = {}
        for ex_record in self.ex_records:
            self.ex_recordd[ex_record["identifier"]] = ex_record

        self.ex_identifiers = set([ record["identifier"] for record in self.ex_records ])

    def db_end(self):
        ## save
        with open(self.filename_db, "w") as fout:
            yaml.dump(self.db, fout)

if __name__ == '__main__':
    import oms_helpers

    parser = argparse.ArgumentParser(description='load event data')
    parser.add_argument('--destination', help='name of destination', required=True)
    parser.add_argument("--config", help="configuration file", default=oms_helpers.DEFAULT_CONFIG)
    parser.add_argument("--debug", help="show debugging", action="store_true")
    parser.add_argument('filename', help='file to load')
    args = parser.parse_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    import oms_context
    import oms_actions

    context = oms_context.Context(config=args.config)

    result = oms_actions.UserGet(context, user=None, subject={
        "type": "Provider",
        "provider": "account",
        "provider_code": args.destination,
    })
    user = result["object"]
    if not user:
        raise ValueError(f"no user has been created for destination={args.destination}")

    p = Loader(
        destination=args.destination, 
        filename=args.filename, 
        context=context, 
        user=user,
    )
    p.run()

