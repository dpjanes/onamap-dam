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

    def run(self):
        import oms_actions

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
        for in_record in in_records:
            print("---")
            in_identifier = in_record["identifier"]

            ex_record = ex_recordd.get(in_identifier)
            ## print("lookup", in_identifier, (ex_record or {}).get("id"))
            if ex_record:
                in_record["id"] = ex_record["id"]

            if not ex_record or in_record != ex_record:
                cook(in_record)

                if (in_record["type"] == "Image") and ("url" in in_record) and ("data" not in in_record):
                    image = oms_helpers.load_url(in_record["url"], cache=self.cache)
                    in_record.update(image)

                response = oms_actions.ObjectEnsure(self.context, self.user, in_record)
                response_record = response["object"]
                pprint.pprint(response_record)

                in_record["id"] = response_record["id"]
                ex_recordd[in_identifier] = response_record


        ## pprint.pprint(api.d)


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

