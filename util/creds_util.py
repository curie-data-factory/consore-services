import json

from util.util import ROOT_PATH


def get_creds(creds_name):
    path_creds = ROOT_PATH + "/creds/" + creds_name
    with open(path_creds, encoding="utf-8") as vault_file:
        return json.load(vault_file)
