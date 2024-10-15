from models.pharmacies import Pharmacies
from models.claims import Claims
from models.reverts import Reverts
from utils.readers import read_json, read_csv


def read_pharmacies(path) -> Pharmacies:
    k = {'pharmacies': read_csv(path)}
    return Pharmacies(**k)


def read_reverts(path) -> Reverts:
    k = {'reverts': read_json(path)}
    return Reverts(**k)


def read_claims(path) -> Claims:
    k = {'claims': read_json(path)}
    return Claims(**k)


def run(claims_path: str, reverts_path: str, pharmacies_path: str):
    print(f"Reading from: {claims_path}, {reverts_path}, {pharmacies_path}")
    claims = read_claims(claims_path)
    reverts = read_reverts(reverts_path)
    pharmacies = read_pharmacies(pharmacies_path)
