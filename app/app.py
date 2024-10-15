from collections import defaultdict

from utils.readers import read_claims, read_pharmacies, read_reverts
from models.pharmacies import Pharmacies
from models.claims import Claims
from models.reverts import Reverts



def filter_pharmacy_events(p: Pharmacies, c: Claims):
    """Filter out Claims that belong to valid Pharmacies (by NPI)."""
    valid_pharmacies = {pharmacy.npi for pharmacy in p.pharmacies}
    filtered_claims = [cm for cm in c.claims if cm.npi in valid_pharmacies]
    return Claims(**{'claims': filtered_claims})


def compute_metrics(c: Claims, r: Reverts):
    metrics = defaultdict(lambda: {
        "fills": 0,
        "reverted": 0,
        "total_price": 0.0,
        "total_quantity": 0
    })

    reverted_claims = {revert.claim_id for revert in r.reverts}

    for claim in c.claims:
        npi, ndc = claim.npi, claim.ndc
        quantity, price = claim.quantity, claim.price

        key = (npi, ndc)
        metrics[key]["fills"] += 1
        metrics[key]["total_price"] += price
        metrics[key]["total_quantity"] += quantity

        if claim.id in reverted_claims:
            metrics[key]["reverted"] += 1

    results = []
    for (npi, ndc), data in metrics.items():
        avg_price = data["total_price"] / data["total_quantity"] if data["total_quantity"] > 0 else 0.0
        results.append({
            "npi": npi,
            "ndc": ndc,
            "fills": data["fills"],
            "reverted": data["reverted"],
            "avg_price": round(avg_price, 2),
            "total_price": round(data["total_price"], 2)
        })

    return results

def run(claims_path: str, reverts_path: str, pharmacies_path: str):
    print(f"Reading from: {claims_path}, {reverts_path}, {pharmacies_path}")
    #Read/Validation of data.
    claims = read_claims(claims_path)
    reverts = read_reverts(reverts_path)
    pharmacies = read_pharmacies(pharmacies_path)

    #Compute metrics
    valid_claims = filter_pharmacy_events(pharmacies,claims)
    metrics = compute_metrics(valid_claims, reverts)
    print(metrics)