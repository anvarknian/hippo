from collections import defaultdict

from pyspark.sql import Window
from pyspark.sql import functions as F

from models.claims import Claims
from models.pharmacies import Pharmacies
from models.reverts import Reverts
from spark_proc import create_dataframe, claims_schema
from spark_proc import pharmacies_schema, reverts_schema
from utils.readers import read_claims, read_pharmacies, read_reverts
from utils.writers import save_to_json

OUTPUT_PATH = './output'


def filter_pharmacy_events(p: Pharmacies, c: Claims):
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

    # 1 - Read data stored in JSON files + Validation
    claims = read_claims(claims_path)
    reverts = read_reverts(reverts_path)
    pharmacies = read_pharmacies(pharmacies_path)

    # Filter metrics
    valid_claims = filter_pharmacy_events(pharmacies, claims)

    # 2 - Calculate metrics for some dimensions
    metrics = compute_metrics(valid_claims, reverts)
    save_to_json(f'{OUTPUT_PATH}/metrics-2.json', metrics)

    # Spark Data
    claims_dict = claims.model_dump(mode='json').get('claims')
    pharmacies_dict = pharmacies.model_dump(mode='json').get('pharmacies')
    reverts_dict = reverts.model_dump(mode='json').get('reverts')
    claims_df = create_dataframe(claims_dict, claims_schema)
    pharmacies_df = create_dataframe(pharmacies_dict, pharmacies_schema)
    reverts_df = create_dataframe(reverts_dict, reverts_schema)
    valid_claims_df = claims_df.join(
        reverts_df, claims_df.id == reverts_df.claim_id, "left_anti"
    )
    claims_with_chain_df = valid_claims_df.join(
        pharmacies_df, valid_claims_df.npi == pharmacies_df.npi, "inner"
    ).select(valid_claims_df["*"], pharmacies_df.chain).cache()

    # 3 -  Make a recommendation for the top 2 Chain to be displayed for each Drug.
    avg_price_df = claims_with_chain_df.groupBy("ndc", "chain") \
        .agg(F.avg((F.col("price") / F.col("quantity"))).alias("avg_price")) \
        .orderBy("ndc", "avg_price")

    top_chains_df = avg_price_df.withColumn("rank", F.row_number().over(
        Window.partitionBy("ndc").orderBy("avg_price"))) \
        .filter(F.col("rank") <= 2) \
        .select("ndc", "chain", "avg_price")

    final_df = top_chains_df.groupBy("ndc") \
        .agg(F.collect_list(F.struct(
        F.col("chain").alias("name"),
        F.col("avg_price")
    )).alias("chain"))
    final_df.write.mode("overwrite").json(f'{OUTPUT_PATH}/metrics-3.json')

    # 4 - Understand Most common quantity prescribed for a given Drug
    quantity_counts_df = claims_with_chain_df.groupBy("ndc", "quantity") \
        .count() \
        .orderBy("ndc", F.desc("count"))
    most_common_quantities_df = quantity_counts_df.groupBy("ndc") \
        .agg(F.collect_list("quantity").alias("most_prescribed_quantity"))
    most_common_quantities_df.write.mode("overwrite").json(f'{OUTPUT_PATH}/metrics-4.json')
