import argparse

from app import run

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Read Datasets.")
    parser.add_argument("--claims", required=True, help="Path to the claims folder")
    parser.add_argument("--reverts", required=True, help="Path to the reverts folder")
    parser.add_argument("--pharmacies", required=True, help="Path to the pharmacies folder")

    args = parser.parse_args()
    run(args.claims, args.reverts, args.pharmacies)