"""Ingestion layer file readers.

Deserializes pipeline inputs from disk: read_products() loads the product
list from JSON into a list of Product instances; read_transactions()
streams transactions from CSV one row at a time as a generator.
"""

import csv
import json
from collections.abc import Generator
from pathlib import Path

from fmcg_pulse.models.data import Product, Transaction


def read_products(file_path: Path) -> list[Product]:
    """Deserialize a JSON file into a list of Product dataclasses.

    Args:
        file_path (Path): Path to the products JSON file.

    Returns:
        list[Product]: Deserialized list of Product instances.

    """
    with file_path.open() as json_file:
        return [Product(**entry) for entry in json.load(json_file)]


def read_transactions(file_path: Path) -> Generator[Transaction]:
    """Yield Transaction instances from a CSV file one row at a time.

    Args:
        file_path (Path): Path to the transactions CSV file.

    Yields:
        Transaction: One Transaction per row.

    """
    with file_path.open() as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            transaction = Transaction(
                trn_id=row["transaction_id"],
                trn_date=row["date"],  # type: ignore[arg-type]
                store_id=row["store_id"],
                barcode=row["barcode"],
                quantity=int(row["quantity"]),
                unit_price=float(row["unit_price"]),
            )
            yield transaction
