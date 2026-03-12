"""Synthetic data generation.

Builds a list of unique Product instances sampled from the static
archetypes in catalog.py, then generates a stream of Transaction records
against that product list. generate_all() coordinates both and writes the
output to disk as products.json and transactions.csv.
"""

import csv
import json
import logging
import random
from collections.abc import Generator
from dataclasses import asdict
from datetime import date, timedelta

from faker import Faker

from fmcg_pulse.catalog import CATALOG
from fmcg_pulse.models.config import AppConfig
from fmcg_pulse.models.data import Product, Transaction

logger = logging.getLogger(__name__)


LIQUID_KEYWORDS: frozenset[str] = frozenset(
    {
        "gel",
        "liquid",
        "lotion",
        "spray",
        "shampoo",
        "conditioner",
        "bleach",
        "cleaner",
        "milk",
        "drink",
        "cola",
        "soda",
        "lemonade",
        "foam",
        "softener",
    }
)

SOLID_KEYWORDS: frozenset[str] = frozenset(
    {
        "powder",
        "chocolate",
        "crisps",
        "spread",
        "coffee",
        "pizza",
        "toothpaste",
        "zahncreme",
        "gummies",
        "instant",
        "yoghurt",
        "joghurt",
        "gläschen",
    }
)

COUNT_KEYWORDS: frozenset[str] = frozenset(
    {
        "tabs",
        "pods",
        "bags",
        "pouches",
        "blades",
        "sticks",
        "nappies",
    }
)

LIQUID_SUBCATEGORIES: frozenset[str] = frozenset(
    {
        "Laundry",
        "Fabric Softener",
        "Dishwashing",
        "Surface Cleaning",
        "Body Care",
        "Hair Care",
        "Deodorant",
        "Cola",
        "Fruit Soda",
        "Lemonade",
        "Energy Drinks",
        "Long Life Milk",
    }
)

SOLID_SUBCATEGORIES: frozenset[str] = frozenset(
    {
        "Oral Care",
        "Ground Coffee",
        "Instant Coffee",
        "Cream Cheese",
        "Yoghurt",
        "Chocolate",
        "Crisps",
        "Gummy Candy",
        "Spreads",
        "Frozen Pizza",
        "Frozen Fish",
        "Baby Food",
    }
)

COUNT_SUBCATEGORIES: frozenset[str] = frozenset(
    {
        "Dishwasher Tablets",
        "Tea",
        "Juice Drinks",
        "Shaving",
        "Nappies",
        "Cat Food",
        "Dog Food",
    }
)

UNIT_TYPES: frozenset[str] = frozenset({"liquid", "solid", "count"})

fake = Faker()


def _infer_unit_type(descriptor: str, sub_category: str) -> str:
    """Infer the unit type for a product based on its descriptor and sub-category.

    Keyword matches in the descriptor are checked first; if none apply,
    the sub-category is used as a fallback.

    Args:
        descriptor (str): Product descriptor such as "Universal Gel" or "Tea Bags".
        sub_category (str): Product sub-category (e.g., "Ground Coffee", "Tea").

    Returns:
        str: One of {"liquid", "solid", "count"} if inferred, otherwise "unknown".

    """
    tokens = descriptor.lower().split()

    if any(kw in tokens for kw in LIQUID_KEYWORDS):
        return "liquid"
    if any(kw in tokens for kw in SOLID_KEYWORDS):
        return "solid"
    if any(kw in tokens for kw in COUNT_KEYWORDS):
        return "count"
    if sub_category in LIQUID_SUBCATEGORIES:
        return "liquid"
    if sub_category in SOLID_SUBCATEGORIES:
        return "solid"
    if sub_category in COUNT_SUBCATEGORIES:
        return "count"

    logger.warning(
        "Unit type could not be inferred from descriptor: '%s' or sub_category: '%s'.",
        descriptor,
        sub_category,
    )
    return "unknown"


def _format_size(size: float, unit_type: str) -> str:
    """Format a product size into a human-readable string.

    Args:
        size (float): Numeric size greater than zero.
        unit_type (str): One of {"liquid", "solid", "count"}.

    Raises:
        ValueError: If the unit type is invalid or the size is not positive.

    Returns:
        str: Formatted size (e.g., "500ml", "1L", "2.25kg", "5 Tabs").

    """
    u_type = unit_type.lower()

    # Sanity checks
    if u_type not in UNIT_TYPES:
        raise ValueError(
            f"Invalid unit type. Expected any of '{UNIT_TYPES}', got '{unit_type}'."
        )
    if size <= 0:
        raise ValueError("size must be a positive number.")

    def _fmt(x: float) -> str:
        return str(int(x)) if x.is_integer() else str(x)

    if u_type == "liquid" and size >= 1.0:
        return f"{_fmt(size)}L"
    if u_type == "liquid" and size < 1.0:
        return f"{size * 1000:.0f}ml"
    if u_type == "solid" and size >= 1.0:
        return f"{_fmt(size)}kg"
    if u_type == "solid" and size < 1.0:
        return f"{size * 1000:.0f}g"
    if u_type == "count":
        return f"{int(size)} Tabs"

    raise ValueError(f"Unhandled unit type: '{unit_type}'.")


def build_products(n_products: int, catalog: list[dict]) -> list[Product]:
    """Build a list of unique synthetic products sampled from a catalog.

    Samples catalog entries at random, inferring unit type and formatting
    size strings for each product. Skips entries where unit type cannot be
    inferred and guards against duplicate product names.

    Args:
        n_products (int): Number of unique products to build.
        catalog (list[dict]): Product archetypes to sample from.

    Raises:
        ValueError: If n_products exceeds the number of unique products
            possible from the catalog.

    Returns:
        list[Product]: Built products. May be shorter than n_products if
            the attempt cap is reached.

    """
    max_unique = sum(
        len(entry["descriptors"]) * len(entry["sizes"]) for entry in catalog
    )
    if n_products > max_unique:
        raise ValueError(
            f"n_products ({n_products}) exceeds maximum unique products "
            f"possible from catalog ({max_unique})."
        )

    products: list[Product] = []
    seen_names: set[str] = set()
    max_attempts = n_products * 3
    attempts = 0

    while len(products) < n_products:
        if attempts >= max_attempts:
            logger.warning(
                "Reached attempt limit. Built %d of %d products.",
                len(products),
                n_products,
            )
            break

        entry = random.choice(catalog)
        category = entry["category"]
        sub_category = entry["sub_category"]
        manufacturer = entry["manufacturer"]
        brand = entry["brand"]
        is_private_label = entry["is_private_label"]
        descriptor = random.choice(entry["descriptors"])

        # Infer unit type and skip entries where it cannot be inferred
        unit_type = _infer_unit_type(descriptor, sub_category)
        if unit_type == "unknown":
            attempts += 1
            continue

        # Format size and build product name
        size = random.choice(entry["sizes"])
        formatted_size = _format_size(size, unit_type)
        name = f"{entry['brand']} {descriptor} {formatted_size}"

        # Skip duplicate names
        if name in seen_names:
            continue
        seen_names.add(name)

        # Compute price and generate barcode
        ref_price = round(
            random.uniform(*entry["price_range"]) * (size / entry["sizes"][0]) ** 0.85,
            2,
        )
        barcode = fake.ean(length=13)

        product = Product(
            barcode,
            name,
            category,
            sub_category,
            manufacturer,
            brand,
            is_private_label,
            ref_price,
        )
        products.append(product)

    return products


def generate_transactions(
    n_transactions: int,
    products: list[Product],
    start_date: date,
    end_date: date,
) -> Generator[Transaction]:
    """Yield synthetic transactions sampled from a list of products.

    Each transaction applies +/-15% price jitter around the product's
    ref_price and samples quantity with weights favouring lower values.

    Args:
        n_transactions (int): Number of transactions to generate.
        products (list[Product]): Products to sample from.
        start_date (date): Earliest allowed transaction date (inclusive).
        end_date (date): Latest allowed transaction date (inclusive).

    Yields:
        Transaction: One transaction per iteration.

    """
    diff_days = (end_date - start_date).days

    for trn in range(1, n_transactions + 1):
        product = random.choice(products)

        # Construct transaction variables
        trn_id = f"TXN-{trn:04d}"
        trn_date = start_date + timedelta(days=random.randint(0, diff_days))
        store_id = f"STORE-{random.randint(1, 100):03d}"
        quantity = random.choices([1, 2, 3, 4, 5], weights=[40, 30, 15, 10, 5])[0]
        unit_price = round(product.ref_price * random.uniform(0.85, 1.15), 2)

        transaction = Transaction(
            trn_id, trn_date, store_id, product.barcode, quantity, unit_price
        )
        yield transaction


def generate_all(config: AppConfig) -> None:
    """Coordinate product building and transaction generation.

    Builds products via build_products, serializes them to JSON, then
    consumes the generate_transactions generator and writes each row
    to CSV. All file I/O is handled here; neither build_products nor
    generate_transactions touches the filesystem.

    Args:
        config (AppConfig): Pipeline configuration holding generation
            settings and output paths.

    """
    file_path_json = config.paths.raw_dir / "products.json"
    file_path_csv = config.paths.raw_dir / "transactions.csv"

    logger.info("Building %d products.", config.generation.n_products)
    products = build_products(config.generation.n_products, CATALOG)

    # Serialize and dump products into JSON file
    with file_path_json.open("w") as json_file:
        products_serialized = [asdict(product) for product in products]
        json.dump(products_serialized, json_file, indent=4)
    logger.info("Wrote %d products to '%s'.", len(products), file_path_json)

    logger.info("Generating %d transactions.", config.generation.n_transactions)
    transactions = generate_transactions(
        config.generation.n_transactions,
        products,
        config.generation.start_date,
        config.generation.end_date,
    )
    n_transactions = 0

    # Iterate over transactions and write them to CSV file
    with file_path_csv.open("w") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(
            ["transaction_id", "date", "store_id", "barcode", "quantity", "unit_price"]
        )
        for transaction in transactions:
            writer.writerow(
                [
                    transaction.trn_id,
                    transaction.trn_date,
                    transaction.store_id,
                    transaction.barcode,
                    transaction.quantity,
                    transaction.unit_price,
                ]
            )
            n_transactions += 1
    logger.info(
        "Wrote %d transactions to '%s'.",
        n_transactions,
        file_path_csv,
    )
