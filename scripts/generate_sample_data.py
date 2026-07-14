#!/usr/bin/env python3
"""
generate_sample_data.py

Generates synthetic retail source-system CSVs that stand in for OLTP
extracts landing in S3 (raw zone) before being picked up by the Glue/
Airflow ingestion job. Uses only the Python standard library so it runs
anywhere without extra dependencies.

Usage:
    python scripts/generate_sample_data.py --rows 5000 --out data/raw
"""
import argparse
import csv
import random
from datetime import date, timedelta
from pathlib import Path

random.seed(42)

FIRST_NAMES = ["Aarav", "Sita", "Ram", "Priya", "John", "Maria", "Wei", "Fatima",
               "Carlos", "Emma", "Noah", "Olivia", "Liam", "Ava", "Sanjay", "Anita"]
LAST_NAMES = ["Sharma", "Thapa", "Gurung", "Smith", "Johnson", "Lopez", "Chen",
              "Khan", "Garcia", "Brown", "Karki", "Rai", "Adhikari", "Silva"]
CITIES = [("Kathmandu", "Nepal"), ("Pokhara", "Nepal"), ("Boston", "USA"),
          ("Seattle", "USA"), ("San Francisco", "USA"), ("London", "UK"),
          ("Toronto", "Canada"), ("Sydney", "Australia")]
CATEGORIES = {
    "Electronics": ["Wireless Earbuds", "Smartwatch", "Laptop Stand", "USB-C Hub", "Bluetooth Speaker"],
    "Home": ["Ceramic Mug Set", "LED Desk Lamp", "Throw Blanket", "Air Purifier", "Storage Bin"],
    "Apparel": ["Running Shoes", "Denim Jacket", "Wool Sweater", "Rain Jacket", "Cotton T-Shirt"],
    "Sports": ["Yoga Mat", "Dumbbell Set", "Cycling Helmet", "Water Bottle", "Resistance Bands"],
    "Books": ["Data Engineering Guide", "SQL Cookbook", "Cloud Architecture 101", "Python Basics"],
}
CHANNELS = ["web", "mobile_app", "in_store", "marketplace"]
PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "cash", "gift_card"]


def gen_customers(n):
    rows = []
    for i in range(1, n + 1):
        city, country = random.choice(CITIES)
        rows.append({
            "customer_id": f"CUST{i:05d}",
            "first_name": random.choice(FIRST_NAMES),
            "last_name": random.choice(LAST_NAMES),
            "email": f"customer{i}@example.com",
            "city": city,
            "country": country,
            "signup_date": (date(2023, 1, 1) + timedelta(days=random.randint(0, 900))).isoformat(),
            "customer_segment": random.choice(["Consumer", "Small Business", "Enterprise"]),
        })
    return rows


def gen_stores(n):
    rows = []
    for i in range(1, n + 1):
        city, country = random.choice(CITIES)
        rows.append({
            "store_id": f"STORE{i:03d}",
            "store_name": f"{city} Flagship {i}",
            "city": city,
            "country": country,
            "store_type": random.choice(["Retail", "Warehouse", "Online-Only"]),
        })
    return rows


def gen_products(n):
    rows = []
    pid = 1
    categories = list(CATEGORIES.items())
    for _ in range(n):
        category, items = random.choice(categories)
        name = random.choice(items)
        rows.append({
            "product_id": f"PROD{pid:05d}",
            "product_name": name,
            "category": category,
            "unit_price": round(random.uniform(8.99, 299.99), 2),
            "unit_cost": round(random.uniform(3.00, 180.00), 2),
        })
        pid += 1
    return rows


def gen_orders(n, customers, stores, products, start, end):
    rows = []
    span = (end - start).days
    for i in range(1, n + 1):
        cust = random.choice(customers)
        store = random.choice(stores)
        product = random.choice(products)
        order_date = start + timedelta(days=random.randint(0, span))
        quantity = random.randint(1, 5)
        unit_price = product["unit_price"]
        discount_pct = random.choice([0, 0, 0, 5, 10, 15, 20])
        gross_amount = round(unit_price * quantity, 2)
        discount_amount = round(gross_amount * discount_pct / 100, 2)
        net_amount = round(gross_amount - discount_amount, 2)
        rows.append({
            "order_id": f"ORD{i:07d}",
            "order_date": order_date.isoformat(),
            "customer_id": cust["customer_id"],
            "store_id": store["store_id"],
            "product_id": product["product_id"],
            "quantity": quantity,
            "unit_price": unit_price,
            "discount_pct": discount_pct,
            "gross_amount": gross_amount,
            "discount_amount": discount_amount,
            "net_amount": net_amount,
            "channel": random.choice(CHANNELS),
            "payment_method": random.choice(PAYMENT_METHODS),
        })
    return rows


def write_csv(path, rows):
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"wrote {len(rows):>6} rows -> {path}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=5000, help="number of order rows to generate")
    parser.add_argument("--customers", type=int, default=400)
    parser.add_argument("--stores", type=int, default=12)
    parser.add_argument("--products", type=int, default=60)
    parser.add_argument("--out", type=str, default="data/raw")
    args = parser.parse_args()

    out_dir = Path(args.out)
    customers = gen_customers(args.customers)
    stores = gen_stores(args.stores)
    products = gen_products(args.products)
    orders = gen_orders(args.rows, customers, stores, products,
                         date(2025, 1, 1), date(2026, 6, 30))

    write_csv(out_dir / "customers.csv", customers)
    write_csv(out_dir / "stores.csv", stores)
    write_csv(out_dir / "products.csv", products)
    write_csv(out_dir / "orders.csv", orders)


if __name__ == "__main__":
    main()
