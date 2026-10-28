"""A self-contained Bronze/Silver/Gold ETL pipeline for retail order events."""
import csv
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
SOURCE, BRONZE, SILVER, GOLD = (DATA / name for name in ("source", "bronze", "silver", "gold"))
REQUIRED = {"order_id", "event_time", "customer_id", "product", "category", "quantity", "unit_price"}


def write_csv(path, rows, fields):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def create_source_events():
    """Create representative source events, including one invalid event for QA."""
    SOURCE.mkdir(parents=True, exist_ok=True)
    events = [
        {"order_id": "R-1001", "event_time": "2026-01-04T10:30:00Z", "customer_id": "C-01", "product": "Coffee Maker", "category": "Home", "quantity": 1, "unit_price": 120.0},
        {"order_id": "R-1002", "event_time": "2026-01-16T16:00:00Z", "customer_id": "C-02", "product": "Wireless Headphones", "category": "Electronics", "quantity": 2, "unit_price": 85.0},
        {"order_id": "R-1003", "event_time": "2026-02-02T09:10:00Z", "customer_id": "C-01", "product": "Coffee Maker", "category": "Home", "quantity": 1, "unit_price": 120.0},
        {"order_id": "R-1004", "event_time": "2026-02-19T14:20:00Z", "customer_id": "C-03", "product": "Desk Organizer", "category": "Office", "quantity": 3, "unit_price": 24.0},
        {"order_id": "R-1005", "event_time": "2026-03-08T11:45:00Z", "customer_id": "C-02", "product": "Wireless Headphones", "category": "Electronics", "quantity": 1, "unit_price": 85.0},
        {"order_id": "R-BAD", "event_time": "2026-03-10T11:45:00Z", "customer_id": "C-04", "product": "Broken Event", "category": "Office", "quantity": -1, "unit_price": 10.0},
    ]
    with (SOURCE / "orders.jsonl").open("w", encoding="utf-8") as file:
        for event in events:
            file.write(json.dumps(event) + "\n")


def bronze_ingest():
    BRONZE.mkdir(parents=True, exist_ok=True)
    source = SOURCE / "orders.jsonl"
    destination = BRONZE / "orders_raw.jsonl"
    destination.write_text(source.read_text(encoding="utf-8"), encoding="utf-8")
    return destination


def validate(event):
    missing = REQUIRED - set(event)
    if missing:
        return f"missing required fields: {', '.join(sorted(missing))}"
    try:
        datetime.fromisoformat(event["event_time"].replace("Z", "+00:00"))
        if int(event["quantity"]) <= 0 or float(event["unit_price"]) < 0:
            return "quantity must be positive and unit_price cannot be negative"
    except (TypeError, ValueError):
        return "invalid date or numeric value"
    return None


def silver_transform(bronze_file):
    clean, rejected = [], []
    seen = set()
    for line in bronze_file.read_text(encoding="utf-8").splitlines():
        event = json.loads(line)
        error = validate(event)
        if event.get("order_id") in seen:
            error = "duplicate order_id"
        if error:
            rejected.append({"raw_event": line, "reason": error, "rejected_at": datetime.now(timezone.utc).isoformat()})
            continue
        seen.add(event["order_id"])
        timestamp = datetime.fromisoformat(event["event_time"].replace("Z", "+00:00"))
        clean.append({**event, "quantity": int(event["quantity"]), "unit_price": float(event["unit_price"]), "revenue": round(int(event["quantity"]) * float(event["unit_price"]), 2), "month": timestamp.strftime("%Y-%m")})
    write_csv(SILVER / "orders_clean.csv", clean, [*REQUIRED, "revenue", "month"])
    write_csv(SILVER / "rejected_events.csv", rejected, ["raw_event", "reason", "rejected_at"])
    return clean, rejected


def gold_aggregate(orders):
    monthly_product, customer = defaultdict(lambda: {"orders": 0, "revenue": 0.0}), defaultdict(lambda: {"orders": 0, "revenue": 0.0})
    for order in orders:
        product_key = (order["month"], order["category"])
        monthly_product[product_key]["orders"] += 1
        monthly_product[product_key]["revenue"] += order["revenue"]
        customer[order["customer_id"]]["orders"] += 1
        customer[order["customer_id"]]["revenue"] += order["revenue"]
    product_rows = [{"month": key[0], "category": key[1], "orders": value["orders"], "revenue": round(value["revenue"], 2)} for key, value in monthly_product.items()]
    customer_rows = [{"customer_id": key, "orders": value["orders"], "lifetime_revenue": round(value["revenue"], 2)} for key, value in customer.items()]
    write_csv(GOLD / "monthly_category_metrics.csv", product_rows, ["month", "category", "orders", "revenue"])
    write_csv(GOLD / "customer_metrics.csv", customer_rows, ["customer_id", "orders", "lifetime_revenue"])
    return product_rows, customer_rows


def run():
    create_source_events()
    clean, rejected = silver_transform(bronze_ingest())
    gold_aggregate(clean)
    print(f"Pipeline complete: {len(clean)} valid orders, {len(rejected)} rejected events")


if __name__ == "__main__":
    run()
