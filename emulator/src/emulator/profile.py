"""Basic profiling of the Olist source files."""

from pathlib import Path


from .data import FILES, load_table


def profile(data_dir) -> None:
    path = Path(data_dir)

    missing = [
        filename for filename in FILES.values() if not (path / filename).is_file()
    ]
    if missing:
        raise FileNotFoundError(f"Missing CSV files: {missing}")

    tables = {
        name: load_table(path, name)
        for name in (
            "customers",
            "orders",
            "items",
            "payments",
            "reviews",
            "products",
            "sellers",
        )
    }

    print("== Source row counts ==")
    for name, df in tables.items():
        print(f"{name:15s} {len(df):>10,}")

    orders = tables["orders"]
    items = tables["items"]
    payments = tables["payments"]
    reviews = tables["reviews"]

    print("\n== Order statuses ==")
    print(orders["order_status"].value_counts(dropna=False))

    print("\n== Data quality checks ==")

    print(
        "Duplicate order IDs:",
        orders["order_id"].duplicated().sum(),
    )

    print(
        "Orders without items:",
        (~orders["order_id"].isin(items["order_id"])).sum(),
    )

    print(
        "Orders without payments:",
        (~orders["order_id"].isin(payments["order_id"])).sum(),
    )

    print(
        "Duplicate review IDs:",
        reviews["review_id"].duplicated().sum(),
    )
