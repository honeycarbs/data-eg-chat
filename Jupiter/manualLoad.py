import json
import os
from datetime import datetime          # still here in case your Transformer uses it
import pandas as pd

from transformer import Transformer
from dataValidation import Validation
from insert import DataFrameSQLInserter


def validate_transform_load(json_file_path: str) -> None:

    # ────────────────── 1. LOAD ──────────────────
    with open(json_file_path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    messages = payload.get("messages")
    if not isinstance(messages, list):
        raise ValueError(
            f"{json_file_path} must contain a top-level 'messages' array"
        )

    # ────────────────── 2. PARSE STRINGS → LIST[DICT] ──────────────────
    parsed = [
        dict(field.strip().split(": ", 1) for field in msg.split(", "))
        for msg in messages
    ]
    df = pd.DataFrame(parsed)

    # ───── Optional numeric coercions ─────
    numeric_cols = [
        "EVENT_NO_TRIP",
        "EVENT_NO_STOP",
        "VEHICLE_ID",
        "METERS",
        "ACT_TIME",
        "GPS_LONGITUDE",
        "GPS_LATITUDE",
        "GPS_SATELLITES",
        "GPS_HDOP",
    ]
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")

    # ────────────────── 3. VALIDATE, TRANSFORM, RE-VALIDATE ──────────────────
    validator = Validation(df)
    validator.validateBeforeTransform()
    validated_df = validator.get_dataframe()

    transformer = Transformer(validated_df)
    transformer.transform()
    transformed_df = transformer.get_dataframe()

    Validation(transformed_df).validateAfterTransform()
    print(transformed_df)

    # ────────────────── 4. INSERT INTO DB ──────────────────
    db_uri = os.getenv("DB_URI")
    breadcrumb_df = Transformer.createBreadcrumbDF(transformed_df)
    
    # with DataFrameSQLInserter(db_uri) as inserter:
    inserter.insert_dataframe(breadcrumb_df, "breadcrumb")


if __name__ == "__main__":
    # Example usage
    validate_transform_load("data-2025-05-25.json")