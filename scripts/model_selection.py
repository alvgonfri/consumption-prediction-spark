import os
import sqlite3

import polars as pl
from dotenv import load_dotenv

load_dotenv()
BASE_PATH = os.getenv("BASE_PATH")

WEIGHTS = {
    "rmse_test": 0.40,
    "mae_test": 0.20,
    "r2_test": 0.20,
    "generalization_score": 0.10,
    "inference_time": 0.10,
}


def compute_generalization(df):
    rmse_train = df["rmse_train"]
    rmse_val = df["rmse_val"]

    rel_gap = (rmse_val - rmse_train).abs() / (
        rmse_train + 1e-10
    )  # Avoid division by zero

    return 1 / (1 + rel_gap)  # Higher is better


def normalize(series, higher_is_better=True):
    min_val = series.min()
    max_val = series.max()

    denom = max_val - min_val + 1e-10  # Avoid division by zero

    if higher_is_better:
        return (series - min_val) / denom
    else:
        return (max_val - series) / denom


def model_selection():
    conn = sqlite3.connect(os.path.join(BASE_PATH, "db", "audit.db"))
    df = pl.read_database(
        "SELECT * FROM audit ORDER BY run_datetime DESC LIMIT 4", conn
    )  # Get the 4 models from the latest run
    conn.close()

    # ====================== Compute generalization score ======================
    df = df.with_columns(compute_generalization(df).alias("generalization_score"))

    # ====================== Normalize metrics ======================
    df = df.with_columns(
        [
            normalize(df["rmse_test"], higher_is_better=False).alias("rmse_test_norm"),
            normalize(df["mae_test"], higher_is_better=False).alias("mae_test_norm"),
            normalize(df["r2_test"], higher_is_better=True).alias("r2_test_norm"),
            normalize(df["generalization_score"], higher_is_better=True).alias(
                "generalization_norm"
            ),
            normalize(df["inference_time"], higher_is_better=False).alias(
                "inference_time_norm"
            ),
        ]
    )

    # ====================== Compute final score ======================
    df = df.with_columns(
        (
            df["rmse_test_norm"] * WEIGHTS["rmse_test"]
            + df["mae_test_norm"] * WEIGHTS["mae_test"]
            + df["r2_test_norm"] * WEIGHTS["r2_test"]
            + df["generalization_norm"] * WEIGHTS["generalization_score"]
            + df["inference_time_norm"] * WEIGHTS["inference_time"]
        ).alias("final_score")
    )

    # ====================== Show scores for each model ======================
    print("\n=== Model Scores ===")
    print(
        df.select(
            [
                "model",
                "rmse_test_norm",
                "mae_test_norm",
                "r2_test_norm",
                "generalization_norm",
                "inference_time_norm",
                "final_score",
            ]
        )
    )

    # ====================== Select best model ======================
    best_model = df.sort("final_score", descending=True).row(0, named=True)

    print("\n=== Best Model Selected ===")
    print(f"Model: {best_model['model']}")
    print(f"Score: {best_model['final_score']:.4f}")
    print(
        {
            "rmse_test": best_model["rmse_test"],
            "mae_test": best_model["mae_test"],
            "r2_test": best_model["r2_test"],
            "generalization_score": best_model["generalization_score"],
            "inference_time": best_model["inference_time"],
        }
    )

    # ====================== Update best model symlink ======================
    best_model_path = os.path.join(BASE_PATH, "models", "best_model")
    if os.path.exists(best_model_path):
        os.remove(best_model_path)
    os.symlink(os.path.join(BASE_PATH, "models", best_model["model"]), best_model_path)


model_selection()
