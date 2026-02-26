"""
Generate precision/recall tables at thresholds 0.05, 0.10, ..., 0.50.

Outputs 4 CSV files:
  - LR_precision_by_threshold.csv
  - LR_recall_by_threshold.csv
  - RF_precision_by_threshold.csv
  - RF_recall_by_threshold.csv

Each table: rows = tasks (DVs), columns = thresholds.
"""
import argparse
import os
import pandas as pd
import numpy as np
from sklearn.metrics import precision_score, recall_score

TASKS = [
    "hosp_mort_24h_gap4h",
    "ARF_2h_gap4h",
    "ARF_6h_gap4h",
    "shock_2h_gap4h",
    "shock_6h_gap4h",
]
THRESHOLDS = [0.05, 0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40, 0.45, 0.50]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--predictions_path",
        type=str,
        default=os.path.join(os.path.dirname(__file__), "..", "output", "benchmarks", "test_predictions.csv"),
    )
    parser.add_argument(
        "--output_dir",
        type=str,
        default=os.path.join(os.path.dirname(__file__), "..", "output", "benchmarks"),
    )
    args = parser.parse_args()

    df = pd.read_csv(args.predictions_path)

    for model in ["LR", "RF"]:
        prec_rows = []
        rec_rows = []
        for task in TASKS:
            gt_col = f"{task}_gt"
            prob_col = f"{task}_{model}_prob"
            if gt_col not in df.columns or prob_col not in df.columns:
                continue
            sub = df[[gt_col, prob_col]].dropna(subset=[gt_col])
            y_true = sub[gt_col].astype(int).values
            y_prob = sub[prob_col].values

            prec = []
            rec = []
            for th in THRESHOLDS:
                y_pred = (y_prob >= th).astype(int)
                prec.append(precision_score(y_true, y_pred, zero_division=0))
                rec.append(recall_score(y_true, y_pred, zero_division=0))

            prec_rows.append([task] + prec)
            rec_rows.append([task] + rec)

        prec_df = pd.DataFrame(prec_rows, columns=["task"] + [f"{t:.2f}" for t in THRESHOLDS])
        rec_df = pd.DataFrame(rec_rows, columns=["task"] + [f"{t:.2f}" for t in THRESHOLDS])

        prec_path = os.path.join(args.output_dir, f"{model}_precision_by_threshold.csv")
        rec_path = os.path.join(args.output_dir, f"{model}_recall_by_threshold.csv")
        prec_df.to_csv(prec_path, index=False)
        rec_df.to_csv(rec_path, index=False)
        print(f"Saved {prec_path}")
        print(f"Saved {rec_path}")

    return 0


if __name__ == "__main__":
    exit(main())
