"""
Generate benchmark summary figures.

Usage:
    python plot_benchmark_results.py --benchmarks_dir ../output/benchmarks
"""
import argparse
import os

import matplotlib
matplotlib.use("Agg")  # headless backend
import matplotlib.pyplot as plt
import pandas as pd


def load_data(benchmarks_dir):
    """Load metrics and population summary CSVs."""
    metrics = pd.read_csv(os.path.join(benchmarks_dir, "test_metrics.csv"))
    pop = pd.read_csv(os.path.join(benchmarks_dir, "test_population_summary.csv"))
    return metrics, pop


def plot_auc_by_task(metrics, out_path):
    """Bar chart: AUC by task and model."""
    fig, ax = plt.subplots(figsize=(10, 5))
    tasks = metrics["task"].unique()
    x = range(len(tasks))
    w = 0.35

    lr = metrics[metrics["model"] == "LR"].set_index("task").loc[tasks, "auc"]
    rf = metrics[metrics["model"] == "RF"].set_index("task").loc[tasks, "auc"]

    ax.bar([i - w / 2 for i in x], lr.values, w, label="LR", color="steelblue")
    ax.bar([i + w / 2 for i in x], rf.values, w, label="RF", color="coral")

    ax.set_xticks(x)
    ax.set_xticklabels([t.replace("_gap4h", "") for t in tasks], rotation=25, ha="right")
    ax.set_ylabel("Test AUC")
    ax.set_title("Test AUC by Task and Model")
    ax.legend()
    ax.set_ylim(0.5, 0.9)
    ax.axhline(0.5, color="gray", linestyle="--", alpha=0.5)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close()
    print(f"Saved {out_path}")


def plot_class_balance(pop, out_path):
    """Stacked bar chart: n_positive and n_negative by task."""
    fig, ax = plt.subplots(figsize=(10, 5))
    tasks = pop["task"].tolist()
    labels = [t.replace("_gap4h", "") for t in tasks]
    x = range(len(tasks))

    ax.bar(x, pop["n_negative"], label="Negative", color="lightsteelblue")
    ax.bar(x, pop["n_positive"], bottom=pop["n_negative"], label="Positive", color="indianred")

    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=25, ha="right")
    ax.set_ylabel("n stays")
    ax.set_title("Test Set Class Balance by Task")
    ax.legend()
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close()
    print(f"Saved {out_path}")


def plot_precision_recall(metrics, out_path):
    """Scatter: precision vs recall by task and model."""
    fig, ax = plt.subplots(figsize=(8, 8))

    for model in ["LR", "RF"]:
        m = metrics[metrics["model"] == model]
        ax.scatter(m["recall"], m["precision"], label=model, s=80, alpha=0.8)

    for _, row in metrics.iterrows():
        ax.annotate(
            row["task"].replace("_gap4h", "").replace("hosp_mort_24h", "hosp_mort"),
            (row["recall"], row["precision"]),
            fontsize=8,
            alpha=0.8,
        )

    ax.set_xlabel("Recall (threshold 0.5)")
    ax.set_ylabel("Precision (threshold 0.5)")
    ax.set_title("Precision vs Recall by Task and Model")
    ax.legend()
    ax.set_xlim(-0.05, 1.05)
    ax.set_ylim(-0.05, 1.05)
    ax.plot([0, 1], [0, 1], "k--", alpha=0.3)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close()
    print(f"Saved {out_path}")


def plot_pct_positive(pop, out_path):
    """Bar chart: % positive class by task."""
    fig, ax = plt.subplots(figsize=(10, 5))
    tasks = pop["task"].tolist()
    labels = [t.replace("_gap4h", "") for t in tasks]
    colors = plt.cm.RdYlGn_r(pop["pct_positive"] / 50)  # red=low, green=high

    ax.bar(range(len(tasks)), pop["pct_positive"], color=colors)
    ax.set_xticks(range(len(tasks)))
    ax.set_xticklabels(labels, rotation=25, ha="right")
    ax.set_ylabel("% positive class")
    ax.set_title("Class Imbalance by Task")
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close()
    print(f"Saved {out_path}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--benchmarks_dir",
        type=str,
        default=os.path.join(os.path.dirname(__file__), "..", "output", "benchmarks"),
    )
    parser.add_argument(
        "--out_dir",
        type=str,
        default=None,
        help="Output directory for figures (default: benchmarks_dir/BENCHMARK_FIGURES)",
    )
    args = parser.parse_args()

    benchmarks_dir = os.path.abspath(args.benchmarks_dir)
    out_dir = os.path.abspath(args.out_dir or os.path.join(benchmarks_dir, "BENCHMARK_FIGURES"))
    os.makedirs(out_dir, exist_ok=True)

    metrics, pop = load_data(benchmarks_dir)

    plot_auc_by_task(metrics, os.path.join(out_dir, "auc_by_task.png"))
    plot_class_balance(pop, os.path.join(out_dir, "class_balance.png"))
    plot_precision_recall(metrics, os.path.join(out_dir, "precision_recall_tradeoff.png"))
    plot_pct_positive(pop, os.path.join(out_dir, "pct_positive_by_task.png"))


if __name__ == "__main__":
    main()
