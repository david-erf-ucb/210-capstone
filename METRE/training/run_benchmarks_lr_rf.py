"""
Run LR and RF benchmark models on compiled MEEP data.

Usage:
    python run_benchmarks_lr_rf.py --data_path ../output/MIMIC_compile.npy --output_dir ../output/benchmarks

Saves: trained models (.joblib), results (CSV, JSON), and test predictions CSV to output_dir.
Requires: scikit-learn, numpy.
"""
import argparse
import json
import os
import subprocess
import sys
from datetime import datetime

import numpy as np
import joblib
from sklearn.model_selection import KFold, cross_val_score
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import roc_auc_score, average_precision_score


def filter_los(static_data, vitals_data, thresh, gap):
    los = [i.shape[1] for i in vitals_data]
    ind = [i for i in range(len(los)) if los[i] >= (thresh + gap) and not np.isnan(static_data[i, 0])]
    vitals_reduce = [vitals_data[i][:, :thresh] for i in ind]
    static_data = static_data[ind]
    return static_data, vitals_reduce


def filter_arf(vital, thresh, gap):
    vital_reduce, target = [], []
    for i in range(len(vital)):
        arf_flag = np.where(vital[i][184, :] == 1)[0]
        peep_flag = np.union1d(np.where(vital[i][157, :] == 1)[0], np.where(vital[i][159, :] == 1)[0])
        if len(arf_flag) == 0:
            if len(peep_flag) > 0:
                if peep_flag[0] >= (thresh + gap):
                    vital_reduce.append(vital[i][:, :thresh])
                    target.append(1)
            else:
                vital_reduce.append(vital[i][:, :thresh])
                target.append(0)
        elif arf_flag[0] >= (thresh + gap):
            if (len(peep_flag) > 0 and peep_flag[0] >= (thresh + gap)) or len(peep_flag) == 0:
                vital_reduce.append(vital[i][:, :thresh])
                target.append(1)
    return vital_reduce, np.asarray(target)


def filter_shock(vital, thresh, gap):
    vital_reduce, target = [], []
    for i in range(len(vital)):
        shock_flag = np.where(vital[i][186:191].sum(axis=0) >= 1)[0]
        if len(shock_flag) == 0:
            vital_reduce.append(vital[i][:, :thresh])
            target.append(0)
        elif shock_flag[0] >= (thresh + gap):
            vital_reduce.append(vital[i][:, :thresh])
            target.append(1)
    return vital_reduce, np.asarray(target)


def flatten_for_sklearn(data_list):
    """Flatten list of (200, T) arrays to (n_samples, 200*T)."""
    return np.stack([d.flatten() for d in data_list], axis=0)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_path", type=str,
                        default=os.path.join(os.path.dirname(__file__), "..", "output", "MIMIC_compile.npy"),
                        help="Path to MIMIC_compile.npy (from compile_meep_to_npy.py)")
    parser.add_argument("--output_dir", type=str,
                        default=os.path.join(os.path.dirname(__file__), "..", "output", "benchmarks"),
                        help="Directory for saved models and results")
    parser.add_argument("--n_jobs", type=int, default=-1)
    args = parser.parse_args()

    data_path = os.path.abspath(args.data_path)
    output_dir = os.path.abspath(args.output_dir)
    models_dir = os.path.join(output_dir, "models")
    os.makedirs(models_dir, exist_ok=True)
    if not os.path.exists(data_path):
        print(f"Error: {data_path} not found. Run compile_meep_to_npy.py first.")
        return 1

    data = np.load(data_path, allow_pickle=True).item()
    train_head = data["train_head"]
    dev_head = data["dev_head"]
    test_head = data["test_head"]
    s_train = np.stack(data["static_train_filter"], axis=0)
    s_dev = np.stack(data["static_dev_filter"], axis=0)
    s_test = np.stack(data["static_test_filter"], axis=0)

    task_map = {0: "hosp_mort", 1: "ARF", 2: "shock"}
    tasks = [
        (0, 24, 4),   # hosp mort, 24h, gap 4
        (1, 2, 4),    # ARF 2h
        (1, 6, 4),    # ARF 6h
        (2, 2, 4),    # Shock 2h
        (2, 6, 4),    # Shock 6h
    ]

    kf = KFold(n_splits=5, random_state=42, shuffle=True)
    results = []

    for target_idx, thresh, gap in tasks:
        if target_idx == 0:
            static_train, train_data = filter_los(s_train, train_head, thresh, gap)
            static_dev, dev_data = filter_los(s_dev, dev_head, thresh, gap)
            static_test, test_data = filter_los(s_test, test_head, thresh, gap)
            train_label = static_train[:, 0]  # mort_hosp
            dev_label = static_dev[:, 0]
            test_label = static_test[:, 0]
        elif target_idx == 1:
            train_data, train_label = filter_arf(train_head, thresh, gap)
            dev_data, dev_label = filter_arf(dev_head, thresh, gap)
            test_data, test_label = filter_arf(test_head, thresh, gap)
        else:
            train_data, train_label = filter_shock(train_head, thresh, gap)
            dev_data, dev_label = filter_shock(dev_head, thresh, gap)
            test_data, test_label = filter_shock(test_head, thresh, gap)

        X_train = flatten_for_sklearn(train_data)
        X_test = flatten_for_sklearn(test_data)
        trainval_data = train_data + dev_data
        trainval_label = np.concatenate([train_label, dev_label])
        X_trainval = flatten_for_sklearn(trainval_data)

        for model_name, model in [
            ("LR", LogisticRegression(class_weight="balanced", max_iter=1000, random_state=0, solver="lbfgs")),
            ("RF", RandomForestClassifier(n_estimators=100, class_weight="balanced", random_state=0, n_jobs=args.n_jobs)),
        ]:
            scores = cross_val_score(model, X_trainval, trainval_label, cv=kf, scoring="roc_auc", n_jobs=args.n_jobs)
            model.fit(X_trainval, trainval_label)
            test_auc = roc_auc_score(test_label, model.predict_proba(X_test)[:, 1])
            test_ap = average_precision_score(test_label, model.predict_proba(X_test)[:, 1])
            task_name = f"{task_map[target_idx]}_{thresh}h_gap{gap}h"
            results.append((task_name, model_name, np.mean(scores), np.std(scores), test_auc, test_ap))

            # Save model
            model_path = os.path.join(models_dir, f"{model_name}_{task_name}.joblib")
            joblib.dump(model, model_path)
            print(f"{task_name} {model_name}: CV AUC {np.mean(scores):.3f} +/- {np.std(scores):.3f}  Test AUC {test_auc:.3f}  Test AP {test_ap:.3f}  -> {model_path}")

    # Save results to CSV and JSON
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_data = [
        {"task": r[0], "model": r[1], "cv_auc_mean": float(r[2]), "cv_auc_std": float(r[3]), "test_auc": float(r[4]), "test_ap": float(r[5])}
        for r in results
    ]
    results_json = os.path.join(output_dir, f"results_{timestamp}.json")
    results_csv = os.path.join(output_dir, f"results_{timestamp}.csv")
    with open(results_json, "w") as f:
        json.dump(results_data, f, indent=2)
    with open(results_csv, "w") as f:
        f.write("task,model,cv_auc_mean,cv_auc_std,test_auc,test_ap\n")
        for r in results:
            f.write(f"{r[0]},{r[1]},{r[2]:.4f},{r[3]:.4f},{r[4]:.4f},{r[5]:.4f}\n")

    print(f"\n--- Summary ---")
    print(f"Models saved to: {models_dir}")
    print(f"Results saved to: {results_json}")
    print(f"Results saved to: {results_csv}")
    for r in results:
        print(f"  {r[0]} {r[1]}: Test AUC {r[4]:.3f}")

    # Export per-stay predictions for downstream analysis (precision, recall, disparate impact)
    input_dir = os.path.dirname(data_path)
    export_script = os.path.join(os.path.dirname(__file__), "export_predictions.py")
    export_cmd = [
        sys.executable,
        export_script,
        "--data_path", data_path,
        "--input_dir", input_dir,
        "--models_dir", models_dir,
        "--output_path", os.path.join(output_dir, "test_predictions.csv"),
        "--dict_path", os.path.join(output_dir, "DATA_DICTIONARY.md"),
        "--metrics_path", os.path.join(output_dir, "test_metrics.csv"),
        "--population_path", os.path.join(output_dir, "test_population_summary.csv"),
    ]
    print(f"\nExporting test predictions...")
    ret = subprocess.run(export_cmd)
    if ret.returncode != 0:
        print(f"Warning: export_predictions.py exited with code {ret.returncode}")
    else:
        print(f"Test predictions saved to: {os.path.join(output_dir, 'test_predictions.csv')}")

    return 0


if __name__ == "__main__":
    exit(main())
