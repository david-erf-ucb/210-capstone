# METRE Benchmark Results Summary

**Date:** February 2025  
**Data:** MIMIC-IV 3.1 (MEEP parquet pipeline)  
**Reference:** [Liao & Voldman, J Biomed Inform 141 (2023)](https://github.com/weiliao97/METRE)

---

## 0. Task Populations: Why n_total Differs

Each task uses a **different inclusion filter** based on the prediction setup. All tasks share the same underlying test set (20% of stays, seed 41), but only stays that meet task-specific criteria are included.

| Task | n_total | Inclusion logic |
|------|---------|-----------------|
| **hosp_mort_24h_gap4h** | 8,736 | LOS ≥ 28h (24h + 4h gap); valid mort_hosp |
| **ARF_2h_gap4h** | 2,854 | Vent/PEEP onset ≥ 6h or never; uses first 2h |
| **ARF_6h_gap4h** | 2,464 | Vent/PEEP onset ≥ 10h or never; uses first 6h |
| **shock_2h_gap4h** | 7,181 | Vasopressor onset ≥ 6h or never; uses first 2h |
| **shock_6h_gap4h** | 6,927 | Vasopressor onset ≥ 10h or never; uses first 6h |

**Why the differences?**

- **Time-based:** Longer observation windows (6h vs 2h) require longer ICU stays (≥10h vs ≥6h), so fewer stays qualify.
- **Outcome-based:** ARF and shock exclude stays where the event occurs *before* the gap (to avoid label leakage). Hosp_mort has no such exclusion beyond LOS.
- **hosp_mort** has the largest n because it only requires LOS ≥ 28h and a valid mortality label.
- **ARF** has the smallest n because it requires a clear vent/PEEP timeline and excludes early-onset cases.

---

## 1. Models Run

| Model | Type | Notes |
|-------|------|------|
| **LR** | Logistic Regression | `class_weight="balanced"`, `max_iter=1000`, L-BFGS |
| **RF** | Random Forest | 100 trees, `class_weight="balanced"` |

Both use flattened vital + intervention features (200 × T → 200T). No hyperparameter tuning (no Bayesian optimization); fixed configs for reproducibility.

---

## 2. Results

### Test AUC

| Task | LR | RF |
|------|-----|-----|
| hosp_mort_24h_gap4h | 0.809 | **0.841** |
| ARF_2h_gap4h | 0.661 | **0.693** |
| ARF_6h_gap4h | 0.639 | **0.659** |
| shock_2h_gap4h | 0.674 | **0.700** |
| shock_6h_gap4h | **0.670** | **0.677** |

### Test AP (Average Precision)

| Task | LR | RF |
|------|-----|-----|
| hosp_mort_24h_gap4h | 0.331 | 0.350 |
| ARF_2h_gap4h | 0.564 | 0.586 |
| ARF_6h_gap4h | 0.405 | 0.404 |
| shock_2h_gap4h | 0.183 | 0.196 |
| shock_6h_gap4h | 0.123 | 0.119 |

### Class Balance & Precision/Recall (threshold 0.5)

| Task | n_pos | n_neg | % pos | LR prec | LR rec | RF prec | RF rec |
|------|-------|-------|-------|---------|--------|---------|--------|
| hosp_mort_24h | 787 | 7,949 | 9.0% | 0.23 | 0.68 | 0.67 | 0.04 |
| ARF_2h | 1,053 | 1,801 | 36.9% | 0.52 | 0.52 | 0.64 | 0.31 |
| ARF_6h | 663 | 1,801 | 26.9% | 0.36 | 0.54 | 0.55 | 0.04 |
| shock_2h | 720 | 6,461 | 10.0% | 0.17 | 0.54 | 0.26 | 0.13 |
| shock_6h | 466 | 6,461 | 6.7% | 0.12 | 0.57 | 0.18 | 0.00 |

---

## 3. Assessment: Are These Results Good?

**Hosp_mort (AUC 0.81–0.84):** Strong. In-hospital mortality from early ICU data typically reaches AUC 0.80–0.85 in MIMIC. These results are in that range.

**ARF (AUC 0.64–0.69):** Moderate. Predicting respiratory failure from pre-onset vitals is harder than mortality. AUC in the mid-0.60s is reasonable for this task.

**Shock (AUC 0.67–0.70):** Moderate. Similar to ARF; vasopressor onset prediction from prior vitals is challenging.

**vs. original METRE ([weiliao97/METRE](https://github.com/weiliao97/METRE)):**

- Original METRE uses **48h** for mortality (we use 24h), **12h** for ARF (we use 2h/6h), and Bayesian optimization for LR/RF.
- Their README reports AUC for 48h mortality; typical values are ~0.82–0.85 for LR/RF on MIMIC.
- Our hosp_mort AUC (0.81–0.84) is comparable despite a shorter window (24h vs 48h).
- Our ARF/shock setups differ in time windows and definitions, so direct comparison is limited. Our results are in a plausible range for these tasks.

---

## 4. Results Relative to One Another

- **RF generally outperforms LR** on AUC (except shock_6h, where they are close).
- **Hosp_mort is the strongest task** (AUC 0.81–0.84); it has the most data and a well-defined outcome.
- **ARF and shock are similar** (AUC ~0.64–0.70); both predict acute events from prior vitals.
- **Longer windows (6h vs 2h)** do not improve AUC here; ARF_6h and shock_6h are slightly worse or similar to 2h.
- **Precision–recall trade-off:** RF tends toward higher precision and lower recall at 0.5 threshold (especially for rare outcomes), while LR is more recall-oriented. For imbalanced tasks, threshold tuning is important.

---

## 5. Why Models Perform Well or Poorly

**Hosp_mort performs best because:**
- Large sample (8,736 stays).
- Mortality is a clear, well-recorded outcome.
- Early vitals and interventions carry strong prognostic signal.
- Class imbalance (9% positive) is manageable with `class_weight="balanced"`.

**ARF and shock are harder because:**
- Smaller effective samples (2.5k–7k).
- Onset timing is more variable and definition-dependent.
- Pre-onset signal may be weaker than for mortality.
- ARF has more balanced classes (27–37% positive); shock is more imbalanced (7–10%).

**RF vs LR:**
- RF benefits from non-linear interactions and may handle high-dimensional flattened inputs better.
- LR can hit `max_iter` limits; increasing it may help.
- RF’s tendency toward high precision / low recall at 0.5 suggests it is conservative; threshold tuning could improve utility.

---

## 6. Proposals for Future Work

1. **Hyperparameter tuning:** Use Bayesian optimization (as in original METRE) for LR and RF.
2. **Threshold optimization:** Choose thresholds by utility (e.g., F2, cost-sensitive) instead of 0.5.
3. **Temporal models:** Add TCN/LSTM (as in METRE) to exploit time structure instead of flattening.
4. **Longer windows:** Test 48h for mortality and 12h for ARF to match original METRE.
5. **Fairness analysis:** Use `test_predictions.csv` (age, gender, race) for disparate impact and calibration by subgroup.
6. **eICU validation:** Run the same pipeline on eICU for cross-database validation.
7. **Feature importance:** Inspect RF feature importances and LR coefficients for interpretability.
8. **Calibration:** Evaluate probability calibration (e.g., reliability diagrams) for clinical use.

---

## 7. Visuals

**Interactive HTML (open in browser):** `BENCHMARK_FIGURES/benchmark_results.html`  
Charts: AUC by task, class balance, % positive, precision vs recall.

**Static PNGs (optional):** Run `python training/plot_benchmark_results.py` to generate:
- `auc_by_task.png` — AUC by task and model
- `class_balance.png` — Class balance (n_pos, n_neg) by task
- `pct_positive_by_task.png` — % positive class
- `precision_recall_tradeoff.png` — Precision vs recall by task and model  

Requires `matplotlib`.
