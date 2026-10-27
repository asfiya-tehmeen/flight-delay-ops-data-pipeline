# Databricks notebook source
# MAGIC %md
# MAGIC # 07 - Model: will this flight arrive 15+ minutes late?
# MAGIC Prediction is made before departure, from `gold.feat_delay_model` only.
# MAGIC
# MAGIC * **Chronological split** (dates in `config.py`): train, then validation
# MAGIC   (used only to choose the decision threshold), a one-day gap, then test.
# MAGIC * **Baseline first:** the trailing delay rate (route, else origin, else
# MAGIC   carrier, else the training base rate). Models must beat it.
# MAGIC * **Metrics:** AUC-PR, precision/recall, confusion matrix. Not accuracy:
# MAGIC   about 3 in 4 flights are on time, so "never delayed" already scores ~75%.
# MAGIC * Runs are logged with MLflow.
# MAGIC
# MAGIC One month of data is a limitation: the first days have no history and the
# MAGIC test week is a single regime. Results are indicative, not production claims.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

import numpy as np
import pandas as pd
import mlflow
import mlflow.sklearn
from pyspark.sql import functions as F
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, roc_auc_score, confusion_matrix, precision_recall_curve
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, OrdinalEncoder, StandardScaler

LABEL = "label_arr_delayed15"
pdf = spark.table(f"{GOLD}.feat_delay_model").toPandas()
pdf["dep_date_utc"] = pd.to_datetime(pdf["dep_date_utc"])

# Cold start: until HIST_MIN_DAYS of arrivals exist there is no usable history,
# so those first days are not used for training (test/val have full history).
first_day = pdf.dep_date_utc.min()
warm_from = first_day + pd.Timedelta(days=HIST_MIN_DAYS)
n_cold = int((pdf.dep_date_utc < warm_from).sum())
print(f"cold-start rows excluded from training: {n_cold:,} (before {warm_from.date()})")

train = pdf[(pdf.dep_date_utc <= TRAIN_END) & (pdf.dep_date_utc >= warm_from)]
val = pdf[(pdf.dep_date_utc > TRAIN_END) & (pdf.dep_date_utc <= VAL_END)]
test = pdf[pdf.dep_date_utc >= TEST_START]
for n, d in (("train", train), ("val", val), ("test", test)):
    print(f"{n}: {len(d):,} rows, delayed rate {d[LABEL].mean():.3f}, "
          f"{d.dep_date_utc.min().date()} .. {d.dep_date_utc.max().date()}")
assert train.dep_date_utc.max() < val.dep_date_utc.min() < test.dep_date_utc.min(), "split is not chronological"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Features

# COMMAND ----------

NUMERIC = ["day_of_week", "sched_dep_hour_local", "distance_miles", "crs_elapsed_min"] \
    + [f"wx_{v}" for v in WEATHER_VARS] \
    + [f"hist_{k}_{m}" for k in ("route", "origin", "carrier") for m in ("rate", "n")]
# `month` is constant in a one-month build, so it is deliberately not used here.

# origin/dest have ~350 values; keep the most common in train, bucket the rest
TOP = 150
top_origin = set(train.origin.value_counts().head(TOP).index)
top_dest = set(train.dest.value_counts().head(TOP).index)

def prep(df):
    df = df.copy()
    df["origin_c"] = df.origin.where(df.origin.isin(top_origin), "OTHER")
    df["dest_c"] = df.dest.where(df.dest.isin(top_dest), "OTHER")
    return df

train, val, test = prep(train), prep(val), prep(test)
CATEGORICAL = ["marketing_carrier", "origin_c", "dest_c"]
FEATURES = NUMERIC + CATEGORICAL

def xy(df):
    return df[FEATURES], df[LABEL].values

(Xtr, ytr), (Xva, yva), (Xte, yte) = xy(train), xy(val), xy(test)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Baseline: trailing delay rate

# COMMAND ----------

base_rate = ytr.mean()

def baseline_score(df):
    return (df.hist_route_rate.fillna(df.hist_origin_rate)
              .fillna(df.hist_carrier_rate).fillna(base_rate).values)

def evaluate(name, y, score, threshold=None):
    out = {"auc_pr": average_precision_score(y, score), "roc_auc": roc_auc_score(y, score)}
    if threshold is not None:
        pred = score >= threshold
        tn, fp, fn, tp = confusion_matrix(y, pred).ravel()
        out.update(threshold=float(threshold), precision=tp / max(tp + fp, 1), recall=tp / max(tp + fn, 1),
                   tp=int(tp), fp=int(fp), fn=int(fn), tn=int(tn))
    return out

def best_f1_threshold(y, score):
    p, r, t = precision_recall_curve(y, score)
    f1 = 2 * p[:-1] * r[:-1] / np.clip(p[:-1] + r[:-1], 1e-9, None)
    return t[int(np.argmax(f1))]

results = {}
thr = best_f1_threshold(yva, baseline_score(val))
results["baseline_trailing_rate"] = evaluate("baseline", yte, baseline_score(test), thr)
results["prevalence (random guess AUC-PR)"] = {"auc_pr": float(yte.mean())}
print(pd.DataFrame(results).T[["auc_pr", "roc_auc", "precision", "recall"]])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Models (logged with MLflow)

# COMMAND ----------

logreg = Pipeline([
    ("prep", ColumnTransformer([
        ("num", Pipeline([("imp", SimpleImputer(strategy="median", add_indicator=True)),
                          ("sc", StandardScaler())]), NUMERIC),
        ("cat", OneHotEncoder(handle_unknown="ignore", min_frequency=200), CATEGORICAL),
    ])),
    ("clf", LogisticRegression(max_iter=500, C=0.5)),
])

# HistGradientBoosting handles NULL history natively, so no imputing.
# Number of trees is chosen on the CHRONOLOGICAL validation window, not by the
# built-in early stopping, which holds out a random slice of the training
# period and so rewards fitting that period (the future-leaking split we avoid).
# Hyperparameters below were fixed a priori, deliberately low capacity because
# the test period is a different weather regime from the training period.
def make_prep():
    # categoricals come out first (columns 0-2), numerics pass through after
    return ColumnTransformer(
        [("cat", OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1), CATEGORICAL)],
        remainder="passthrough")

def make_gbm(n_trees):
    return HistGradientBoostingClassifier(
        max_iter=n_trees, learning_rate=0.05, max_leaf_nodes=15, min_samples_leaf=200,
        l2_regularization=1.0, early_stopping=False, random_state=0,
        categorical_features=[0, 1, 2])

probe_prep = make_prep()
Xtr_t, Xva_t = probe_prep.fit_transform(Xtr), probe_prep.transform(Xva)
probe = make_gbm(400).fit(Xtr_t, ytr)
val_ap = [average_precision_score(yva, p[:, 1]) for p in probe.staged_predict_proba(Xva_t)]
best_trees = int(np.argmax(val_ap)) + 1
print(f"trees chosen on validation: {best_trees} (validation AUC-PR {max(val_ap):.4f}; "
      f"at 400 trees {val_ap[-1]:.4f})")

gbm = Pipeline([("prep", make_prep()), ("clf", make_gbm(best_trees))])

fitted = {}
for name, model in (("logistic_regression", logreg), ("gradient_boosting", gbm)):
    with mlflow.start_run(run_name=name):
        model.fit(Xtr, ytr)
        thr = best_f1_threshold(yva, model.predict_proba(Xva)[:, 1])
        res = evaluate(name, yte, model.predict_proba(Xte)[:, 1], thr)
        mlflow.log_params({"model": name, "n_train": len(Xtr), "train_end": TRAIN_END,
                           "test_start": TEST_START, "features": ",".join(FEATURES),
                           "hist_min_days": HIST_MIN_DAYS, "gbm_trees": best_trees})
        mlflow.log_metrics({k: v for k, v in res.items()})
        mlflow.log_metric("baseline_auc_pr", results["baseline_trailing_rate"]["auc_pr"])
        try:
            mlflow.sklearn.log_model(model, "model")
        except Exception as e:           # report, don't hide: metrics are still logged
            print(f"[{name}] model artifact not logged: {e}")
    fitted[name], results[name] = model, res

table = pd.DataFrame(results).T
display(table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkpoint: does it beat the baseline, and where does it fail?

# COMMAND ----------

best = max(("logistic_regression", "gradient_boosting"), key=lambda k: results[k]["auc_pr"])
base_ap, best_ap = results["baseline_trailing_rate"]["auc_pr"], results[best]["auc_pr"]
print(f"best model: {best} | AUC-PR {best_ap:.4f} vs baseline {base_ap:.4f} "
      f"(+{best_ap - base_ap:.4f}, {100 * (best_ap / base_ap - 1):.1f}% relative) "
      f"vs random {yte.mean():.4f}")
print("beats baseline:", best_ap > base_ap)
r = results[best]
print(f"@F1 threshold {r['threshold']:.3f}: precision {r['precision']:.3f}, recall {r['recall']:.3f}")
print("confusion matrix [tn fp / fn tp]:", r["tn"], r["fp"], "/", r["fn"], r["tp"])

# COMMAND ----------

# Where does it fail? AUC-PR by test day, carrier and hour block.
scored = test.assign(score=fitted[best].predict_proba(Xte)[:, 1],
                     base=baseline_score(test))

def segment(col):
    rows = []
    for k, g in scored.groupby(col):
        if g[LABEL].nunique() < 2 or len(g) < 500:
            continue
        rows.append({col: k, "n": len(g), "delayed_rate": g[LABEL].mean(),
                     "model_auc_pr": average_precision_score(g[LABEL], g.score),
                     "baseline_auc_pr": average_precision_score(g[LABEL], g.base)})
    return pd.DataFrame(rows)

display(segment("dep_date_utc"))
display(segment("marketing_carrier"))
scored["hour_block"] = pd.cut(scored.sched_dep_hour_local, [-1, 5, 11, 17, 23],
                              labels=["night", "morning", "afternoon", "evening"])
display(segment("hour_block"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Why the leakage rules matter (demonstration, NOT a valid model)
# MAGIC Adding the departure delay, which the feature table rightly excludes, gives
# MAGIC an impressive score that could never be used before departure.

# COMMAND ----------

GRAIN = ["flight_date", "marketing_carrier", "flight_number", "origin", "dest", "sched_dep_utc"]
leak = (spark.table(f"{SILVER}.flights_wx").filter("flight_status = 'completed'")
        .select(*GRAIN, "dep_delay_min").toPandas())

def with_dep_delay(df):
    m = df.merge(leak, on=GRAIN, how="left", validate="one_to_one")
    assert m.dep_delay_min.notna().all(), "grain join lost rows"
    return m

ltr, lte = with_dep_delay(train), with_dep_delay(test)
lk = HistGradientBoostingClassifier(random_state=0).fit(ltr[["dep_delay_min"]], ltr[LABEL])
leaky_ap = average_precision_score(lte[LABEL], lk.predict_proba(lte[["dep_delay_min"]])[:, 1])
print(f"AUC-PR using dep_delay alone (INVALID, leaks): {leaky_ap:.4f} | honest best model: {best_ap:.4f}")
