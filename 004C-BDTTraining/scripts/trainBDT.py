#!/usr/bin/env python3
"""
Worker script to train the qqbar-vs-gg XGBoost classifier for one era.

Reads the parquet outputs of a 004C-BDTTraining extraction run for a single
dataset (per training_config.yaml's TrainingSample -- nominal
ttbar_SemiLeptonic), keeps only events with y in {1, 2} (qqbar / gg hard
scattering, dropping qg/qq'/qq-same-flavour/undefined), balances the two
classes, grid-searches an XGBoost classifier, computes built-in and
permutation feature importance, and optionally retrains on the top-N most
important features. Mirrors the structure of the old (now-deleted, git
history commit f7fd8f5) 004B-BDT/scripts/old_ignore/BDT.py, corrected to
filter strictly to y in {1, 2} instead of qqbar-vs-everything-else, and
swapped from sklearn's GradientBoostingClassifier to xgboost.XGBClassifier.

Usage:
    python scripts/trainBDT.py --datasetJSON <Parquet_..._datasets.json> \\
        --trainingConfig training_config.yaml --outputDir <dir> --era <era> \\
        [--sample] [--force]
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

import joblib
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.inspection import permutation_importance
from sklearn.metrics import accuracy_score, confusion_matrix, roc_auc_score, roc_curve
from sklearn.model_selection import GridSearchCV, train_test_split
from xgboost import XGBClassifier

sys.path.insert(0, str(Path(__file__).parent))
import utils

SAMPLE_ROW_CAP = 20000


def setup_logging(era, output_dir):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    file_handler = logging.FileHandler(os.path.join(output_dir, f"trainBDT_{era}.log"))
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)


def resolve_parquet_files(dataset_json_path, training_sample):
    with open(dataset_json_path) as f:
        datasetJSON = json.load(f)

    DataMC, group, dataset = (
        training_sample["DataMC"], training_sample["group"], training_sample["dataset"]
    )
    try:
        files_dict = datasetJSON[DataMC][group][dataset]
    except KeyError:
        logging.error(
            f"TrainingSample {DataMC}/{group}/{dataset} not found in {dataset_json_path}. "
            f"Available: { {dm: list(datasetJSON[dm].keys()) for dm in datasetJSON} }"
        )
        raise
    return list(files_dict.keys())


def read_parquet_files(files, columns, sample=False):
    dfs = []
    total_rows = 0
    for path in files:
        table = pq.read_table(path, columns=columns)
        df = table.to_pandas()
        dfs.append(df)
        total_rows += len(df)
        logging.info(f"  Read {len(df)} rows from {os.path.basename(path)} (running total {total_rows})")
        if sample and total_rows >= SAMPLE_ROW_CAP:
            logging.info(f"  --sample: reached {total_rows} rows, stopping read early.")
            break
    df = pd.concat(dfs, ignore_index=True)
    if sample and len(df) > SAMPLE_ROW_CAP:
        df = df.iloc[:SAMPLE_ROW_CAP].reset_index(drop=True)
    return df


def derive_binary_label(df, target_branch, labels):
    labels = {int(k): int(v) for k, v in labels.items()}
    n_before = len(df)
    df = df[df[target_branch].isin(labels.keys())].copy()
    df["y_binary"] = df[target_branch].map(labels)
    logging.info(f"Label filter: {n_before} events -> {len(df)} events with y in {list(labels.keys())}")
    for y_val, cls in labels.items():
        n = int((df[target_branch] == y_val).sum())
        logging.info(f"  y=={y_val} -> class {cls}: {n} events")
    return df


def balance_classes(df, method, random_state):
    df_class0 = df[df["y_binary"] == 0]
    df_class1 = df[df["y_binary"] == 1]
    logging.info(f"Before balancing: class 0 = {len(df_class0)}, class 1 = {len(df_class1)}")

    if method == "downsample":
        min_size = min(len(df_class0), len(df_class1))
        df_class0 = df_class0.sample(n=min_size, random_state=random_state)
        df_class1 = df_class1.sample(n=min_size, random_state=random_state)
        df = pd.concat([df_class0, df_class1], ignore_index=True)
        logging.info(f"Downsampled to {min_size} events per class ({len(df)} total).")
        return df, None
    elif method == "scale_pos_weight":
        logging.info("scale_pos_weight balancing: no downsampling; ratio computed after train/test split.")
        return df, "scale_pos_weight"
    else:
        raise ValueError(f"Unknown ClassBalancing.method: {method}")


def build_preprocessor(float_features, integer_features):
    return ColumnTransformer(
        transformers=[
            ("float_imputer", SimpleImputer(strategy="mean"), float_features),
            ("int_imputer", SimpleImputer(strategy="median"), integer_features),
        ],
        verbose_feature_names_out=False,
    ).set_output(transform="pandas")


def make_xgb(fixed_params, regularization_params, extra=None):
    params = dict(fixed_params)
    params.update(regularization_params)
    if extra:
        params.update(extra)
    return XGBClassifier(**params)


def run_grid_search(X_train, y_train, fixed_params, regularization_params, param_grid, grid_cfg, extra_params=None):
    base_bdt = make_xgb(fixed_params, regularization_params, extra_params)
    n_combos = int(np.prod([len(v) for v in param_grid.values()]))
    logging.info(f"Parameter grid: {param_grid}")
    logging.info(f"Total combinations to test: {n_combos} (x cv={grid_cfg['cv']} folds)")
    grid_search = GridSearchCV(
        estimator=base_bdt,
        param_grid=param_grid,
        cv=grid_cfg["cv"],
        scoring=grid_cfg["scoring"],
        n_jobs=grid_cfg["n_jobs"],
        verbose=grid_cfg["verbose"],
    )
    grid_search.fit(X_train, y_train)
    logging.info(f"Grid search completed! Best parameters: {grid_search.best_params_}")
    logging.info(f"Best cross-validation {grid_cfg['scoring']}: {grid_search.best_score_:.4f}")
    return grid_search


def evaluate(bdt, X_test, y_test):
    y_pred_proba = bdt.predict_proba(X_test)[:, 1]
    y_pred = bdt.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    auc_score = roc_auc_score(y_test, y_pred_proba)
    cm = confusion_matrix(y_test, y_pred)
    logging.info(f"Model accuracy: {accuracy:.4f}, AUC: {auc_score:.4f}")
    logging.info(f"Confusion matrix:\n{cm}")
    return y_pred, y_pred_proba, accuracy, auc_score, cm


def compute_importances(bdt, features, X_test, y_test, perm_cfg, output_dir):
    importance_df = pd.DataFrame({
        "feature": features,
        "importance": bdt.feature_importances_,
    }).sort_values("importance", ascending=False)
    importance_df.to_csv(output_dir / "feature_importance.csv", index=False)
    logging.info("Built-in (gain) feature importance:\n" + importance_df.to_string(index=False))

    perm = permutation_importance(
        bdt, X_test, y_test,
        n_repeats=perm_cfg["n_repeats"],
        random_state=perm_cfg["random_state"],
        scoring=perm_cfg["scoring"],
        n_jobs=-1,
    )
    perm_df = pd.DataFrame({
        "feature": features,
        "importance_mean": perm.importances_mean,
        "importance_std": perm.importances_std,
    }).sort_values("importance_mean", ascending=False)
    perm_df.to_csv(output_dir / "permutation_importance.csv", index=False)
    logging.info("Permutation importance:\n" + perm_df.to_string(index=False))

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
    imp_sorted = importance_df.sort_values("importance")
    ax1.barh(imp_sorted["feature"], imp_sorted["importance"])
    ax1.set_xlabel("Importance"); ax1.set_ylabel("Feature")
    ax1.set_title("Built-in Feature Importance (gain)", fontweight="bold")
    ax1.grid(axis="x", alpha=0.3)

    perm_sorted = perm_df.sort_values("importance_mean")
    ax2.barh(perm_sorted["feature"], perm_sorted["importance_mean"],
             xerr=perm_sorted["importance_std"], capsize=3)
    ax2.axvline(x=0, color="red", linestyle="--", linewidth=1, label="Zero importance")
    ax2.set_xlabel("Drop in ROC-AUC"); ax2.set_ylabel("Feature")
    ax2.set_title("Permutation Feature Importance", fontweight="bold")
    ax2.legend(); ax2.grid(axis="x", alpha=0.3)

    plt.tight_layout()
    plt.savefig(output_dir / "feature_importance_comparison.png", dpi=300, bbox_inches="tight")
    plt.close(fig)

    return importance_df, perm_df


def save_roc_curve(y_test, y_pred_proba, auc_score, output_dir, title):
    fpr, tpr, _ = roc_curve(y_test, y_pred_proba)
    fig, ax = plt.subplots(figsize=(6, 6))
    ax.plot(fpr, tpr, label=f"AUC = {auc_score:.4f}")
    ax.plot([0, 1], [0, 1], linestyle="--", color="gray", label="Random")
    ax.set_xlabel("False Positive Rate"); ax.set_ylabel("True Positive Rate")
    ax.set_title(title); ax.legend()
    plt.tight_layout()
    plt.savefig(output_dir / "roc_curve.png", dpi=300, bbox_inches="tight")
    plt.close(fig)


def save_scores(df_test, y_test, y_pred_proba, y_pred, target_branch, out_path):
    df_scores = pd.DataFrame({
        "y_original": df_test[target_branch].values,
        "y_binary": y_test.values,
        "bdt_score": y_pred_proba,
        "prediction": y_pred,
    })
    df_scores.to_csv(out_path, index=False)
    logging.info(f"Scores saved to {out_path}")


def main():
    parser = argparse.ArgumentParser(description="Train the qqbar-vs-gg XGBoost classifier for one era.")
    parser.add_argument("--datasetJSON", required=True, help="Path to Parquet_{tag}_{era}_datasets.json")
    parser.add_argument("--trainingConfig", required=True, help="Path to training_config.yaml")
    parser.add_argument("--outputDir", required=True, help="Directory to write all training artifacts")
    parser.add_argument("--era", required=True, help="Era label (logging/manifest only)")
    parser.add_argument("--parquetHash", default=None,
                        help="Config hash of the extraction run being trained on (manifest/provenance only)")
    parser.add_argument("--sample", action="store_true",
                        help=f"Cap total rows read to ~{SAMPLE_ROW_CAP} for a fast mechanical smoke pass")
    parser.add_argument("--force", action="store_true",
                        help="Accepted for symmetry with extractParquet.py; run_all.py already gates on "
                             "this before invoking, so it has no effect inside this script.")
    args = parser.parse_args()

    output_dir = Path(args.outputDir)
    output_dir.mkdir(parents=True, exist_ok=True)
    setup_logging(args.era, output_dir)

    logging.info(f"Starting BDT training for era {args.era}")
    logging.info(f"Arguments: {vars(args)}")

    training_hash = utils.compute_config_hash(args.trainingConfig)
    logging.info(f"Training config hash: {training_hash}")

    cfg = utils.load_config(args.trainingConfig)
    features = list(cfg["Features"])
    integer_features = list(cfg.get("IntegerFeatures", []))
    float_features = [f for f in features if f not in integer_features]
    ordered_features = float_features + integer_features
    target_branch = cfg["TargetBranch"]

    files = resolve_parquet_files(args.datasetJSON, cfg["TrainingSample"])
    logging.info(f"Found {len(files)} parquet part file(s) for {cfg['TrainingSample']}")

    logging.info("Reading parquet files...")
    df = read_parquet_files(files, features + [target_branch], sample=args.sample)
    logging.info(f"Total events read: {len(df)}")

    df = derive_binary_label(df, target_branch, cfg["Labels"])
    df, balancing_mode = balance_classes(df, cfg["ClassBalancing"]["method"], cfg["ClassBalancing"]["random_state"])

    split_cfg = cfg["Split"]
    df_train, df_test = train_test_split(
        df, test_size=split_cfg["test_size"], random_state=split_cfg["random_state"],
        stratify=df["y_binary"] if split_cfg.get("stratify", True) else None,
    )
    logging.info(f"Train: {len(df_train)} events (class0={sum(df_train['y_binary'] == 0)}, "
                 f"class1={sum(df_train['y_binary'] == 1)})")
    logging.info(f"Test:  {len(df_test)} events (class0={sum(df_test['y_binary'] == 0)}, "
                 f"class1={sum(df_test['y_binary'] == 1)})")

    extra_params = {}
    if balancing_mode == "scale_pos_weight":
        n0 = int((df_train["y_binary"] == 0).sum())
        n1 = int((df_train["y_binary"] == 1).sum())
        spw = n0 / n1 if n1 > 0 else 1.0
        extra_params["scale_pos_weight"] = spw
        logging.info(f"scale_pos_weight computed from train split: {spw:.4f}")

    preprocessor = build_preprocessor(float_features, integer_features)
    X_train = preprocessor.fit_transform(df_train[features])
    X_test = preprocessor.transform(df_test[features])
    # Integer feature columns come back as imputed floats (e.g. median of an
    # even-count column); round + cast back so they stay semantically counts.
    # sel_nJet/sel_nbjet are always filled by SelectedObjectsProducer (even
    # as 0), so this imputer branch should essentially never actually fire --
    # it exists for defensive completeness, not because missingness is expected.
    for col in integer_features:
        X_train[col] = X_train[col].round().astype(int)
        X_test[col] = X_test[col].round().astype(int)
    y_train = df_train["y_binary"]
    y_test = df_test["y_binary"]

    logging.info("\n" + "=" * 60)
    logging.info("GRID SEARCH (full feature set)")
    logging.info("=" * 60)
    grid_search = run_grid_search(
        X_train, y_train, cfg["FixedParams"], cfg["RegularizationParams"],
        cfg["ParamGrid"], cfg["GridSearch"], extra_params,
    )
    bdt = grid_search.best_estimator_
    y_pred, y_pred_proba, accuracy, auc_score, cm = evaluate(bdt, X_test, y_test)

    logging.info("\n" + "=" * 60)
    logging.info("FEATURE IMPORTANCE ANALYSIS")
    logging.info("=" * 60)
    importance_df, perm_df = compute_importances(
        bdt, ordered_features, X_test, y_test, cfg["FeatureSelection"]["permutation_importance"], output_dir,
    )
    save_roc_curve(y_test, y_pred_proba, auc_score, output_dir, f"ROC curve -- {args.era} (full model)")

    best_params = {
        "era": args.era,
        "parquetHash": args.parquetHash,
        "training_hash": training_hash,
        "n_train": len(df_train),
        "n_test": len(df_test),
        "class_balance": {"method": cfg["ClassBalancing"]["method"], **extra_params},
        "best_parameters": grid_search.best_params_,
        "best_cv_auc": float(grid_search.best_score_),
        "test_accuracy": float(accuracy),
        "test_auc": float(auc_score),
        "confusion_matrix": cm.tolist(),
        "timestamp": datetime.now().isoformat(),
    }
    with open(output_dir / "best_params.json", "w") as f:
        json.dump(best_params, f, indent=4)
    logging.info(f"Best parameters saved to {output_dir / 'best_params.json'}")

    joblib.dump({"model": bdt, "imputer": preprocessor, "features": ordered_features},
                output_dir / "bdt_model.pkl")
    logging.info(f"Full model saved to {output_dir / 'bdt_model.pkl'}")

    save_scores(df_test, y_test, y_pred_proba, y_pred, target_branch, output_dir / "scores.csv")

    select_n = cfg.get("FeatureSelection", {}).get("select_features")
    if select_n:
        logging.info("\n" + "=" * 60)
        logging.info(f"RETRAINING WITH TOP {select_n} FEATURES")
        logging.info("=" * 60)
        select_n = min(select_n, len(ordered_features))
        selected_features = perm_df.head(select_n)["feature"].tolist()
        removed_features = [f for f in ordered_features if f not in selected_features]
        logging.info(f"Selected features ({select_n}): {selected_features}")
        logging.info(f"Removed features ({len(removed_features)}): {removed_features}")

        # Preserve float-then-int order (matching ordered_features) for the
        # actual data slicing and for a standalone reduced-model preprocessor,
        # keeping selected_features (importance-ranked) just for logging/JSON.
        selected_ordered = [f for f in ordered_features if f in selected_features]
        selected_float = [f for f in float_features if f in selected_features]
        selected_int = [f for f in integer_features if f in selected_features]

        # Re-slicing the already-imputed X_train/X_test would leave
        # bdt_model_reduced.pkl's imputer expecting the FULL feature set as
        # input (inconsistent with its own bundled `features` list) -- fit a
        # dedicated reduced-feature preprocessor instead, so the reduced
        # bundle is self-contained: feed it df[features], get model-ready
        # input back. Per-column imputation statistics (mean/median) are
        # unaffected by which other columns are present, so this reproduces
        # the same imputed values as the full preprocessor did.
        preprocessor_reduced = build_preprocessor(selected_float, selected_int)
        X_train_reduced = preprocessor_reduced.fit_transform(df_train[selected_ordered])
        X_test_reduced = preprocessor_reduced.transform(df_test[selected_ordered])
        for col in selected_int:
            X_train_reduced[col] = X_train_reduced[col].round().astype(int)
            X_test_reduced[col] = X_test_reduced[col].round().astype(int)

        grid_search_reduced = run_grid_search(
            X_train_reduced, y_train, cfg["FixedParams"], cfg["RegularizationParams"],
            cfg["ParamGrid"], cfg["GridSearch"], extra_params,
        )
        bdt_reduced = grid_search_reduced.best_estimator_
        y_pred_r, y_pred_proba_r, accuracy_r, auc_r, cm_r = evaluate(bdt_reduced, X_test_reduced, y_test)

        auc_diff = auc_r - auc_score
        threshold = cfg["FeatureSelection"]["performance_drop_threshold"]
        verdict = "minimal" if abs(auc_diff) < threshold else "significant"
        logging.info(f"Full model test AUC: {auc_score:.4f}; reduced model test AUC: {auc_r:.4f} "
                     f"(delta {auc_diff:+.4f}) -- {verdict} (threshold {threshold}).")

        reduced_params = {
            "era": args.era,
            "selected_features": selected_features,
            "removed_features": removed_features,
            "n_features": select_n,
            "best_parameters": grid_search_reduced.best_params_,
            "best_cv_auc": float(grid_search_reduced.best_score_),
            "test_accuracy": float(accuracy_r),
            "test_auc": float(auc_r),
            "full_model_test_auc": float(auc_score),
            "auc_difference": float(auc_diff),
        }
        with open(output_dir / "reduced_model_params.json", "w") as f:
            json.dump(reduced_params, f, indent=4)
        logging.info(f"Reduced model parameters saved to {output_dir / 'reduced_model_params.json'}")

        joblib.dump({"model": bdt_reduced, "imputer": preprocessor_reduced, "features": selected_ordered},
                    output_dir / "bdt_model_reduced.pkl")
        logging.info(f"Reduced model saved to {output_dir / 'bdt_model_reduced.pkl'}")

        save_scores(df_test, y_test, y_pred_proba_r, y_pred_r, target_branch,
                    output_dir / "scores_reduced.csv")

    manifest = utils.create_output_metadata(training_hash, "trainBDT.py")
    manifest.update({
        "era": args.era,
        "parquetHash": args.parquetHash,
        "datasetJSON": args.datasetJSON,
        "source_parquet_files": files,
        "n_events_after_label_filter_and_balancing": int(len(df)),
        "features": ordered_features,
    })
    with open(output_dir / "run_manifest.json", "w") as f:
        json.dump(manifest, f, indent=4)

    logging.info(f"Finished training for era {args.era}.")


if __name__ == "__main__":
    main()
