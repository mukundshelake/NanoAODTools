import pandas as pd
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import accuracy_score, roc_auc_score, confusion_matrix, roc_curve, auc
from sklearn.inspection import permutation_importance
import matplotlib.pyplot as plt
import joblib
import numpy as np
from sklearn.impute import SimpleImputer
import os
import argparse
import logging
import uproot
import json

# Configure logging
def setup_logging(script_name, output_dir):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Create handlers
    console_handler = logging.StreamHandler()
    file_handler = logging.FileHandler(os.path.join(output_dir, f"{script_name}.log"))

    # Create formatters and add them to handlers
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    # Add handlers to the logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

def load_fileset_from_json(json_path):
    """Load fileset dictionary from JSON file.
    
    Args:
        json_path: Path to JSON file containing fileset structure
        
    Returns:
        Dictionary with structure {dataset: {process: {filepath: treename}}}
    """
    with open(json_path, 'r') as f:
        fileset = json.load(f)
    logging.info(f"Loaded fileset from {json_path}")
    return fileset

def read_root_files(fileset, branch_list, batch_size=50):
    """Read ROOT files from fileset, loading only specified branches.
    
    Args:
        fileset: Dictionary with structure {dataset: {process: {filepath: treename}}}
        branch_list: List of branch names to load
        batch_size: Number of files to process before concatenating (memory management)
        
    Returns:
        pandas DataFrame with all events from all files
    """
    dfs = []
    file_count = 0
    total_events = 0
    skipped_files = 0
    
    # Flatten the fileset structure to get all file paths
    all_files = []
    for dataset_name, processes in fileset.items():
        for process_name, files in processes.items():
            if 'SemiLeptonic' not in process_name:
                continue
            for filepath, treename in files.items():
                all_files.append((filepath, treename, dataset_name, process_name))
    
    logging.info(f"Found {len(all_files)} ROOT files to process")
    logging.info(f"Loading branches: {branch_list}")
    
    batch_dfs = []
    
    for filepath, treename, dataset_name, process_name in all_files:
        try:
            with uproot.open(f"{filepath}:{treename}") as tree:
                num_entries = tree.num_entries
                
                if num_entries == 0:
                    logging.warning(f"Skipping empty file: {filepath}")
                    skipped_files += 1
                    continue
                
                # Read only the specified branches
                df_temp = tree.arrays(branch_list, library="pd")
                batch_dfs.append(df_temp)
                
                file_count += 1
                total_events += len(df_temp)
                
                # Log progress every 10 files
                if file_count % 10 == 0:
                    logging.info(f"Processed {file_count}/{len(all_files)} files, loaded {total_events} events")
                
                # Concatenate in batches to manage memory
                if len(batch_dfs) >= batch_size:
                    dfs.append(pd.concat(batch_dfs, ignore_index=True))
                    batch_dfs = []
                    
        except Exception as e:
            logging.warning(f"Error reading {filepath}: {e}")
            skipped_files += 1
            continue
    
    # Concatenate any remaining batch
    if batch_dfs:
        dfs.append(pd.concat(batch_dfs, ignore_index=True))
    
    logging.info(f"Successfully processed {file_count} files")
    logging.info(f"Skipped {skipped_files} files due to errors or zero entries")
    logging.info(f"Total events loaded: {total_events}")
    
    # Final concatenation
    df_final = pd.concat(dfs, ignore_index=True)
    logging.info(f"Final DataFrame shape: {df_final.shape}")
    
    return df_final

def main():
    parser = argparse.ArgumentParser(description="Train BDT models on ROOT files.")
    parser.add_argument(
        '-j', '--json-file',
        type=str,
        required=True,
        help="Path to JSON file containing ROOT file paths (e.g., midNov_bdtvariables_parton_UL2016preVFP_dataFiles.json)"
    )
    parser.add_argument(
        '-o', '--output-dir',
        type=str,
        default='outputs/bdt',
        help="Output directory for models and results (default: outputs/bdt)"
    )
    parser.add_argument(
        '--select-features',
        type=int,
        default=None,
        help="Number of top features to select for retraining (optional). If specified, model will be retrained with only the N most important features."
    )
    args = parser.parse_args()

    outputDir = args.output_dir
    if not os.path.exists(outputDir):
        os.makedirs(outputDir)

    setup_logging(os.path.splitext(os.path.basename(__file__))[0], outputDir)

    # Load fileset from JSON
    fileset = load_fileset_from_json(args.json_file)
    
    # Define branches to load (y label + BDT variables)
    # obss = ['FW1', 'nJet', 'pTSum', 'P', 'A', 'p2in', 'Syy', 'Sxy']
    obss  = ["JetHT", "pTSum", "FW1", "FW2", "FW3", "AL", "Sxx", "Syy", "Sxy", "Sxz", "Syz", "Szz", "S", "P", "A", "p2in", "p2out"]
    branch_list = ['y'] + obss
    
    # Read ROOT files with only necessary branches
    logging.info("Reading ROOT files...")
    df = read_root_files(fileset, branch_list)
    
    # Convert to binary classification: y==0 vs y!=0
    df['y_binary'] = (df['y'] != 1).astype(int)
    
    df_class0 = df[df['y_binary'] == 0]
    df_class1 = df[df['y_binary'] == 1]
    
    logging.info(f"Number of events in class 0 (y==0) before balancing: {len(df_class0)}")
    logging.info(f"Number of events in class 1 (y!=0) before balancing: {len(df_class1)}")
    
    # Balance classes by downsampling to the size of the smaller class
    min_class_size = min(len(df_class0), len(df_class1))
    logging.info(f"Balancing classes to {min_class_size} events each")
    
    df_class0_balanced = df_class0.sample(n=min_class_size, random_state=42)
    df_class1_balanced = df_class1.sample(n=min_class_size, random_state=42)
    
    # Combine the balanced classes
    df = pd.concat([df_class0_balanced, df_class1_balanced], ignore_index=True)
    
    logging.info(f"Balanced dataset - class 0: {len(df[df['y_binary'] == 0])}, class 1: {len(df[df['y_binary'] == 1])}")
    logging.info(f"Total balanced events: {len(df)}")
    
    # Split the data into 30% for testing and 70% for training
    df_train, df_test = train_test_split(df, test_size=0.3, random_state=42, stratify=df['y_binary'])
    
    logging.info(f"Training set - class 0: {len(df_train[df_train['y_binary'] == 0])}, class 1: {len(df_train[df_train['y_binary'] == 1])}")
    logging.info(f"Testing set - class 0: {len(df_test[df_test['y_binary'] == 0])}, class 1: {len(df_test[df_test['y_binary'] == 1])}")
    
    # Select features and target variable
    X_train = df_train[obss]
    y_train = df_train['y_binary']
    
    X_test = df_test[obss]
    y_test = df_test['y_binary']
    
    # Check for NaNs
    logging.info("Checking for NaNs in the data...")
    logging.info(f"NaNs in X_train: {X_train.isna().sum().sum()}")
    logging.info(f"NaNs in X_test: {X_test.isna().sum().sum()}")
    
    # Impute missing values
    imputer = SimpleImputer(strategy='mean')
    X_train = imputer.fit_transform(X_train)
    X_test = imputer.transform(X_test)
    
    # Check for remaining NaNs after imputation
    logging.info("Checking for NaNs after imputation...")
    logging.info(f"NaNs in X_train: {np.isnan(X_train).sum()}")
    logging.info(f"NaNs in X_test: {np.isnan(X_test).sum()}")
    
    # Set up the parameter grid for hyperparameter optimization
    param_grid = {
        'n_estimators': [50, 100, 200],
        'learning_rate': [0.01, 0.1, 0.2],
        'max_depth': [3, 4, 5]
    }
    
    logging.info("Setting up GridSearchCV for hyperparameter optimization...")
    logging.info(f"Parameter grid: {param_grid}")
    logging.info(f"Total combinations to test: {np.prod([len(v) for v in param_grid.values()])}")
    
    # Set up the base model and GridSearchCV
    base_bdt = GradientBoostingClassifier(random_state=42)
    grid_search = GridSearchCV(
        estimator=base_bdt,
        param_grid=param_grid,
        cv=3,
        scoring='roc_auc',
        n_jobs=-1,
        verbose=2
    )
    
    # Train the model with grid search
    logging.info("Starting grid search (this may take a while)...")
    grid_search.fit(X_train, y_train)
    
    # Get the best model
    bdt = grid_search.best_estimator_
    
    logging.info("Grid search completed!")
    logging.info(f"Best parameters: {grid_search.best_params_}")
    logging.info(f"Best cross-validation AUC: {grid_search.best_score_:.4f}")
    
    # Predict probabilities for the testing dataset
    y_pred_proba = bdt.predict_proba(X_test)[:, 1]
    y_pred = bdt.predict(X_test)
    
    # Calculate accuracy and AUC
    accuracy = accuracy_score(y_test, y_pred)
    auc_score = roc_auc_score(y_test, y_pred_proba)
    
    logging.info(f"Model accuracy: {accuracy:.4f}")
    logging.info(f"Model AUC: {auc_score:.4f}")
    
    # ===== Feature Importance Analysis =====
    logging.info("\n" + "="*60)
    logging.info("FEATURE IMPORTANCE ANALYSIS")
    logging.info("="*60)
    
    # 1. Built-in Feature Importance
    logging.info("\n1. Built-in Feature Importance (Gini-based):")
    feature_importance = bdt.feature_importances_
    importance_df = pd.DataFrame({
        'feature': obss,
        'importance': feature_importance
    }).sort_values('importance', ascending=False)
    
    logging.info("\n" + importance_df.to_string(index=False))
    importance_df.to_csv(f'{outputDir}/feature_importance.csv', index=False)
    logging.info(f"\nSaved to {outputDir}/feature_importance.csv")
    
    # 2. Permutation Importance
    logging.info("\n2. Computing Permutation Importance (this may take a moment)...")
    perm_importance = permutation_importance(
        bdt, X_test, y_test,
        n_repeats=10,
        random_state=42,
        scoring='roc_auc',
        n_jobs=-1
    )
    
    perm_df = pd.DataFrame({
        'feature': obss,
        'importance_mean': perm_importance.importances_mean,
        'importance_std': perm_importance.importances_std
    }).sort_values('importance_mean', ascending=False)
    
    logging.info("\nPermutation Importance (sorted by mean):")
    logging.info("\n" + perm_df.to_string(index=False))
    perm_df.to_csv(f'{outputDir}/permutation_importance.csv', index=False)
    logging.info(f"\nSaved to {outputDir}/permutation_importance.csv")
    
    # Identify low-importance features
    low_importance_features = perm_df[perm_df['importance_mean'] <= 0.001]['feature'].tolist()
    if low_importance_features:
        logging.info(f"\n⚠️  Features with near-zero importance: {low_importance_features}")
    
    negative_importance_features = perm_df[perm_df['importance_mean'] < 0]['feature'].tolist()
    if negative_importance_features:
        logging.info(f"⚠️  Features with negative importance: {negative_importance_features}")
    
    # 3. Visualizations
    logging.info("\n3. Creating feature importance visualizations...")
    
    # Plot 1: Built-in importance
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
    
    importance_df_sorted = importance_df.sort_values('importance')
    ax1.barh(importance_df_sorted['feature'], importance_df_sorted['importance'])
    ax1.set_xlabel('Importance', fontsize=12)
    ax1.set_ylabel('Feature', fontsize=12)
    ax1.set_title('Built-in Feature Importance (Gini-based)', fontsize=14, fontweight='bold')
    ax1.grid(axis='x', alpha=0.3)
    
    # Plot 2: Permutation importance with error bars
    perm_df_sorted = perm_df.sort_values('importance_mean')
    ax2.barh(perm_df_sorted['feature'], perm_df_sorted['importance_mean'],
             xerr=perm_df_sorted['importance_std'], capsize=3)
    ax2.axvline(x=0, color='red', linestyle='--', linewidth=1, label='Zero importance')
    ax2.set_xlabel('Drop in ROC-AUC', fontsize=12)
    ax2.set_ylabel('Feature', fontsize=12)
    ax2.set_title('Permutation Feature Importance', fontsize=14, fontweight='bold')
    ax2.legend()
    ax2.grid(axis='x', alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(f'{outputDir}/feature_importance_comparison.png', dpi=300, bbox_inches='tight')
    logging.info(f"Saved visualization to {outputDir}/feature_importance_comparison.png")
    plt.close()
    
    # 4. Optional: Retrain with selected features
    if args.select_features is not None:
        n_features = args.select_features
        logging.info("\n" + "="*60)
        logging.info(f"RETRAINING WITH TOP {n_features} FEATURES")
        logging.info("="*60)
        
        if n_features > len(obss):
            logging.warning(f"Requested {n_features} features but only {len(obss)} available. Using all features.")
            n_features = len(obss)
        elif n_features < 1:
            logging.error(f"Invalid number of features: {n_features}. Must be >= 1.")
        else:
            # Select top N features based on permutation importance
            selected_features = perm_df.head(n_features)['feature'].tolist()
            removed_features = [f for f in obss if f not in selected_features]
            
            logging.info(f"\nSelected features ({n_features}): {selected_features}")
            logging.info(f"Removed features ({len(removed_features)}): {removed_features}")
            
            # Get feature indices
            selected_indices = [obss.index(f) for f in selected_features]
            
            # Prepare reduced feature sets
            X_train_reduced = X_train[:, selected_indices]
            X_test_reduced = X_test[:, selected_indices]
            
            logging.info(f"\nRetraining model with {n_features} features...")
            
            # Retrain with same grid search
            grid_search_reduced = GridSearchCV(
                estimator=GradientBoostingClassifier(random_state=42),
                param_grid=param_grid,
                cv=3,
                scoring='roc_auc',
                n_jobs=-1,
                verbose=2
            )
            
            grid_search_reduced.fit(X_train_reduced, y_train)
            bdt_reduced = grid_search_reduced.best_estimator_
            
            logging.info(f"\nReduced model best parameters: {grid_search_reduced.best_params_}")
            logging.info(f"Reduced model best CV AUC: {grid_search_reduced.best_score_:.4f}")
            
            # Evaluate reduced model
            y_pred_proba_reduced = bdt_reduced.predict_proba(X_test_reduced)[:, 1]
            y_pred_reduced = bdt_reduced.predict(X_test_reduced)
            
            accuracy_reduced = accuracy_score(y_test, y_pred_reduced)
            auc_score_reduced = roc_auc_score(y_test, y_pred_proba_reduced)
            
            logging.info(f"\nReduced model test accuracy: {accuracy_reduced:.4f}")
            logging.info(f"Reduced model test AUC: {auc_score_reduced:.4f}")
            
            # Compare models
            logging.info("\n" + "="*60)
            logging.info("MODEL COMPARISON")
            logging.info("="*60)
            logging.info(f"Full model ({len(obss)} features):")
            logging.info(f"  - CV AUC: {grid_search.best_score_:.4f}")
            logging.info(f"  - Test AUC: {auc_score:.4f}")
            logging.info(f"  - Test Accuracy: {accuracy:.4f}")
            logging.info(f"\nReduced model ({n_features} features):")
            logging.info(f"  - CV AUC: {grid_search_reduced.best_score_:.4f}")
            logging.info(f"  - Test AUC: {auc_score_reduced:.4f}")
            logging.info(f"  - Test Accuracy: {accuracy_reduced:.4f}")
            logging.info(f"\nPerformance change:")
            logging.info(f"  - ΔAUC: {auc_score_reduced - auc_score:+.4f}")
            logging.info(f"  - ΔAccuracy: {accuracy_reduced - accuracy:+.4f}")
            
            if abs(auc_score_reduced - auc_score) < 0.01:
                logging.info(f"\n✓ Performance drop is minimal (<0.01). Removed features are likely insignificant.")
            else:
                logging.info(f"\n⚠️  Performance change is significant (≥0.01). Consider keeping more features.")
            
            # Save reduced model results
            reduced_params = {
                'selected_features': selected_features,
                'removed_features': removed_features,
                'n_features': n_features,
                'best_parameters': grid_search_reduced.best_params_,
                'best_cv_auc': float(grid_search_reduced.best_score_),
                'test_accuracy': float(accuracy_reduced),
                'test_auc': float(auc_score_reduced),
                'full_model_test_auc': float(auc_score),
                'auc_difference': float(auc_score_reduced - auc_score)
            }
            with open(f'{outputDir}/reduced_model_params.json', 'w') as f:
                json.dump(reduced_params, f, indent=4)
            logging.info(f"\nReduced model parameters saved to {outputDir}/reduced_model_params.json")
            
            # Save reduced model
            joblib.dump(bdt_reduced, f'{outputDir}/bdt_model_reduced.pkl')
            logging.info(f"Reduced model saved to {outputDir}/bdt_model_reduced.pkl")
            
            # Save scores for reduced model
            df_scores_reduced = pd.DataFrame({
                'y_original': df_test['y'].values,
                'y_binary': y_test.values,
                'bdt_score': y_pred_proba_reduced,
                'prediction': y_pred_reduced
            })
            df_scores_reduced.to_csv(f'{outputDir}/scores_reduced.csv', index=False)
            logging.info(f"Reduced model scores saved to {outputDir}/scores_reduced.csv")
    
    logging.info("\n" + "="*60)
    logging.info("FEATURE IMPORTANCE ANALYSIS COMPLETE")
    logging.info("="*60 + "\n")
    
    # Create a DataFrame to store the actual y values and BDT score
    df_scores = pd.DataFrame({
        'y_original': df_test['y'].values,
        'y_binary': y_test.values,
        'bdt_score': y_pred_proba,
        'prediction': y_pred
    })
    
    logging.info("Scores DataFrame head:")
    logging.info(df_scores.head())
    
    # Save the DataFrame to a CSV file
    df_scores.to_csv(f'{outputDir}/scores.csv', index=False)
    logging.info(f"Scores saved to {outputDir}/scores.csv")
    
    # Save the best hyperparameters to a JSON file
    best_params = {
        'best_parameters': grid_search.best_params_,
        'best_cv_auc': float(grid_search.best_score_),
        'test_accuracy': float(accuracy),
        'test_auc': float(auc_score)
    }
    with open(f'{outputDir}/best_params.json', 'w') as f:
        json.dump(best_params, f, indent=4)
    logging.info(f"Best parameters saved to {outputDir}/best_params.json")
    
    # Save the best model
    joblib.dump(bdt, f'{outputDir}/bdt_model.pkl')
    logging.info("Best model saved")

if __name__ == '__main__':
    main()