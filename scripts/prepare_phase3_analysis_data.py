import pandas as pd
import numpy as np
import argparse
import sys
import os
from tqdm import tqdm

def main():
    parser = argparse.ArgumentParser(description="Convert Phase 3 CSV Results to Analysis-Ready Parquet")
    parser.add_argument("--input", required=True, help="Path to raw Phase 3 CSV results")
    parser.add_argument("--output", required=True, help="Path to output Parquet file")
    parser.add_argument("--sim-output", help="Path to output Similarity Matrix CSV")
    
    args = parser.parse_args()
    
    input_path = args.input
    output_path = args.output
    sim_output_path = args.sim_output
    
    if not os.path.exists(input_path):
        print(f"Error: Input file not found: {input_path}")
        sys.exit(1)
        
    try:
        print(f"Loading {input_path}...")
        df = pd.read_csv(input_path)
        print(f"Loaded {len(df)} rows.")
        
        # 1. Date Conversion
        if 'window_start_month' in df.columns:
            print("Converting 'window_start_month' to datetime 'date'...")
            df['date'] = pd.to_datetime(df['window_start_month'])
        else:
            print("Warning: 'window_start_month' column not found.")
            
        # 2. Metric Aliasing
        candidates = [
            'baseline_penultimate_corrected',
            'baseline_penultimate_raw',
            'baseline_penultimate'
        ]
        
        found_suffix = None
        for cand in candidates:
            if f'drift_{cand}' in df.columns:
                found_suffix = cand
                break
        
        if found_suffix:
            print(f"Selected primary metric variant: {found_suffix}")
            
            # Map key metrics
            if f'k_{found_suffix}' in df.columns:
                df['k'] = df[f'k_{found_suffix}']
                df['intrinsic_dimension_k'] = df[f'k_{found_suffix}']
            
            if f'drift_{found_suffix}' in df.columns:
                df['drift'] = df[f'drift_{found_suffix}']
                df['centroid_drift'] = df[f'drift_{found_suffix}']
                
            # Procrustes and Similarity Inference
            if f'procrustes_{found_suffix}' in df.columns:
                df['procrustes'] = df[f'procrustes_{found_suffix}']
                df['similarity'] = 1.0 - df[f'procrustes_{found_suffix}'].clip(0, 1)

            # Projections
            dims = ['funcional', 'social', 'afectiva']
            for dim in dims:
                col_c = f'centroid_proj_{dim}_{found_suffix}'
                if col_c in df.columns:
                    df[f'score_centroid_{dim}_contextual'] = df[col_c]
                    
                col_s = f'subspace_proj_{dim}_{found_suffix}'
                if col_s in df.columns:
                    df[f'score_{dim}_contextual'] = df[col_s]

            # 4. Generate Similarity Matrix (if requested)
            if sim_output_path:
                print("Generating Similarity Matrix...")
                path_col = f'subspace_path_{found_suffix}'
                
                if path_col not in df.columns:
                    print(f"Error: Subspace path column '{path_col}' not found. Cannot generate similarity matrix.")
                else:
                    # Resolve Paths
                    # The CSV paths might be relative to project root or results dir.
                    # User said npz files are in `results/phase3/mh_strict_covidOK_2020-03_2021-03_w3_FINAL_CLEANED`
                    # The CSV paths usually look like `results\phase3\...\artifacts\subspaces\window_...npz`
                    # We need to make sure we can find them.
                    
                    # Assume running from project root (where scripts/ is)
                    # Use the input CSV directory as a hint?
                    # Or just try to open them as is (if relative to CWD)
                    
                    windows = df['window_start_month'].unique().tolist()
                    sim_df = pd.DataFrame(np.zeros((len(windows), len(windows))), index=windows, columns=windows)
                    u_matrices = {}
                    
                    print(f"Loading {len(windows)} subspaces...")
                    
                    # Pre-load U matrices
                    for idx, row in tqdm(df.iterrows(), total=len(df)):
                        win = row['window_start_month']
                        rel_path = row[path_col]
                        
                        # Try to resolve path
                        if os.path.exists(rel_path):
                            path = rel_path
                        # Fallback: relative to basedir input CSV?
                        # Input: results/phase3/.../phase3_results.csv
                        # Path in CSV: results\phase3\...\artifacts\subspaces\...
                        # They likely align if running from root.
                        else:
                             # Try constructing from known structure if 'results' in path
                             # If running from root, and file is in results/..., it should be fine.
                             # If not found, warn.
                             print(f"Warning: Subspace file not found: {rel_path}")
                             continue
                             
                        try:
                            with np.load(path) as data:
                                u_matrices[win] = data['U']
                        except Exception as e:
                            print(f"Error loading {path}: {e}")

                    print("Computing pairwise similarities...")
                    # Compute Matrix
                    # Sim(A, B) = ||A^T B||_F^2 / k_A (assuming k_A approx k_B or normalized by self?)
                    # Reporte_Integral uses: np.linalg.norm(Ui.T @ Uj)**2 / ki
                    
                    for win_i in list(u_matrices.keys()):
                        Ui = u_matrices[win_i]
                        ki = Ui.shape[1] 
                        # Note: In SVD U is (d, k).
                        
                        for win_j in list(u_matrices.keys()):
                            Uj = u_matrices[win_j]
                            
                            # Overlap Metric
                            # Grassmanian Kernel or Projection Metric
                            # Trace(Ui @ Ui.T @ Uj @ Uj.T) = || Ui.T @ Uj ||_F^2
                            # Normalized by dim k (max possible overlap is k)
                            overlap = np.linalg.norm(Ui.T @ Uj)**2
                            sim_val = overlap / ki
                            
                            sim_df.loc[win_i, win_j] = sim_val
                            
                    print(f"Saving Similarity Matrix to {sim_output_path}...")
                    os.makedirs(os.path.dirname(os.path.abspath(sim_output_path)), exist_ok=True)
                    sim_df.to_csv(sim_output_path)

        else:
            print("Warning: No suitable metric variant found (baseline/dapt).")
            
        # 3. Export Parquet
        print(f"Exporting Results to {output_path}...")
        os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
        df.to_parquet(output_path, index=False)
        print("Done.")
        
    except Exception as e:
        print(f"Error during conversion: {e}")
        # Print stack trace for debugging
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
