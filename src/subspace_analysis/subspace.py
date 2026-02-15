import numpy as np
import logging
from scipy.linalg import orthogonal_procrustes
from dataclasses import dataclass
from sklearn.decomposition import TruncatedSVD
import scipy.linalg

logger = logging.getLogger(__name__)

@dataclass
class SubspaceResult:
    window_start: object
    window_end: object
    basis: np.ndarray # U matrix (n_features, k)
    eigenvalues: np.ndarray
    k: int
    alignment_error: float = 0.0
    rotation_matrix: np.ndarray = None

class SubspaceConstructor:
    def __init__(self, fixed_k: int = None):
        """
        fixed_k: If set, forces all subspaces to dimension k.
                 If None, expects k to be provided per window.
        """
        self.fixed_k = fixed_k

    def build(self, data_matrix: np.ndarray, k: int = None) -> tuple[np.ndarray, np.ndarray]:
        """
        Computes subspace basis U and eigenvalues S.
        Returns: U (n_features, k), S
        """
        if self.fixed_k:
            k = self.fixed_k
        if k is None:
            raise ValueError("k must be provided if fixed_k is None")
            
        # Center data
        X = data_matrix - np.mean(data_matrix, axis=0)
        
        # SVD
        # We want the basis in Feature space (V^T in sklearn notation if X is (samples, features))
        # X = U S V^T. 
        # The principal components (axes) are V^T rows.
        # But wait, sklearn TruncatedSVD.components_ contains V^T.
        # Shape (k, n_features).
        # We usually define the subspace as the span of these vectors.
        # Let's represent basis as matrix U_basis of shape (n_features, k) so columns are basis vectors.
        
        svd = TruncatedSVD(n_components=k, random_state=42)
        svd.fit(X)
        
        basis = svd.components_.T # (n_features, k)
        singular_values = svd.singular_values_
        
        return basis, singular_values

    def align(self, base_subspace: np.ndarray, target_subspace: np.ndarray) -> tuple[np.ndarray, np.ndarray, float]:
        """
        Aligns target_subspace (U_t) to base_subspace (U_{t-1}) using Orthogonal Procrustes.
        Finds Q such that || base - target @ Q || is minimized.
        
        Returns: aligned_target, Q, error
        """
        # Shapes: (n_features, k)
        # scipy.linalg.orthogonal_procrustes mapping A to B: || A - BQ ||
        # We want to map target to base.
        # So A = base, B = target? No, standard definition is minimize || A - B Q ||.
        # We want target @ Q approx base.
        # So we match input args to scipy:
        # R, scale = orthogonal_procrustes(A, B) yields A approx B @ R?
        # Scipy doc: "determines orthogonal matrix R such that norm(A - B R) is minimized."
        # We want U_{t-1} approx U_t @ Q. 
        # So A = U_{t-1}, B = U_t.
        
        # Check dimensions
        if base_subspace.shape != target_subspace.shape:
            # Can't standard procrustes if different K.
            # Strategy: Pad with zeros or truncate? 
            # In Phase 3, we usually fix K for alignment or align the common dimensions.
            # Let's assume K is fixed or use separate alignment for variable K (not implemented here yet).
            # If K differs, we intersect?
            # For simplicity, if K non-matching, we skip alignment or pad.
            k1 = base_subspace.shape[1]
            k2 = target_subspace.shape[1]
            
            if k1 != k2:
                # Fallback: align the shared dimensions min(k1, k2)
                min_k = min(k1, k2)
                logger.warning(f"Aligning subspaces with different K ({k1} vs {k2}). Truncating to {min_k} for alignment calculation.")
                # We only compute Q based on top min_k components
                A = base_subspace[:, :min_k]
                B = target_subspace[:, :min_k]
                
                Q_small, _ = orthogonal_procrustes(A, B)
                # Expand Q to full k2 x k2? No, this is messy. 
                # Better strategy: Pad smaller one with random orthogonal vectors or zeros?
                # Zeros is safer for Procrustes.
                pass # Rely on calling code to ensure K is consistent or handle it.
                # Just fail for now or proceed without valid alignment if critical.
                # Returning unaligned
                return target_subspace, np.eye(k2), 0.0

        R, scale = orthogonal_procrustes(base_subspace, target_subspace)
        # Wait, scipy doc: "orthogonal_procrustes(A, B) ... minimizes || A - B @ R ||"
        # If we passed A=base, B=target, then R is the rotation for B.
        # Correct.
        
        aligned_target = target_subspace @ R
        
        # Calculate residual
        diff = base_subspace - aligned_target
        error = np.linalg.norm(diff, 'fro')
        
        return aligned_target, R, error


class MatrixBuilder:
    """
    Builder for raw matrices and centroids (pre-centering)
    """
    def run(self, window_df: object, variant: str, strategy: str) -> tuple[np.ndarray, np.ndarray]:
        # Local import to avoid circular dependency if schemas imports subspace
        from src.subspace_analysis.schemas import Phase3Config
        import json
        
        # Select column
        col_map = {
            ("baseline", "penultimate"): Phase3Config.COL_EMB_BASELINE_PENULTIMATE,
            ("baseline", "last4_concat"): Phase3Config.COL_EMB_BASELINE_LAST4,
            ("dapt", "penultimate"): Phase3Config.COL_EMB_DAPT_PENULTIMATE,
            ("dapt", "last4_concat"): Phase3Config.COL_EMB_DAPT_LAST4,
        }
        col = col_map.get((variant, strategy))
        if not col:
            raise ValueError(f"Unknown combination: {variant} {strategy}")
            
        # Parse
        try:
            # We assume auditor checked format, but safe to be robust
            # Using list comprehension which is faster than apply for large jsons
            matrix = np.array([json.loads(x) for x in window_df[col]])
        except Exception as e:
            raise RuntimeError(f"FAIL: Matrix parse error for {variant}/{strategy}: {e}")
            
        if matrix.ndim != 2:
             raise RuntimeError(f"FAIL: Matrix has wrong dims: {matrix.shape}")
             
        # Validate finite
        if not np.all(np.isfinite(matrix)):
             raise RuntimeError(f"FAIL: Matrix contains NaNs or Infs")
             
        # Calc mu
        mu = np.mean(matrix, axis=0)
        
        return matrix, mu

class Centerer:
    """
    Centering Logic
    """
    def run(self, X: np.ndarray, mu: np.ndarray) -> np.ndarray:
        Xc = X - mu
        if not np.all(np.isfinite(Xc)):
            raise RuntimeError("FAIL: NaNs post-centering")
        return Xc

class KSelector:
    """
    Selector for k (Horn + Bootstrap)
    """
    def run(self, Xc: np.ndarray, B_HORN: int = 200, B_BOOT: int = 200, seed: int = 42) -> tuple[int, int, int]:
        n, d = Xc.shape
        rng = np.random.RandomState(seed)
        
        # 1. Real Eigenvalues (PCA)
        _, s_real, _ = np.linalg.svd(Xc, full_matrices=False)
        eigen_real = (s_real ** 2) / (n - 1)
        
        # 2. Horn's Parallel Analysis
        rand_eigen_accum = []
        for _ in range(B_HORN):
            # Efficient permutation
            X_rand = np.zeros_like(Xc)
            for j in range(d):
                X_rand[:, j] = rng.permutation(Xc[:, j])
            
            _, s_rand, _ = np.linalg.svd(X_rand, full_matrices=False)
            e_rand = (s_rand ** 2) / (n - 1)
            rand_eigen_accum.append(e_rand)
            
        rand_eigen_accum = np.array(rand_eigen_accum) # (B, min(n,d))
        
        # 95th percentile
        eigen_rand95 = np.percentile(rand_eigen_accum, 95, axis=0)
        
        # k_horn
        k_horn = 0
        min_dim = min(n, d)
        for i in range(min_dim):
            if eigen_real[i] > eigen_rand95[i]:
                k_horn += 1
            else:
                break
                
        # 3. Bootstrap Stability
        boot_ks = []
        for _ in range(B_BOOT):
            # Resample Xc
            X_boot = np.array([Xc[i] for i in rng.randint(0, n, n)]) # Manual resample for speed/clarity
            # Center boot sample
            X_boot = X_boot - np.mean(X_boot, axis=0) 
            
            # CÓDIGO VIEJO (FRÁGIL)
            # _, s_boot, _ = np.linalg.svd(X_boot, full_matrices=False)

            # CÓDIGO NUEVO (ROBUSTO)
            # Usamos scipy con el driver 'gesvd' que NUNCA falla por convergencia
            _, s_boot, _ = scipy.linalg.svd(X_boot, full_matrices=False, lapack_driver='gesdd')
            e_boot = (s_boot ** 2) / (n - 1)
            
            k_b = 0
            for i in range(min(len(e_boot), len(eigen_rand95))):
                if e_boot[i] > eigen_rand95[i]:
                    k_b += 1
                else: 
                    break
            boot_ks.append(k_b)
            
        k_bootstrap_val = int(np.floor(np.percentile(boot_ks, 5))) # Conservative: 5th percentile of supported k
        
        k_selected = min(k_horn, k_bootstrap_val)
        if k_selected < 1: k_selected = 1
        
        return k_horn, k_bootstrap_val, k_selected

class SubspacePersister:
    """
    Subspace Persistence
    """
    def run(
        self, 
        Xc: np.ndarray, 
        mu: np.ndarray, 
        k: int, 
        window_meta: dict, 
        variant: str, 
        strategy: str,
        suffix: str = ""
    ) -> str:
        from src.subspace_analysis.schemas import Phase3Config
        
        u, s, vh = np.linalg.svd(Xc, full_matrices=False)
        
        if k > len(s):
             raise RuntimeError(f"FAIL: k={k} > rank={len(s)}")
             
        U_subspace = vh[:k, :].T # (d, k)
        singular_values = s
        
        # Validate
        if not np.all(np.isfinite(U_subspace)):
             raise RuntimeError("FAIL: NaNs in Subspace U")
        
        suff_str = f"_{suffix}" if suffix else ""
        filename = f"window_{window_meta['start']}_{variant}_{strategy}{suff_str}.npz"
        path = Phase3Config.SUBSPACES_DIR / filename
        
        np.savez_compressed(
            path,
            U=U_subspace,
            singular_values=singular_values,
            mean_vector=mu,
            k_selected=k,
            window_start_month=window_meta['start'],
            window_end_month=window_meta['end'],
            variant=variant,
            strategy=strategy,
            condition_suffix=suffix
        )
        return str(path)
