# Mathematical Framework

## Overview

To model the evolution of mental health discourse as a dynamic, multidimensional construct, the following algebraic pipeline was formalized. This approach shifts from traditional single-point vector representations to **semantic subspaces**, enabling the capture of structural shifts in meaning.

## Temporal Windowing

The dataset is aggregated using a **3-month rolling time window** with a **1-month sliding step**, producing **11 temporal windows**. A threshold of N>1,000 keyword occurrences ensures sufficient data density for robust SVD decomposition relative to BETO's dimensionality (d=768).

## Subspace Construction (SVD)

For each window *t*, an embedding matrix **X_t** ∈ ℝ^(n×d) is constructed from all keyword occurrences. Singular Value Decomposition (SVD) identifies the principal axes of variation:

```
X_t = U_t · Σ_t · V_t^T
```

Where:
- **U_t** contains left singular vectors (word positions — *who is involved*)
- **Σ_t** contains singular values (pattern strength — *how much it matters*)
- **V_t^T** contains right singular vectors forming an orthogonal basis (*where/when patterns cluster*)

Each identified frame is statistically independent via orthogonality, allowing isolation of dimensions without overlap.

### SVD Convergence Fix

During Phase 3, a convergence failure occurred in window 7 (September–November 2020) using NumPy's default LAPACK driver (`gesdd`). A **hybrid fallback mechanism** was implemented:
1. Try fast `gesdd` driver (~99% of cases)
2. If `LinAlgError`, fallback to SciPy's numerically stable `gesvd` driver

This guarantees convergence for rectangular matrices where divide-and-conquer methods fail.

## Framing Metrics

### 1. Semantic Drift (Grassmannian Distance)

Measures the rate of structural semantic change between consecutive windows:

```
d(S_t, S_{t+1}) = √(Σ sin²(θ_i))
```

Where θ_i are the principal angles between the orthonormal bases. Unlike cosine similarity on centroids, Grassmannian distance is invariant to orthogonal rotations, providing a theoretically founded comparison across time periods.

### 2. Shannon Entropy

Measures the concentration/ambiguity of discourse within each subspace:

```
H = -Σ p_i · log₂(p_i)
```

Where p_i are the normalized singular values. Higher entropy = more semantic ambiguity (many different contextual meanings). Lower entropy = convergence around a stable discourse.

### 3. Intrinsic Dimensionality (k)

The minimum number of principal components explaining meaningful variance, determined via **Horn's Parallel Analysis**:

```
k = max{i : λ_i > λ_i^random}
```

This separates meaningful discourse structure from linguistic noise by selecting only empirical eigenvalues that exceed those from a randomized baseline.

### 4. Anchor Projections

Subspaces are projected onto three theoretically predefined axes to quantify framing orientation:

- **Affective**: Internal psychological states (anxiety, depression, fear, trauma)
- **Social**: Interpersonal/community context (isolation, support, family dynamics)
- **Functional**: Systemic/institutional aspects (healthcare, policy, resources)

Two projection methods are computed:

**Centroid Projection** (mean vector):
```
proj_centroid = cos(μ_t, a_D)
```

**Subspace Projection** (full orthonormal basis):
```
proj_subspace = ||A_D^T · U_t||_F
```

The subspace projection captures the "structural" alignment of the most energetic themes, unlike centroids which collapse all variation into a single point.

## Anchor Definitions

Anchors are defined in `data/metadata/anchors/dimensiones_ancla_mh_es_covid_FSA.json` with three dimensions (Functional, Social, Affective), each containing contextualized Spanish sentences grounding the semantic poles.
