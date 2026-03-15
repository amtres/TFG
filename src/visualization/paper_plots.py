import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
import os
import sys
from matplotlib.dates import DateFormatter
import matplotlib.dates as mdates


def setup_pub_style():
    """
    Configures matplotlib for publication-quality figures (Nature/Science style).
    """
    plt.style.use('seaborn-v0_8-paper')

    # Font settings
    plt.rcParams['font.family'] = 'serif'
    plt.rcParams['font.serif'] = ['Times New Roman', 'DejaVu Serif']
    plt.rcParams['font.size'] = 10
    plt.rcParams['axes.labelsize'] = 11
    plt.rcParams['axes.titlesize'] = 12
    plt.rcParams['xtick.labelsize'] = 9
    plt.rcParams['ytick.labelsize'] = 9
    plt.rcParams['legend.fontsize'] = 9

    # Figure settings
    plt.rcParams['figure.dpi'] = 300
    plt.rcParams['savefig.dpi'] = 300
    plt.rcParams['axes.grid'] = True
    plt.rcParams['grid.alpha'] = 0.3
    plt.rcParams['grid.linestyle'] = '--'

    # Colors
    plt.rcParams['axes.prop_cycle'] = plt.cycler(color=['#004e66', '#d14a2b', '#e5b22b', '#5d7667', '#8c9c90'])

# --- 1. GENERAL PLOTS ---

def _handle_date_axis(ax, df, date_col, categorical=True):
    """
    Helper to handle x-axis formatting.
    If categorical=True, plots against range(N) and labels with date strings.
    If categorical=False, assumes x-axis is datetime and uses DateFormatter.
    """
    if categorical:
        x_vals = np.arange(len(df))
        ax.set_xticks(x_vals)
        labels = df[date_col].astype(str).tolist()
        if len(labels) > 40:
             n = len(labels) // 40 + 1
             for i in range(len(labels)):
                 if i % n != 0:
                     labels[i] = ""
        ax.set_xticklabels(labels, rotation=45, ha='right')
        return x_vals, labels
    else:
        date_form = DateFormatter("%b %y")
        ax.xaxis.set_major_formatter(date_form)
        plt.xticks(rotation=45)
        return df[date_col], None

def plot_news_volume(df, date_col='date', count_col='volumen_yape', output_path=None):
    """Plots bar chart of news volume per window."""
    setup_pub_style()
    fig, ax = plt.subplots(figsize=(12, 5))
    df = df.sort_values(by=date_col)

    if count_col not in df.columns and 'count' in df.columns:
        count_col = 'count'

    x_vals, _ = _handle_date_axis(ax, df, date_col, categorical=True)

    ax.bar(x_vals, df[count_col], color='#2c3e50', alpha=0.7)
    ax.set_title('Distribucion de Noticias por Ventana Temporal')
    ax.set_ylabel('Cantidad de Noticias/Vectores')
    ax.set_xlabel('Ventana Temporal')

    if output_path: plt.savefig(output_path, bbox_inches='tight')
    plt.show()

# --- 2. PHASE 3: SUBSPACE PLOTS ---

def plot_similarity_matrix(sim_df, title="Matriz de Similitud Temporal (Subspace Overlap)", output_path=None):
    """Plots the window-to-window similarity matrix."""
    setup_pub_style()
    plt.figure(figsize=(10, 8))
    sns.heatmap(sim_df, cmap='viridis', square=True, vmin=0, vmax=1, cbar_kws={'label': 'Similitud ($Tr(U_i^T U_j)$)'})

    plt.title(title)
    plt.xlabel('Ventana Temporal Destino')
    plt.ylabel('Ventana Temporal Origen')

    if len(sim_df) > 40:
        ticks = np.arange(0, len(sim_df), 3)
        plt.xticks(ticks + 0.5, sim_df.columns[ticks], rotation=90, fontsize=8)
        plt.yticks(ticks + 0.5, sim_df.index[ticks], rotation=0, fontsize=8)
    else:
        plt.xticks(rotation=90)
        plt.yticks(rotation=0)

    plt.tight_layout()
    if output_path: plt.savefig(output_path, bbox_inches='tight')
    plt.show()

def plot_complexity_evolution(df, date_col='date', k_col='k', drift_col='drift', output_path=None):
    """Plots stacked evolution of Intrinsic Dimension (K) and Semantic Drift."""
    setup_pub_style()
    fig, axes = plt.subplots(2, 1, figsize=(12, 8), sharex=True)

    df = df.sort_values(by=date_col)
    x_vals, labels = _handle_date_axis(axes[1], df, date_col, categorical=True)

    axes[0].set_xticks(x_vals)
    axes[0].set_xticklabels([])

    axes[0].plot(x_vals, df[k_col], marker='o', color='purple', linestyle='-', linewidth=2)
    axes[0].set_title("Evolucion de la Complejidad Dimensional ($k$ de Horn)")
    axes[0].set_ylabel("Dimensiones Latentes ($k$)")
    axes[0].grid(True, alpha=0.3)

    axes[1].plot(x_vals, df[drift_col], marker='x', color='crimson', label='Drift (Inestabilidad)')
    axes[1].set_title("Inestabilidad Semantica ($1 - CosineSimilarity_{t, t-1}$)")
    axes[1].set_ylabel("Magnitud del Cambio")
    axes[1].fill_between(x_vals, df[drift_col], alpha=0.1, color='crimson')

    plt.tight_layout()
    if output_path: plt.savefig(output_path, bbox_inches='tight')
    plt.show()

def plot_projection_comparison(df, metric_prefix='score_centroid_', title_prefix='Proyeccion', output_path=None):
    """
    Plots comparison between Contextual (Usage) and Static (Dictionary) meanings.
    """
    setup_pub_style()
    dims = ['funcional', 'social', 'afectiva']
    colors = {'funcional': '#004e66', 'social': '#5d7667', 'afectiva': '#d14a2b'}

    fig, axes = plt.subplots(1, 3, figsize=(18, 5), sharey=True, sharex=True)

    df = df.sort_values(by='date')
    x_vals, labels = _handle_date_axis(axes[0], df, 'date', categorical=True)

    for ax in axes:
        ax.set_xticks(x_vals)
        ax.set_xticklabels(labels, rotation=45, ha='right')

    for i, dim in enumerate(dims):
        ax = axes[i]
        c = colors[dim]

        col_ctx = f'{metric_prefix}{dim}_contextual'
        if col_ctx in df.columns:
            ax.plot(x_vals, df[col_ctx], color=c, linestyle='-', marker='o', markersize=4, label='Contextual (Uso Real)')

        col_sta = f'{metric_prefix}{dim}_static'
        if col_sta in df.columns:
            ax.plot(x_vals, df[col_sta], color='gray', linestyle='--', marker='x', markersize=4, alpha=0.6, label='Estatico (Definicion)')

        ax.set_title(f"Dimension {dim.capitalize()}")
        ax.grid(True, alpha=0.3)

        if i == 0:
            ax.set_ylabel("Similitud Coseno")
            ax.legend()

    plt.suptitle(f"{title_prefix} - Comparativa: Uso vs Diccionario", y=1.05)
    plt.tight_layout()
    if output_path: plt.savefig(output_path, bbox_inches='tight')
    plt.show()

def plot_semantic_drift(df, date_col='date', drift_col='drift', events=None, output_path=None):
    """
    Plots drift with events.
    """
    setup_pub_style()
    fig, ax = plt.subplots(figsize=(12, 6))
    df = df.sort_values(by=date_col)

    x_vals, _ = _handle_date_axis(ax, df, date_col, categorical=True)

    ax.plot(x_vals, df[drift_col], color='#2c3e50', linewidth=1.5, marker='o', markersize=3,
            label=r'Deriva Semantica ($1 - \cos(S_t, S_{t-1})$)')
    ax.fill_between(x_vals, df[drift_col], alpha=0.1, color='#2c3e50')

    if events:
        y_max = df[drift_col].max()
        dates_series = pd.to_datetime(df[date_col]) if not isinstance(df[date_col].iloc[0], (pd.Timestamp, float)) else df[date_col]

        for date_str, label in events.items():
            date_obj = pd.to_datetime(date_str)
            if dates_series.min() <= date_obj <= dates_series.max():
                idx = (dates_series - date_obj).abs().idxmin()
                ax.axvline(idx, color='#e74c3c', linestyle='--', linewidth=1, alpha=0.7)
                ax.text(idx, y_max * 0.95, f' {label}', rotation=90, va='top', fontsize=8, color='#c0392b')

    ax.set_ylabel('Inestabilidad Semantica')
    ax.set_xlabel('Tiempo')
    ax.set_title('Evolucion Temporal de la Deriva Semantica')

    plt.tight_layout()
    plt.show()

def plot_scree_sequence(eigen_data, title="Evolucion de la Estructura Dimensional (Scree Plots)", output_path=None):
    """
    Plots the Scree Plot (Explained Variance) for selected time windows.
    """
    setup_pub_style()
    fig, ax = plt.subplots(figsize=(10, 6))

    if len(eigen_data) > 4:
        indices = np.linspace(0, len(eigen_data)-1, 4, dtype=int)
        selection = [eigen_data[i] for i in indices]
    else:
        selection = eigen_data

    for item in selection:
        date_label = str(item['date'])
        sv = np.array(item['eigenvalues'])
        variance = (sv ** 2) / np.sum(sv ** 2)
        cum_var = np.cumsum(variance)

        ax.plot(range(1, len(cum_var)+1), cum_var, marker='.', label=f'{date_label}')

    ax.set_xlabel('Numero de Componentes (Dimensiones)')
    ax.set_ylabel('Varianza Explicada Acumulada')
    ax.set_title(title)
    ax.set_ylim(0, 1.05)
    ax.axhline(0.9, color='gray', linestyle=':', label='90% Varianza')
    ax.legend()

    plt.tight_layout()
    if output_path: plt.savefig(output_path, bbox_inches='tight')
    plt.show()


def _resolve_anchors_dir(anchors_dir=None):
    """
    Resolves the directory containing .npz anchor files.

    Search order:
    1. Use anchors_dir if explicitly provided and it exists.
    2. Search upward from CWD for results/.../artifacts/anchors structure.
    3. Search upward from CWD for legacy data/phase3/artifacts/anchors structure.

    Returns the resolved path string, or None if not found.
    """
    # 1. Explicit path provided — use it directly
    if anchors_dir is not None:
        anchors_dir = str(anchors_dir)
        if os.path.exists(anchors_dir):
            return anchors_dir
        else:
            print(f"Warning: Provided anchors_dir does not exist: {anchors_dir}")
            return None

    # 2. Search upward from CWD — new results structure
    # Expected: results/phase3/<run_name>/artifacts/anchors/
    for root in ['.', '..', os.path.join('..', '..'), os.path.join('..', '..', '..')]:
        results_base = os.path.join(root, 'results')
        if not os.path.exists(results_base):
            continue
        # Walk results/phase3/*/artifacts/anchors
        phase3_base = os.path.join(results_base, 'phase3')
        if os.path.exists(phase3_base):
            for run_dir in os.listdir(phase3_base):
                candidate = os.path.join(phase3_base, run_dir, 'artifacts', 'anchors')
                if os.path.exists(candidate):
                    # Verify it actually contains .npz files
                    npz_files = [f for f in os.listdir(candidate) if f.endswith('.npz')]
                    if npz_files:
                        return candidate

    # 3. Legacy data/phase3/artifacts/anchors structure
    for root in ['.', '..', os.path.join('..', '..'), os.path.join('..', '..', '..')]:
        candidate = os.path.join(root, 'data', 'phase3', 'artifacts', 'anchors')
        if os.path.exists(candidate):
            npz_files = [f for f in os.listdir(candidate) if f.endswith('.npz')]
            if npz_files:
                return candidate

    return None


def _resolve_subspaces_dir(subspaces_dir=None):
    """
    Resolves the directory containing window .npz subspace files.

    Search order:
    1. Use subspaces_dir if explicitly provided and it exists.
    2. Search upward from CWD for results/.../artifacts/subspaces structure.
    3. Search upward from CWD for legacy data/phase3/artifacts/subspaces structure.

    Returns the resolved path string, or None if not found.
    """
    # 1. Explicit path provided
    if subspaces_dir is not None:
        subspaces_dir = str(subspaces_dir)
        if os.path.exists(subspaces_dir):
            return subspaces_dir
        else:
            print(f"Warning: Provided subspaces_dir does not exist: {subspaces_dir}")
            return None

    # 2. Search upward — new results structure
    for root in ['.', '..', os.path.join('..', '..'), os.path.join('..', '..', '..')]:
        results_base = os.path.join(root, 'results')
        if not os.path.exists(results_base):
            continue
        phase3_base = os.path.join(results_base, 'phase3')
        if os.path.exists(phase3_base):
            for run_dir in os.listdir(phase3_base):
                candidate = os.path.join(phase3_base, run_dir, 'artifacts', 'subspaces')
                if os.path.exists(candidate):
                    npz_files = [f for f in os.listdir(candidate) if f.endswith('.npz')]
                    if npz_files:
                        return candidate

    # 3. Legacy structure
    for root in ['.', '..', os.path.join('..', '..'), os.path.join('..', '..', '..')]:
        candidate = os.path.join(root, 'data', 'phase3', 'artifacts', 'subspaces')
        if os.path.exists(candidate):
            npz_files = [f for f in os.listdir(candidate) if f.endswith('.npz')]
            if npz_files:
                return candidate

    return None


def plot_flexible_projection(df, anchors_path=None, anchors_dir=None, subspaces_dir=None,
                             variant='baseline', strategy='penultimate',
                             condition='corrected', target_dimension='centroid',
                             title_prefix=None, output_path=None):
    """
    Plots projection of a specific subspace dimension onto anchor axes.

    Args:
        df:               DataFrame with 'date' column and window metadata.
        anchors_path:     Optional. Direct path to a specific .npz anchors file.
                          If provided, overrides anchors_dir + variant/strategy/condition.
        anchors_dir:      Optional. Directory containing .npz anchor files.
                          If None, the function searches automatically.
        subspaces_dir:    Optional. Directory containing window .npz subspace files.
                          If None, the function searches automatically.
        variant:          'baseline' or 'dapt'
        strategy:         'penultimate' or 'last4_concat'
        condition:        'corrected' or 'raw'
        target_dimension: 'centroid' (mean vector) or int (1-based eigenvector index)
        title_prefix:     Title for the plot.
        output_path:      Path to save image.
    """
    setup_pub_style()

    # ── 1. Resolve anchors .npz file ─────────────────────────────────────────
    if anchors_path is None:
        # Resolve the directory first
        resolved_anchors_dir = _resolve_anchors_dir(anchors_dir)

        if resolved_anchors_dir is None:
            print("Error: Could not find anchors directory.")
            print("Pass anchors_dir='/your/path/to/artifacts/anchors' explicitly.")
            return

        # Try with condition suffix first: anchors_{variant}_{strategy}_{condition}.npz
        candidate_with_condition = os.path.join(
            resolved_anchors_dir,
            f"anchors_{variant}_{strategy}_{condition}.npz"
        )
        # Fallback without condition: anchors_{variant}_{strategy}.npz
        candidate_without_condition = os.path.join(
            resolved_anchors_dir,
            f"anchors_{variant}_{strategy}.npz"
        )

        if os.path.exists(candidate_with_condition):
            anchors_path = candidate_with_condition
        elif os.path.exists(candidate_without_condition):
            anchors_path = candidate_without_condition
            print(f"Note: Using anchor file without condition suffix: {candidate_without_condition}")
        else:
            # Show what IS available to help debugging
            print(f"Error: Anchors file not found.")
            print(f"  Looked for: {candidate_with_condition}")
            print(f"  Looked for: {candidate_without_condition}")
            print(f"  Files available in {resolved_anchors_dir}:")
            try:
                for f in sorted(os.listdir(resolved_anchors_dir)):
                    print(f"    {f}")
            except Exception:
                pass
            return

    # Verify final path exists
    if not os.path.exists(anchors_path):
        print(f"Error: Anchors file not found at {anchors_path}")
        return

    # ── 2. Load anchors matrix ────────────────────────────────────────────────
    try:
        with np.load(anchors_path) as data:
            anchors_mat = data['A']  # Shape (D, 3)
            anchor_dims = ['funcional', 'social', 'afectiva']
    except Exception as e:
        print(f"Error loading anchors from {anchors_path}: {e}")
        return

    # ── 3. Resolve subspaces directory ───────────────────────────────────────
    resolved_subspaces_dir = _resolve_subspaces_dir(subspaces_dir)

    if resolved_subspaces_dir is None:
        print("Error: Could not find subspaces directory.")
        print("Pass subspaces_dir='/your/path/to/artifacts/subspaces' explicitly.")
        return

    # ── 4. Iterate windows and compute projections ───────────────────────────
    results = {d: [] for d in anchor_dims}
    dates = []

    if 'date' in df.columns:
        df = df.sort_values('date')

    for idx, row in df.iterrows():
        if hasattr(row['date'], 'strftime'):
            date_str = row['date'].strftime('%Y-%m')
        else:
            date_str = str(row['date'])[:7]

        fname = f"window_{date_str}_{variant}_{strategy}"

        # Subspace U is the same for raw/corrected (centering is applied before SVD)
        candidates = [
            f"{fname}_{condition}.npz",
            f"{fname}.npz"
        ]

        found = False
        U = None
        mu = None
        for cand in candidates:
            full_path = os.path.join(resolved_subspaces_dir, cand)
            if os.path.exists(full_path):
                try:
                    with np.load(full_path) as data:
                        U = data['U'] if 'U' in data else None
                        if 'mean_vector' in data:
                            mu = data['mean_vector']
                        elif 'mu' in data:
                            mu = data['mu']
                        found = True
                        break
                except Exception:
                    continue

        if not found:
            continue

        # Select vector to project
        vec_to_project = None
        if target_dimension == 'centroid':
            if mu is not None:
                vec_to_project = mu
        elif isinstance(target_dimension, int):
            dim_idx = target_dimension - 1
            if U is not None and U.shape[1] > dim_idx:
                vec_to_project = U[:, dim_idx]

        if vec_to_project is not None:
            norm_v = np.linalg.norm(vec_to_project)
            if norm_v > 1e-9:
                dates.append(date_str)
                for i, dim in enumerate(anchor_dims):
                    anchor_vec = anchors_mat[:, i]
                    norm_a = np.linalg.norm(anchor_vec)
                    sim = 0.0
                    if norm_a > 1e-9:
                        sim = np.dot(vec_to_project, anchor_vec) / (norm_v * norm_a)
                    if isinstance(target_dimension, int):
                        sim = abs(sim)  # Sign of eigenvectors is arbitrary
                    results[dim].append(sim)

    # ── 5. Plot ───────────────────────────────────────────────────────────────
    if not dates:
        print(f"No data found to plot.")
        print(f"  Anchors file : {anchors_path}")
        print(f"  Subspaces dir: {resolved_subspaces_dir}")
        print(f"  Variant/Strategy: {variant}/{strategy}")
        print(f"  Sample files in subspaces dir:")
        try:
            files = sorted(os.listdir(resolved_subspaces_dir))[:10]
            for f in files:
                print(f"    {f}")
        except Exception:
            pass
        return

    plot_df = pd.DataFrame(results)
    plot_df['date'] = dates

    fig, axes = plt.subplots(1, 3, figsize=(18, 5), sharey=True)
    colors = {'funcional': '#004e66', 'social': '#5d7667', 'afectiva': '#d14a2b'}

    x_vals, labels = _handle_date_axis(axes[0], plot_df, 'date', categorical=True)

    for i, dim in enumerate(anchor_dims):
        ax = axes[i]
        c = colors[dim]
        ax.plot(x_vals, plot_df[dim], color=c, marker='o', linestyle='-', linewidth=2)
        ax.set_xticks(x_vals)
        ax.set_xticklabels(labels, rotation=45, ha='right')
        ax.set_title(f"Dimension {dim.capitalize()}")
        ax.grid(True, alpha=0.3)
        if i == 0:
            ax.set_ylabel("Similitud Coseno")

    full_title = title_prefix if title_prefix else f"Proyeccion: {target_dimension} ({condition})"
    plt.suptitle(full_title, y=1.05)
    plt.tight_layout()

    if output_path:
        plt.savefig(output_path, bbox_inches='tight')
    plt.show()