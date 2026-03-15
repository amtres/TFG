import os
import logging
from .generator import NotebookGenerator

class Phase4Orchestrator:
    def __init__(self, project_root):
        """
        Initialize the Phase 4 Orchestrator.
        
        Args:
            project_root (str): Absolute path to the project root directory.
        """
        self.project_root = project_root
        self.logger = logging.getLogger(__name__)
        self.generator = NotebookGenerator()
        
        # Define the templates relative to project root
        self.templates = {
            "4_1_methodology": "academic/methodological_report/phase4_4_1_metodologia_eda.ipynb",
            "4_2_math": "academic/methodological_report/phase4_4_2_matematicas_eda.ipynb",
            "4_3_results": "academic/methodological_report/phase4_4_3_resultados_eda.ipynb",
            "4_4_interpretation": "academic/methodological_report/phase4_4_4_interpretacion_eda.ipynb",
            "general_report": "academic/Reporte_Integral_TFM (Actualizado).ipynb"
        }

    def _to_forward_slash(self, path):
        """
        Converts all backslashes in a path to forward slashes.
        This prevents Python SyntaxError when paths are injected as string
        literals into notebook cells on Windows, where backslashes like
        \\U, \\P, \\T are misinterpreted as unicode escape sequences.
        
        Python on Windows accepts forward slashes in all file operations
        (open, pandas, os.path, etc.) so this conversion is completely safe.
        """
        return path.replace('\\', '/')

    def generate_reports(self, phase3_csv_path, output_dir_base, anchors_json_path=None):
        """
        Generates all Phase 4 notebooks using the provided Phase 3 data.
        
        Args:
            phase3_csv_path (str): Absolute path to phase3_results.csv
            output_dir_base (str): Absolute path to the directory where reports should be saved.
            anchors_json_path (str, optional): Absolute path to the dimensions/anchors JSON file.
                                               If None, the orchestrator will search for it automatically.
        """
        # Convert to absolute paths
        phase3_csv_path = os.path.abspath(phase3_csv_path)
        output_dir_base = os.path.abspath(output_dir_base)
        
        self.logger.info(f"Starting Phase 4 report generation. Output: {output_dir_base}")
        
        if not os.path.exists(phase3_csv_path):
            self.logger.error(f"Phase 3 CSV not found at {phase3_csv_path}")
            raise FileNotFoundError(f"Phase 3 CSV not found at {phase3_csv_path}")

        os.makedirs(output_dir_base, exist_ok=True)
        
        # Deduce companion directories from phase3 CSV location
        phase3_dir = os.path.dirname(phase3_csv_path)
        artifacts_dir = os.path.join(phase3_dir, "artifacts")
        subspaces_dir = os.path.join(phase3_dir, "artifacts", "subspaces")
        if not os.path.exists(subspaces_dir):
            subspaces_dir = os.path.join(self.project_root, "data", "phase3", "artifacts", "subspaces")

        # ── Build replacements dict ──────────────────────────────────────────
        # All paths are converted to forward slashes before wrapping in quotes
        # so they are valid Python string literals when injected into notebooks.
        replacements = {
            "PHASE3_CSV":   f"'{self._to_forward_slash(phase3_csv_path)}'",
            "CSV_PATH":     f"'{self._to_forward_slash(phase3_csv_path)}'",
            "base_dir":     f"'{self._to_forward_slash(subspaces_dir)}'",
            "ARTIFACTS_DIR":f"'{self._to_forward_slash(artifacts_dir)}'",
        }

        # ── Find ANCHORS_CSV ─────────────────────────────────────────────────
        possible_anchors_names = ["anchors_matrix.csv", "anchors.csv", "embeddings_anchors.csv"]
        possible_anchors_dirs = [
            phase3_dir,
            os.path.join(phase3_dir, "artifacts"),
            os.path.join(phase3_dir, "artifacts", "anchors"),
            os.path.join(phase3_dir, "anchors"),
            os.path.join(self.project_root, "data"),
        ]
        
        found_anchor = False
        for d in possible_anchors_dirs:
            if found_anchor:
                break
            if not os.path.exists(d):
                continue
            for name in possible_anchors_names:
                p = os.path.join(d, name)
                if os.path.exists(p):
                    replacements["ANCHORS_CSV"] = f"'{self._to_forward_slash(p)}'"
                    self.logger.info(f"Found ANCHORS_CSV: {p}")
                    found_anchor = True
                    break

        if not found_anchor:
            self.logger.warning("ANCHORS_CSV not found in standard locations. Notebooks that require it may fail.")

        # ── Find DIMENSIONS_JSON ─────────────────────────────────────────────
        # If caller provided a path explicitly, use it directly.
        # Otherwise search standard locations including data/metadata/anchors/
        if anchors_json_path and os.path.exists(anchors_json_path):
            replacements["DIMENSIONS_JSON"] = f"'{self._to_forward_slash(os.path.abspath(anchors_json_path))}'"
            self.logger.info(f"Using provided DIMENSIONS_JSON: {anchors_json_path}")
        else:
            # Search for the JSON in standard locations
            possible_dims_names = [
                "dimensiones_ancla_mh_es_covid_FSA_ascii.json",  # your actual filename
                "dimensiones_ancla_mh_es_covid.json",
                "dimensiones_ancla.json",
                "dimensions.json",
                "anchors.json",
            ]
            possible_dims_dirs = [
                phase3_dir,
                os.path.join(phase3_dir, "artifacts"),
                os.path.join(self.project_root, "data", "metadata", "anchors"),  # your actual location
                os.path.join(self.project_root, "data", "metadata"),
                os.path.join(self.project_root, "data"),
                os.path.join(self.project_root, "configs"),
            ]

            found_dim = False
            for d in possible_dims_dirs:
                if found_dim:
                    break
                if not os.path.exists(d):
                    continue
                for name in possible_dims_names:
                    p = os.path.join(d, name)
                    if os.path.exists(p):
                        replacements["DIMENSIONS_JSON"] = f"'{self._to_forward_slash(p)}'"
                        self.logger.info(f"Found DIMENSIONS_JSON: {p}")
                        found_dim = True
                        break

            if not found_dim:
                self.logger.warning(
                    "DIMENSIONS_JSON not found in standard locations. "
                    "Pass --anchors to pipeline_manager.py to specify the path explicitly."
                )

        # ── Find MANIFEST_JSON ───────────────────────────────────────────────
        manifest_path = os.path.join(phase3_dir, "manifest.json")
        if os.path.exists(manifest_path):
            replacements["MANIFEST_JSON"] = f"'{self._to_forward_slash(manifest_path)}'"
            self.logger.info(f"Found MANIFEST_JSON: {manifest_path}")

        # ── FIX: sys.path.append replacement ────────────────────────────────
        # The original code injected a raw Windows path string like:
        #   sys.path.append('C:\Users\alvar\...')
        # which causes SyntaxError because \U, \P etc. are unicode escape sequences.
        # We use forward slashes to avoid this.
        safe_project_root = self._to_forward_slash(self.project_root)
        replacements["sys.path.append('..')"] = f"sys.path.append('{safe_project_root}')"

        # ── Create output artifacts folder ───────────────────────────────────
        artifacts_path_output = os.path.join(output_dir_base, "artifacts")
        os.makedirs(artifacts_path_output, exist_ok=True)

        # ── Generate each notebook ───────────────────────────────────────────
        for key, relative_template_path in self.templates.items():
            template_full_path = os.path.join(self.project_root, relative_template_path)
            
            if not os.path.exists(template_full_path):
                self.logger.warning(f"Template not found: {template_full_path}. Skipping.")
                continue

            output_filename = "General_Report.ipynb" if key == "general_report" \
                              else os.path.basename(relative_template_path)
                
            output_full_path = os.path.join(output_dir_base, output_filename)
            
            self.logger.info(f"Generating {key} -> {output_full_path}")
            
            try:
                self.generator.generate_and_execute(
                    template_path=template_full_path,
                    output_path=output_full_path,
                    replacements=replacements
                )
                self.logger.info(f"Successfully generated {output_filename}")
            except Exception as e:
                self.logger.error(f"Failed to generate {output_filename}: {e}")