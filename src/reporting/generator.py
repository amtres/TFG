import nbformat
from nbconvert.preprocessors import ExecutePreprocessor
import os
import logging

class NotebookGenerator:
    def __init__(self, timeout=600, kernel_name='python3'):
        self.timeout = timeout
        self.kernel_name = kernel_name
        self.logger = logging.getLogger(__name__)

    def generate_and_execute(self, template_path, output_path, replacements):
        """
        Generates a new notebook from a template by replacing variable values and executes it.
        
        Args:
            template_path (str): Path to the source notebook template.
            output_path (str): Path where the generated notebook will be saved.
            replacements (dict): Dictionary of variable names (str) and their new values (str).
                                 Values should be Python code strings (e.g., '"path/to/file.csv"').
        """
        self.logger.info(f"Generating notebook from {template_path} to {output_path}")
        
        # 1. Read the notebook
        with open(template_path, 'r', encoding='utf-8') as f:
            nb = nbformat.read(f, as_version=4)

        # 2. Inject variables
        self._inject_variables(nb, replacements)

        # 3. Execute the notebook
        self.logger.info("Executing notebook...")
        ep = ExecutePreprocessor(timeout=self.timeout, kernel_name=self.kernel_name)
        
        try:
            ep.preprocess(nb, {'metadata': {'path': os.path.dirname(output_path)}})
        except Exception as e:
            self.logger.error(f"Error executing notebook {output_path}: {e}")
            raise

        # 4. Save the executed notebook
        self.logger.info(f"Saving executed notebook to {output_path}")
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f:
            nbformat.write(nb, f)

    def _normalize_path(self, value):
        """
        Converts Windows backslashes to forward slashes inside a path string.
        This prevents Python SyntaxError caused by unicode escape sequences
        like \\U, \\P, \\T when paths are injected as string literals in notebooks.

        Example:
            Input:  'C:\\Users\\alvar\\Documents\\TFG\\results\\file.csv'
            Output: 'C:/Users/alvar/Documents/TFG/results/file.csv'

        Python on Windows accepts forward slashes in all file operations
        (open, pandas, os.path, etc.) so this is completely safe.
        """
        # Strip any surrounding quotes that may have been included in the value
        stripped = value.strip()
        
        # Detect if the value is a quoted string (starts and ends with ' or ")
        if (stripped.startswith("'") and stripped.endswith("'")) or \
           (stripped.startswith('"') and stripped.endswith('"')):
            quote_char = stripped[0]
            inner = stripped[1:-1]
            # Replace backslashes with forward slashes inside the string
            inner_fixed = inner.replace('\\', '/')
            return f"{quote_char}{inner_fixed}{quote_char}"
        
        # If it's not a simple quoted string (e.g. it's a code expression),
        # return as-is — do not modify
        return value

    def _inject_variables(self, nb, replacements):
        for cell in nb.cells:
            if cell.cell_type == 'code':
                lines = cell.source.split('\n')
                new_lines = []
                for line in lines:
                    replaced = False
                    for var_name, var_value in replacements.items():
                        # Case 1: Variable assignment  (e.g.  PHASE3_CSV = "...")
                        is_assignment = (
                            line.strip().startswith(f"{var_name} =") or
                            line.strip().startswith(f"{var_name}=")
                        )
                        # Case 2: Exact line match (e.g. sys.path.append('..'))
                        is_match = line.strip() == var_name

                        if is_assignment or is_match:
                            # Preserve leading whitespace (indentation)
                            indent = line[:len(line) - len(line.lstrip())]

                            # Comment out the original line
                            new_lines.append(f"{indent}# {line.strip()} # Replaced by generator")

                            if is_assignment:
                                # FIX: normalize Windows backslashes before
                                # writing into the notebook source.
                                # Without this, paths like C:\Users\alvar
                                # cause SyntaxError because \U is interpreted
                                # as a unicode escape sequence.
                                safe_value = self._normalize_path(var_value)
                                new_line = f"{indent}{var_name} = {safe_value}"
                                new_lines.append(new_line)
                                self.logger.info(f"Replaced {var_name} with {safe_value}")
                            else:
                                # For exact-line replacements (sys.path.append etc.)
                                safe_value = self._normalize_path(var_value)
                                new_lines.append(f"{indent}{safe_value}")
                                self.logger.info(f"Replaced line matching {var_name}")

                            replaced = True
                            break

                    if not replaced:
                        new_lines.append(line)

                cell.source = '\n'.join(new_lines)