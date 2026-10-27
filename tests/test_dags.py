"""
Pytest tests for all DAGs in the project.

Run with: source .venv/bin/activate && python -m pytest tests/test_dags.py -v
"""

import importlib.util
import sys
from pathlib import Path

import pytest

# Path to the dags directory
DAGS_DIR = Path(__file__).parent.parent / "dags"
PROJECT_ROOT = Path(__file__).parent.parent

# Add project root and dags directory to Python path for imports
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(DAGS_DIR))


@pytest.fixture
def dag_files():
    """Return list of Python files in dags directory (excluding test files)."""
    return [
        f for f in DAGS_DIR.glob("*.py")
        if not f.name.startswith("test_") and not f.name.endswith("_test.py")
    ]


class TestDAGLoading:
    """Test suite for DAG loading."""
    
    def test_dags_directory_exists(self):
        """Test that the dags directory exists."""
        assert DAGS_DIR.exists()
        assert DAGS_DIR.is_dir()
        
    def test_at_least_one_dag_file(self, dag_files):
        """Test that there is at least one DAG file."""
        assert len(dag_files) > 0, "No DAG files found in dags directory"
        
    @pytest.mark.parametrize("py_file", [
        "jss_ingest.py",
        "runescape_ingest.py", 
        "runescape_staging.py"
    ])
    def test_dag_file_imports(self, py_file):
        """Test that each DAG file can be imported without errors."""
        dag_file = DAGS_DIR / py_file
        
        if not dag_file.exists():
            pytest.skip(f"{py_file} does not exist")
        
        spec = importlib.util.spec_from_file_location(
            py_file.replace(".py", ""), 
            dag_file
        )
        module = importlib.util.module_from_spec(spec)
        sys.modules[py_file.replace(".py", "")] = module
        
        # This should not raise any exceptions
        spec.loader.exec_module(module)
        
        # Verify at least one DAG object exists in the module
        dags = [
            obj for name, obj in module.__dict__.items()
            if type(obj).__name__ == "DAG"
        ]
        assert len(dags) > 0, f"No DAG objects found in {py_file}"
        
    def test_all_dags_have_required_attributes(self, dag_files):
        """Test that all DAGs have required attributes."""
        for py_file in dag_files:
            spec = importlib.util.spec_from_file_location(
                py_file.stem, 
                py_file
            )
            module = importlib.util.module_from_spec(spec)
            sys.modules[py_file.stem] = module
            spec.loader.exec_module(module)
            
            # Find all DAG objects
            dags = [
                obj for name, obj in module.__dict__.items()
                if type(obj).__name__ == "DAG"
            ]
            
            for dag in dags:
                assert hasattr(dag, "dag_id"), f"DAG missing dag_id attribute"
                assert hasattr(dag, "schedule"), f"DAG {dag.dag_id} missing schedule attribute"
                assert hasattr(dag, "start_date"), f"DAG {dag.dag_id} missing start_date attribute"
                assert hasattr(dag, "catchup"), f"DAG {dag.dag_id} missing catchup attribute"
