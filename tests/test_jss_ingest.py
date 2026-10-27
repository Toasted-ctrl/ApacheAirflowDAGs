"""
Pytest tests for the JSS Ingest DAG.

Run with: source .venv/bin/activate && python -m pytest tests/test_jss_ingest.py -v
"""

import importlib.util
import sys
from datetime import datetime
from pathlib import Path

import pytest

# Path to the dags directory
DAGS_DIR = Path(__file__).parent.parent / "dags"
PROJECT_ROOT = Path(__file__).parent.parent

# Add project root and dags directory to Python path for imports
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(DAGS_DIR))


@pytest.fixture
def jss_ingest_dag():
    """Import and return the jss_ingest DAG module."""
    jss_ingest_file = DAGS_DIR / "jss_ingest.py"
    
    spec = importlib.util.spec_from_file_location("jss_ingest", jss_ingest_file)
    module = importlib.util.module_from_spec(spec)
    sys.modules["jss_ingest"] = module
    spec.loader.exec_module(module)
    
    # Find the DAG object in the module
    for name, obj in module.__dict__.items():
        if type(obj).__name__ == "DAG" and obj.dag_id == "jss.ingest":
            return obj
    
    raise ValueError("Could not find jss.ingest DAG in module")


class TestJSSIngestDAG:
    """Test suite for the JSS Ingest DAG."""
    
    def test_dag_module_imports(self, jss_ingest_dag):
        """Test that the DAG module can be imported without errors."""
        assert jss_ingest_dag is not None
        
    def test_dag_id(self, jss_ingest_dag):
        """Test that the DAG has the correct ID."""
        assert jss_ingest_dag.dag_id == "jss.ingest"
        
    def test_dag_display_name(self, jss_ingest_dag):
        """Test that the DAG has the correct display name."""
        assert jss_ingest_dag.dag_display_name == "JSS: Ingest"
        
    def test_schedule(self, jss_ingest_dag):
        """Test that the DAG has the correct schedule (Thursday at 15:00)."""
        expected_schedule = "0 15 * * 4"
        assert jss_ingest_dag.schedule == expected_schedule
        
    def test_start_date(self, jss_ingest_dag):
        """Test that the DAG has the correct start date."""
        # Normalize timezone for comparison
        start_date = jss_ingest_dag.start_date.replace(tzinfo=None)
        assert start_date == datetime(2026, 10, 25)
        
    def test_catchup_disabled(self, jss_ingest_dag):
        """Test that catchup is disabled."""
        assert jss_ingest_dag.catchup == False
        
    def test_has_ensure_schemas_task(self, jss_ingest_dag):
        """Test that the DAG has the ensure_schemas task."""
        task_ids = [task.task_id for task in jss_ingest_dag.tasks]
        assert "ensure_schemas" in task_ids
        
    def test_number_of_tasks(self, jss_ingest_dag):
        """Test that the DAG has exactly one task."""
        assert len(list(jss_ingest_dag.tasks)) == 1
