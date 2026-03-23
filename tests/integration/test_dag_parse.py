import importlib

import pytest


def test_dag_module_imports() -> None:
    pytest.importorskip("airflow")
    module = importlib.import_module("potatocore.orchestrator.dags.etl_lotofacil")
    assert hasattr(module, "dag")
