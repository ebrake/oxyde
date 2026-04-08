.PHONY: test test-unit test-integration test-smoke coverage lint format build-core mypy

PYTHON_DIR = python
TEST_DIR = $(PYTHON_DIR)/oxyde/tests
COV_PKG = $(PYTHON_DIR)/oxyde

test:
	pytest $(TEST_DIR)

test-unit:
	pytest $(TEST_DIR)/unit

test-integration:
	pytest $(TEST_DIR)/integration

test-smoke:
	pytest $(TEST_DIR)/smoke

coverage:
	pytest $(TEST_DIR) --cov=$(COV_PKG) --cov-report=term-missing

lint:
	cd $(PYTHON_DIR) && ruff check .

format:
	cd $(PYTHON_DIR) && ruff format .

build-core:
	cd crates/oxyde-core-py && maturin develop --release

mypy:
	python -m mypy $(PYTHON_DIR)/oxyde/ --exclude 'tests/' --config-file $(PYTHON_DIR)/pyproject.toml
