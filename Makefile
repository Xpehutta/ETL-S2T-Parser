# Makefile for ETL S2T Parser

.PHONY: help install install-dev test test-cov lint format clean run

help:
	@echo "Available targets:"
	@echo "  install      Install production dependencies"
	@echo "  install-dev  Install development dependencies (including testing)"
	@echo "  test         Run tests without coverage"
	@echo "  test-cov     Run tests with coverage report"
	@echo "  lint         Run linter (ruff)"
	@echo "  format       Format code (black)"
	@echo "  clean        Remove cache and temporary files"
	@echo "  run          Run the Flask application"

install:
	uv sync --no-dev

install-dev:
	uv sync

test:
	pytest tests/ -v --no-cov

test-cov:
	pytest tests/ --cov=. --cov-config=.coveragerc --cov-report=term --cov-report=html

lint:
	ruff check .

format:
	black .

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".coverage" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "htmlcov" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	rm -f excel_data.db  # optional – remove local database

run:
	uv run python app.py