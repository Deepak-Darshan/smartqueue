.PHONY: run test lint docker-build docker-run clean

run:
	python scripts/run_all.py

test:
	pytest tests/ -v

lint:
	flake8 producer/ worker/ scaler/ shared/ --max-line-length=100

docker-build:
	docker build -t smartqueue .

docker-run:
	docker-compose up

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	rm -rf .pytest_cache
