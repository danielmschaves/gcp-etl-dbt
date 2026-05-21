include .env
export

## Dependencies
install:
	uv sync --group dev

## Ingestion
data-ingestion:
	uv run python -m ingestion.pipeline \
		--table_names $$TABLE_NAMES \
		--gcp_project $$GCP_PROJECT \
		--destination $$DESTINATION \
		--s3_path $$S3_PATH \
		--aws_profile $$AWS_PROFILE

pipeline-test:
	uv run pytest ingestion/tests -v

## dbt
dbt-transform:
	cd $$DBT_FOLDER && dbt run --target $$DBT_TARGET

data-transformation:
	cd $$DBT_FOLDER && dbt run --target $$DBT_TARGET

data-transformation-test:
	cd $$DBT_FOLDER && dbt test --target $$DBT_TARGET

dbt-test:
	cd $$DBT_FOLDER && dbt test

dbt-debug:
	cd $$DBT_FOLDER && dbt debug

dbt-compile:
	cd $$DBT_FOLDER && dbt compile

dbt-docs:
	cd $$DBT_FOLDER && dbt docs generate && dbt docs serve

## Dashboard
dashboard:
	cd dashboard && uv run streamlit run app.py

## Code quality
lint:
	uv run ruff check .

format:
	uv run ruff format .
