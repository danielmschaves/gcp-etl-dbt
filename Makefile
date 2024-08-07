include .env
export 

data-ingestion: 
	python ingestion/pipeline.py \
		--table_names $$TABLE_NAMES \
		--gcp_project $$GCP_PROJECT \
		--destination $$DESTINATION	\
		--s3_path $$S3_PATH \
		--aws_profile $$AWS_PROFILE

run-etl:
	python ingestion/pipeline.py \
		--table_names "$$TABLE_NAMES" \
		--gcp_project "$$GCP_PROJECT" \
		--destination "$$DESTINATION" \
		--s3_path "$$S3_PATH" \
		--aws_profile "$$AWS_PROFILE"

pipeline-test:
	pytest ingestion/tests

dbt-transform:
	cd $$DBT_FOLDER && \
	dbt run

dbt-debug:
	cd $$DBT_FOLDER && \
	dbt debug 

dbt-compile:
	cd $$DBT_FOLDER && \
	dbt compile 

dashboard:
	cd dashboard && \
	streamlit run app.py
	

## Development
install: 
	poetry install

format:
	ruff format . 


# aws-sso-creds:
# # DuckDB aws creds doesn't support loading from sso, so this create temporary creds file
# 	aws configure export-credentials --profile $$AWS_PROFILE --format env-no-export | \
# 	grep -E 'AWS_ACCESS_KEY_ID|AWS_SECRET_ACCESS_KEY|AWS_SESSION_TOKEN' | \
# 	sed -e 's/AWS_ACCESS_KEY_ID/aws_access_key_id/' \
# 		-e 's/AWS_SECRET_ACCESS_KEY/aws_secret_access_key/' \
# 		-e 's/AWS_SESSION_TOKEN/aws_session_token/' \
# 		-e 's/^/ /' -e 's/=/ =/' | \
# 	awk -v profile="$$AWS_PROFILE" 'BEGIN {print "["profile"]"} {print}' > ~/.aws/credentials