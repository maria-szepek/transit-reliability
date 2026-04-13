GCP_CREDENTIALS=$(PWD)/infra/keys/credentials.json

# --- Terraform infra ---
deploy-infra:
	GOOGLE_APPLICATION_CREDENTIALS=$(GCP_CREDENTIALS) \
	TF_VAR_project_id=$(PROJECT_ID) \
	terraform -chdir=infra/terraform init

	GOOGLE_APPLICATION_CREDENTIALS=$(GCP_CREDENTIALS) \
	TF_VAR_project_id=$(PROJECT_ID) \
	terraform -chdir=infra/terraform apply -auto-approve

destroy-infra:
	GOOGLE_APPLICATION_CREDENTIALS=$(GCP_CREDENTIALS) \
	TF_VAR_project_id=$(PROJECT_ID) \
	terraform -chdir=infra/terraform destroy -auto-approve


# --- Docker stack ---
deploy:
	docker compose up -d --build

# destroy:
# 	docker compose down


# --- Main entry point ---
deploy-cloud: deploy-infra deploy  # how to run: make deploy-cloud PROJECT_ID=my-project