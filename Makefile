include .env
export

export TF_VAR_project_id := $(GCP_PROJECT_ID)

# --- Terraform infra ---
infra-init:
	terraform -chdir=infra/terraform init

infra-plan:
	terraform -chdir=infra/terraform plan

infra-apply:
	terraform -chdir=infra/terraform apply

infra-destroy:
	terraform -chdir=infra/terraform destroy


# --- Docker stack ---
deploy:
	docker compose up -d --build

# destroy:
# 	docker compose down


# --- Main entry point ---
deploy-infra: infra-init infra-apply

deploy-cloud: deploy-infra deploy
