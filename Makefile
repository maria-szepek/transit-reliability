GCP_CREDENTIALS=infra/credentials.json

infra:
	GOOGLE_APPLICATION_CREDENTIALS=$(GCP_CREDENTIALS) terraform -chdir=infra/terraform init
	GOOGLE_APPLICATION_CREDENTIALS=$(GCP_CREDENTIALS) terraform -chdir=infra/terraform apply -auto-approve

destroy:
	GOOGLE_APPLICATION_CREDENTIALS=$(GCP_CREDENTIALS) terraform -chdir=infra/terraform destroy -auto-approve