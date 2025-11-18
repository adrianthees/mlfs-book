.DEFAULT_GOAL := help

include .env
export $(shell sed 's/=.*//' .env)

.PHONY: help clean features train inference all

##@ Help

help: ## Display this help message
	@echo "Air Quality ML Pipeline - Makefile Commands"
	@echo ""
	@awk 'BEGIN {FS = ":.*##"; printf "Usage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Pipeline Commands

clean: ## Clean Hopsworks resources
	uv run mlfs/clean_hopsworks_resources.py

features: ## Run feature backfill pipeline
	@echo "Running feature backfill pipeline..."
	uv run src/1_air_quality_feature_backfill.py

train: ## Run training pipeline
	@echo "Running training pipeline..."
	uv run src/3_air_quality_training_pipeline.py

inference: ## Run feature pipeline and batch inference
	@echo "Running feature pipeline..."
	uv run src/2_air_quality_feature_pipeline.py
	@echo "Running batch inference..."
	uv run src/4_air_quality_batch_inference.py

all: features train inference  ## Run complete pipeline (features -> train -> inference)
