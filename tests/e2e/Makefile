# E2E Testing Makefile for SLURM Docker Cluster
.PHONY: help build test test-smoke clean logs shell

# Default target
help:
	@echo "Available targets:"
	@echo "  build      - Build SLURM Docker images"
	@echo "  test       - Run all E2E tests" 
	@echo "  test-smoke - Run smoke tests only"
	@echo "  clean      - Clean up containers and volumes"
	@echo "  logs       - Show container logs"
	@echo "  shell      - Open shell in slurmctld container"
	@echo "  help       - Show this help message"

# Build SLURM Docker images
build:
	@echo "🏗️  Building SLURM Docker images..."
	docker compose -f docker-compose.test.yml --env-file .env.test build

# Run all E2E tests
test: build
	@echo "🧪 Running E2E tests..."
	uv run python -m pytest -v

# Run smoke tests only
test-smoke: build
	@echo "💨 Running smoke tests..."
	uv run python -m pytest -v -m smoke

# Clean up containers and volumes
clean:
	@echo "🧹 Cleaning up containers and volumes..."
	docker compose -f docker-compose.test.yml --env-file .env.test down --rmi local --volumes

# Show container logs
logs:
	docker compose -f docker-compose.test.yml logs -f

# Open shell in slurmctld container
shell:
	docker exec -it slurmctld-test bash