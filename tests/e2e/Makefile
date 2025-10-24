# E2E Testing Makefile for SLURM Docker Cluster
.PHONY: help build test test-smoke clean logs shell collect-logs

# Default target
help:
	@echo "Available targets:"
	@echo "  build        - Build SLURM Docker images"
	@echo "  test         - Run all E2E tests" 
	@echo "  test-smoke   - Run smoke tests only"
	@echo "  clean        - Clean up containers and volumes"
	@echo "  logs         - Show container logs"
	@echo "  collect-logs - Collect container logs to files"
	@echo "  shell        - Open shell in slurmctld container"
	@echo "  help         - Show this help message"

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
	docker compose -f docker-compose.test.yml --env-file .env.test down -v --remove-orphans || true
	docker system prune -f || true

# Show container logs
logs:
	docker compose -f docker-compose.test.yml --env-file .env.test logs -f

# Collect container logs to files
collect-logs:
	@echo "📋 Collecting container logs..."
	@mkdir -p logs
	@for container in mysql-test slurmdbd-test slurmctld-test c1-test c2-test; do \
		if docker ps -a --format '{{.Names}}' | grep -q "^$$container$$"; then \
			echo "Collecting logs for $$container..."; \
			docker logs "$$container" > "logs/$$container.log" 2>&1 || echo "Failed to get logs for $$container"; \
		else \
			echo "Container $$container not found"; \
		fi; \
	done
	@ls -la logs/ || echo "No logs directory found"

# Open shell in slurmctld container
shell:
	docker exec -it slurmctld-test bash