.PHONY: help setup run stop logs clear status

help:
	@echo "Available commands:"
	@echo "  setup - Setup services, downloading images and building everything"
	@echo "  run - Start all services"
	@echo "  stop - Stop all services"
	@echo "  logs - Show services logs"
	@echo "  clear - Clear everything"
	@echo "  status - Check services statuses"

setup:
	@echo "Setting up Brewery Data Pipeline"
	docker compose up airflow-init
	@echo "Setup complete!"

run:
	@echo "Starting services"
	docker compose up -d

stop:
	@echo "Stopping services"
	docker compose down

logs:
	docker compose logs -f

clear:
	@echo "Removing all:"
	@echo "  - stopped containers"
	@echo "  - networks not used"
	@echo "  - dangling images"
	@echo "  - unused build cache"
	docker compose down -v
	docker system prune -f

status:
	docker compose ps

