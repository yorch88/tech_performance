up:
	docker compose up -d --build

cellogs:
	docker compose logs -f celery-beat
	docker compose logs -f celery-worker

logs:
	 docker compose logs -f celery-worker

bash:
	docker compose exec api bash

rebuild:
	docker compose up -d --build

down:
	docker-compose down

run-test:
	docker compose exec api pytest -q
