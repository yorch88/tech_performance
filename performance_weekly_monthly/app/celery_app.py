import os
from celery import Celery
from celery.schedules import crontab

# Broker y backend (Redis)
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
BROKER_URL = os.getenv("CELERY_BROKER_URL", f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}")
RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND", f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}")

celery_app = Celery("perf_tasks", broker=BROKER_URL, backend=RESULT_BACKEND, include=["app.tasks"])

# Programación periódica: cada minuto
celery_app.conf.beat_schedule = {
    "run-performance-if-reports-every-minute": {
        "task": "app.tasks.run_performance_if_reports",
        "schedule": 60.0,
    },
    # si quieres mantener el check informativo:
    # "check-ftp-files-every-minute": {
    #     "task": "app.tasks.check_ftp_reports",
    #     "schedule": 60.0,
    # }
}

celery_app.conf.timezone = os.getenv("CELERY_TIMEZONE", "UTC")
