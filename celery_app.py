import os

from celery import Celery


def make_celery() -> Celery:
    broker_url = os.environ.get("CELERY_BROKER_URL", "redis://127.0.0.1:6379/0")
    result_backend = os.environ.get("CELERY_RESULT_BACKEND", broker_url)
    app = Celery("pro_image", broker=broker_url, backend=result_backend, include=["pro_image_tasks"])
    app.conf.update(
        task_track_started=True,
        worker_prefetch_multiplier=int(os.environ.get("CELERY_WORKER_PREFETCH_MULTIPLIER", "1") or "1"),
        task_acks_late=True,
        timezone=os.environ.get("TZ", "Asia/Shanghai"),
    )
    return app


celery = make_celery()
