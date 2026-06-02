# ProImage Phase 1 Online Runbook

## Scope

Phase 1 moves heavy work out of the Flask request path and makes project data, job
progress, and output files project-scoped.

This runbook assumes a fresh online deployment. No legacy local data migration is
required.

## Services

- `pro-image.service`: Flask/Gunicorn web service.
- `pro-image-worker.service`: Celery worker for source import, manual result import,
  and AI/matching analysis jobs.
- `redis-server`: Celery broker/result backend.
- `nginx`: reverse proxy.

## Runtime Defaults

- Web concurrency is intentionally conservative: `WEB_CONCURRENCY=1`,
  `WEB_THREADS=1`.
- AI/task concurrency is intentionally conservative: `CELERY_CONCURRENCY=1`.
- Raise these only after the remaining template/UI paths are fully project-id
  explicit and the production AI quota is confirmed.

## Project Isolation

- Uploaded files live under `uploads/project_{project_id}/sources`.
- Generated files live under `uploads/project_{project_id}/outputs`.
- Temporary normalized analysis inputs live under `uploads/project_{project_id}/cache`.
- Long-running jobs are persisted in `jobs` and `job_steps`.
- One project cannot run `source_import`, `manual_import`, or `analysis` at the
  same time.

## Smoke Test

1. Start Redis, Web, and Worker.
2. Create two projects with different source files.
3. Wait for source import jobs to complete.
4. Open both projects in separate browser sessions.
5. Run AI/matching analysis for one project.
6. Confirm the other project can still browse data and does not show the first
   project's progress or output.
7. Export comparison, new items, corrections, and statistics for both projects.

## Database

PostgreSQL is required. The app refuses to start without `DATABASE_URL`.

Example:

```text
DATABASE_URL=postgresql+psycopg://pro_image:change-me@127.0.0.1:5432/pro_image
```

Fresh deployments initialize schema and built-in seed data automatically. Legacy
SQLite business data is not migrated.
