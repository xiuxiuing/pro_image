import traceback

from celery_app import celery
from online_jobs import JobStore
from project_analysis_runner import ProgressFns, run_auto_analysis, run_manual_import


@celery.task(name="pro_image.import_project_sources")
def import_project_sources_task(project_id: int, job_id: str = ""):
    """Import uploaded source spreadsheets in a worker process."""
    import app as app_module

    dm = app_module.dm
    store = JobStore(dm)
    try:
        if job_id:
            store.mark_running(job_id)
            store.update_step(job_id, 0, "running", "准备导入")
        dm.import_project_sources(int(project_id))
        if job_id:
            store.update_step(job_id, 0, "done")
            store.update_step(job_id, 1, "done", "数据已写入")
            store.update_step(job_id, 2, "done", "快照已刷新")
            store.update_step(job_id, 3, "done")
            store.finish(job_id, "done")
        dm.update_project_status(int(project_id), "ready")
        return {"status": "ok", "project_id": int(project_id)}
    except BaseException as exc:
        traceback.print_exc()
        try:
            dm.update_project_status(int(project_id), "failed")
        except Exception:
            pass
        if job_id:
            try:
                store.finish(job_id, "failed", str(exc))
            except Exception:
                pass
        raise exc


def _app_progress(app_module):
    return ProgressFns(
        app_module._init_progress,
        app_module._init_import_progress,
        app_module._update_step,
        app_module._schedule_clear_progress,
    )


@celery.task(name="pro_image.run_manual_import")
def run_manual_import_task(payload: dict):
    import app as app_module

    run_manual_import(app_module.dm, payload, _app_progress(app_module))
    return {"status": "ok", "project_id": int(payload["project_id"])}


@celery.task(name="pro_image.run_auto_analysis")
def run_auto_analysis_task(payload: dict):
    import app as app_module

    run_auto_analysis(app_module.dm, payload, _app_progress(app_module))
    return {"status": "ok", "project_id": int(payload["project_id"])}
