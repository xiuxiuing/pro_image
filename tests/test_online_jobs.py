import tempfile
import unittest

from data_mgr import DataManager
from online_jobs import JobStore


class OnlineJobStoreTests(unittest.TestCase):
    def test_job_progress_is_persisted_and_project_locked(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            dm = DataManager(tmpdir)
            store = JobStore(dm)

            job_id = store.create_job(1, "analysis", ["AI提取", "AI匹配"], {"project_id": 1})

            self.assertTrue(store.project_has_active_job(1, ["analysis"]))
            store.mark_running(job_id)
            store.update_step(job_id, 0, "running", "batch 1/2")

            progress = store.latest_project_progress(1)
            self.assertTrue(progress["available"])
            self.assertEqual(progress["job_id"], job_id)
            self.assertEqual(progress["job_status"], "running")
            self.assertEqual(progress["steps"][0]["status"], "running")
            self.assertEqual(progress["steps"][0]["detail"], "batch 1/2")

            store.update_step(job_id, 0, "done")
            store.update_step(job_id, 1, "done", "分析完成")
            store.finish(job_id, "succeeded")

            self.assertFalse(store.project_has_active_job(1, ["analysis"]))
            progress = store.latest_project_progress(1)
            self.assertEqual(progress["job_status"], "succeeded")
            self.assertEqual(progress["pct"], 100)

    def test_queued_job_can_be_marked_running_before_completion(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            dm = DataManager(tmpdir)
            store = JobStore(dm)

            job_id = store.create_job(1, "analysis", ["排队", "执行"], status="queued")

            self.assertTrue(store.project_has_active_job(1, ["analysis"]))
            self.assertEqual(store.latest_project_progress(1)["job_status"], "queued")

            store.mark_running(job_id)
            progress = store.latest_project_progress(1)

            self.assertEqual(progress["job_status"], "running")
            self.assertEqual(progress["steps"][0]["status"], "pending")


if __name__ == "__main__":
    unittest.main()
