import unittest

from project_analysis_runner import ProgressFns, run_manual_import


class _FakeDM:
    def __init__(self):
        self.status = ""
        self.replaced = None

    def parse_links_from_output(self, project_id, output_file):
        self.parsed = (project_id, output_file)
        return [{"main_sku_id": "M1"}]

    def replace_project_links(self, project_id, links_df, categories=None):
        self.replaced = (project_id, links_df, categories)

    def update_project_status(self, project_id, status):
        self.status = (project_id, status)


class _Progress:
    def __init__(self):
        self.init_labels = []
        self.steps = []
        self.cleared = []

    def init_import_progress(self, pid, labels):
        self.init_labels.append((pid, list(labels)))

    def init_progress(self, pid, use_ai, main_name, comp_names):
        raise AssertionError("auto progress should not be used")

    def update_step(self, pid, idx, status, detail=""):
        self.steps.append((pid, idx, status, detail))

    def schedule_clear_progress(self, pid):
        self.cleared.append(pid)


class ProjectAnalysisRunnerTests(unittest.TestCase):
    def test_manual_import_runner_updates_links_status_and_progress(self):
        dm = _FakeDM()
        progress = _Progress()
        fns = ProgressFns(
            progress.init_progress,
            progress.init_import_progress,
            progress.update_step,
            progress.schedule_clear_progress,
        )

        run_manual_import(
            dm,
            {"project_id": 7, "output_file": "/tmp/output.xlsx", "partial_categories": ["可乐"]},
            fns,
        )

        self.assertEqual(progress.init_labels[0][0], 7)
        self.assertEqual(dm.parsed, (7, "/tmp/output.xlsx"))
        self.assertEqual(dm.replaced[0], 7)
        self.assertEqual(dm.replaced[2], ["可乐"])
        self.assertEqual(dm.status, (7, "ready"))
        self.assertEqual(progress.steps[-1], (7, 3, "done", ""))
        self.assertEqual(progress.cleared, [7])


if __name__ == "__main__":
    unittest.main()
