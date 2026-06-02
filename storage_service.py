import os
import re


class StorageService:
    """Centralized project file layout for sources, outputs, and cache files."""

    def __init__(self, base_dir):
        self.base_dir = base_dir

    def project_root(self, project_id):
        return os.path.join(self.base_dir, "uploads", f"project_{int(project_id)}")

    def project_dirs(self, project_id):
        root = self.project_root(project_id)
        return {
            "root": root,
            "sources": os.path.join(root, "sources"),
            "outputs": os.path.join(root, "outputs"),
            "cache": os.path.join(root, "cache"),
        }

    def ensure_project_dirs(self, project_id):
        dirs = self.project_dirs(project_id)
        for path in dirs.values():
            os.makedirs(path, exist_ok=True)
        return dirs

    def source_path(self, project_id, filename):
        return os.path.join(self.ensure_project_dirs(project_id)["sources"], filename)

    def output_path(self, project_id, filename):
        return os.path.join(self.ensure_project_dirs(project_id)["outputs"], filename)

    def cache_dir(self, project_id, name):
        safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(name or "cache")).strip("_") or "cache"
        path = os.path.join(self.ensure_project_dirs(project_id)["cache"], safe)
        os.makedirs(path, exist_ok=True)
        return path
