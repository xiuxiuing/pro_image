import importlib.util
import io
import os
import re
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from PIL import Image
import requests


ROOT = Path(__file__).resolve().parents[1]
MAIN_PATH = ROOT / "main_030822.py"


def load_image_helpers():
    spec = importlib.util.spec_from_file_location("_main_image_helpers", MAIN_PATH)
    module = importlib.util.module_from_spec(spec)
    module.os = os
    module.re = re
    module.requests = requests
    module.Image = Image
    source = MAIN_PATH.read_text(encoding="utf-8")
    start = source.index("# --- Image Utilities ---")
    end = source.index("# --- Embedding & Index ---")
    exec(compile(source[start:end], str(MAIN_PATH), "exec"), module.__dict__)
    return module


def tiny_webp_bytes():
    buf = io.BytesIO()
    Image.new("RGB", (1, 1), (255, 255, 255)).save(buf, format="WEBP")
    return buf.getvalue()


class ImageUrlHandlingTests(unittest.TestCase):
    def test_split_image_urls_uses_configured_separators(self):
        helpers = load_image_helpers()
        self.assertEqual(
            ["https://a.test/1.webp", "https://b.test/2.webp", "https://c.test/3.webp", "https://d.test/4.webp"],
            helpers.split_image_urls(" https://a.test/1.webp； https://b.test/2.webp，https://c.test/3.webp, https://d.test/4.webp; "),
        )

    def test_download_img_uses_first_valid_url(self):
        helpers = load_image_helpers()

        class FakeResponse:
            def __init__(self, content, fail=False):
                self.content = content
                self.fail = fail

            def raise_for_status(self):
                if self.fail:
                    raise RuntimeError("bad url")

        calls = []

        def fake_get(url, timeout):
            calls.append((url, timeout))
            if "bad" in url:
                return FakeResponse(b"not an image", fail=True)
            return FakeResponse(tiny_webp_bytes())

        with tempfile.TemporaryDirectory() as tmpdir:
            with mock.patch.object(helpers.requests, "get", side_effect=fake_get):
                helpers.download_img("https://bad.test/1.webp；https://ok.test/2.webp", "1001", tmpdir)

            self.assertEqual(["https://bad.test/1.webp", "https://ok.test/2.webp"], [c[0] for c in calls])
            self.assertTrue((Path(tmpdir) / "1001.webp").exists())


if __name__ == "__main__":
    unittest.main()
