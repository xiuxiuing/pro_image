import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
INDEX_HTML = ROOT / "templates" / "index.html"


def css_block(selector: str) -> str:
    text = INDEX_HTML.read_text(encoding="utf-8")
    match = re.search(rf"{re.escape(selector)}\s*\{{(?P<body>.*?)\}}", text, re.S)
    if not match:
        raise AssertionError(f"{selector} CSS block not found")
    return match.group("body")


class ModalThumbZoomCssTests(unittest.TestCase):
    def test_modal_thumb_zoom_is_75_square(self):
        block = css_block(".modal-thumb-zoom")

        self.assertRegex(block, r"width:\s*75px\s*;")
        self.assertRegex(block, r"height:\s*75px\s*;")

    def test_card_top_image_is_128_square(self):
        block = css_block(".card-top-image")

        self.assertRegex(block, r"width:\s*128px\s*;")
        self.assertRegex(block, r"height:\s*128px\s*;")


if __name__ == "__main__":
    unittest.main()
