import importlib
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


class _FakeModel:
    def to(self, *_args, **_kwargs):
        return self

    def eval(self):
        return self


def _load_main_module():
    sys.modules.pop("main_030822", None)
    with mock.patch("transformers.AutoImageProcessor.from_pretrained", return_value=object()), \
         mock.patch("transformers.AutoModel.from_pretrained", return_value=_FakeModel()), \
         mock.patch("transformers.AutoTokenizer.from_pretrained", return_value=object()):
        return importlib.import_module("main_030822")


class AstarTextEmbeddingConfigTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.main = _load_main_module()

    def test_segmented_text_includes_default_astar_fields(self):
        text = self.main._build_segmented_text({
            "美团类目一级": "饮品",
            "规格名称": "500ml*1瓶",
            "商品名称": "无糖可乐",
            "A核心品类": "可乐",
            "A商品形态": "瓶装",
            "A关键属性词": "无糖",
            "A颜色": "黑色",
            "A单件净含量": "500ml",
            "A售卖数量": "1",
        }, None)

        self.assertIn("[CAT1]=饮品", text)
        self.assertIn("[CORE]=可乐", text)
        self.assertIn("[FORM]=瓶装", text)
        self.assertIn("[ATTR]=无糖", text)
        self.assertIn("[COLOR]=黑色", text)
        self.assertNotIn("500ml\n", text)
        self.assertNotIn("[A单件净含量]", text)
        self.assertTrue(text.endswith("500ml*1瓶, 无糖可乐"))

    def test_segmented_text_can_disable_astar_fields(self):
        text = self.main._build_segmented_text({
            "美团类目一级": "饮品",
            "规格名称": "500ml*1瓶",
            "商品名称": "无糖可乐",
            "A核心品类": "可乐",
        }, {"category_level": 1, "text_astar_enabled": False})

        self.assertEqual(text, "[CAT1]=饮品\n500ml*1瓶, 无糖可乐")

    def test_segmented_text_skips_empty_astar_fields(self):
        text = self.main._build_segmented_text({
            "美团类目一级": "饮品",
            "规格名称": "500ml*1瓶",
            "商品名称": "无糖可乐",
            "A核心品类": "",
            "A商品形态": None,
            "A关键属性词": "无糖",
        }, None)

        self.assertNotIn("[CORE]=", text)
        self.assertNotIn("[FORM]=", text)
        self.assertIn("[ATTR]=无糖", text)

    def test_text_index_meta_invalidates_when_config_changes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            index_path = str(Path(tmpdir) / "txt.index")
            Path(index_path).write_bytes(b"fake-index")
            self.main._write_text_index_meta(index_path, self.main.default_match_config(), {"vectors": 1})

            self.assertTrue(self.main._text_index_cache_valid(index_path, self.main.default_match_config()))
            self.assertFalse(self.main._text_index_cache_valid(index_path, {"category_level": 1, "text_astar_enabled": False}))

    def test_ops_tools_passes_default_match_config(self):
        source = Path(__file__).resolve().parents[1] / "app_ops_tasks.py"
        text = source.read_text(encoding="utf-8")
        self.assertIn("match_config=main_030822.default_match_config()", text)


if __name__ == "__main__":
    unittest.main()
