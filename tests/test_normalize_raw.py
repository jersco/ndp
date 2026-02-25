import unittest
from pathlib import Path

from parsers import normalize_raw


class NormalizeRawTests(unittest.TestCase):
    def test_validate_raw_output_path_blocks_branded_filename(self):
        with self.assertRaisesRegex(RuntimeError, "Refusing to write raw rows"):
            normalize_raw.validate_raw_output_path(Path("data/outputs/branded-foods.jsonl"))

    def test_validate_raw_output_path_accepts_raw_filename(self):
        normalize_raw.validate_raw_output_path(Path("data/outputs/raw-foods.jsonl"))

    def test_enforce_unique_names_prefers_usda_source(self):
        rows = [
            {"name": "Apple, raw", "source": "cofid", "calories": 52.0, "protein": 1.0, "fiber": 2.4},
            {"name": " apple,   RAW ", "source": "usda_fooddata_central_survey", "calories": 52.0},
            {"name": "Banana, raw", "source": "cofid", "calories": 89.0},
        ]
        deduped = list(normalize_raw.enforce_unique_names(rows))
        self.assertEqual(len(deduped), 2)
        self.assertEqual(deduped[0]["source"], "usda_fooddata_central_survey")
        self.assertEqual(deduped[1]["name"], "Banana, raw")

    def test_enforce_unique_names_prefers_more_complete_non_usda(self):
        rows = [
            {"name": "Apple, raw", "source": "cofid", "calories": 52.0},
            {"name": " apple,   RAW ", "source": "nevo2025", "calories": 52.0, "protein": 0.3},
            {"name": "Banana, raw"},
            {"name": None},
            {"name": None},
        ]
        deduped = list(normalize_raw.enforce_unique_names(rows))
        self.assertEqual(len(deduped), 4)
        self.assertEqual(deduped[0]["source"], "nevo2025")
        self.assertEqual(deduped[1]["name"], "Banana, raw")
        self.assertIsNone(deduped[2]["name"])
        self.assertIsNone(deduped[3]["name"])


if __name__ == "__main__":
    unittest.main()
