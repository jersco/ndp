import unittest
from pathlib import Path

from nutrient_mapping import CORE_FOOD_FIELDS
from parsers import merge_off_branded


class MergeOffBrandedTests(unittest.TestCase):
    def test_validate_branded_output_path_blocks_raw_filename(self):
        with self.assertRaisesRegex(RuntimeError, "Refusing to write branded rows"):
            merge_off_branded.validate_branded_output_path(Path("data/outputs/raw-foods.jsonl"))

    def test_validate_branded_output_path_accepts_branded_filename(self):
        merge_off_branded.validate_branded_output_path(Path("data/outputs/branded-foods.jsonl"))

    def test_upc_match_keys_include_gtin_padding(self):
        keys = merge_off_branded.upc_match_keys("012345678905")
        self.assertIn("012345678905", keys)
        self.assertIn("0012345678905", keys)
        self.assertIn("00012345678905", keys)

    def test_merge_rows_prefers_usda_upc(self):
        usda_rows = [{"source": "usda", "upc": "012345678905", "name": "USDA"}]
        off_rows = [
            {"source": "off", "upc": "0012345678905", "name": "OFF duplicate"},
            {"source": "off", "upc": "9345678901234", "name": "OFF unique"},
        ]

        merged = list(merge_off_branded.merge_rows_with_usda_priority(usda_rows, off_rows))
        self.assertEqual(len(merged), 2)
        self.assertEqual(merged[0]["source"], "usda")
        self.assertEqual(merged[1]["source"], "off")
        self.assertEqual(merged[1]["name"], "OFF unique")

    def test_enforce_unique_upc(self):
        rows = [
            {"source": "a", "upc": "012345678905", "name": "name-a"},
            {"source": "b", "upc": "0012345678905", "name": "name-b"},
            {"source": "c", "upc": "9345678901234", "name": "name-c"},
        ]

        deduped = list(merge_off_branded.enforce_unique_keys(rows, unique_upc=True))
        self.assertEqual(len(deduped), 2)
        self.assertEqual(deduped[0]["name"], "name-a")
        self.assertEqual(deduped[1]["name"], "name-c")

    def test_name_is_not_used_for_dedup(self):
        rows = [
            {"source": "a", "upc": "012345678905", "name": "Same Name"},
            {"source": "b", "upc": "9345678901234", "name": " same   name "},
            {"source": "c", "upc": "123456789012", "name": "Other Name"},
        ]

        deduped = list(merge_off_branded.enforce_unique_keys(rows))
        self.assertEqual(len(deduped), 3)

    def test_convert_iu_for_vitamin_d(self):
        converted = merge_off_branded.convert_nutrient_value(
            100.0,
            "iu",
            "mcg",
            "vitamin_d_calciferol",
        )
        self.assertAlmostEqual(converted, 2.5, places=6)

    def test_map_off_row_converts_units_and_computes_net_carbs(self):
        off_row = {
            "code": "0012345678905",
            "lang": "en",
            "product_name": [{"lang": "en", "text": "Sample Product"}],
            "serving_size": "2 Tbsp (30 g)",
            "serving_quantity": "2",
            "nutriments": [
                {"name": "energy-kcal", "100g": 120.0, "unit": "kcal"},
                {"name": "proteins", "100g": 4.0, "unit": "g"},
                {"name": "carbohydrates", "100g": 20.0, "unit": "g"},
                {"name": "fiber", "100g": 5.0, "unit": "g"},
                {"name": "sodium", "100g": 0.4, "unit": "g"},
                {"name": "vitamin-c", "100g": 60.0, "unit": "mg"},
            ],
        }

        target_units = {field: None for field in CORE_FOOD_FIELDS}
        target_units.update(
            {
                "calories": "kcal",
                "protein": "g",
                "carbohydrates": "g",
                "fiber": "g",
                "sodium": "mg",
                "vitamin_c_ascorbic_acid": "mg",
            }
        )

        mapped = merge_off_branded.map_off_row(off_row, target_units, CORE_FOOD_FIELDS)
        self.assertEqual(set(mapped.keys()), set(CORE_FOOD_FIELDS + ["upc"]))
        self.assertEqual(mapped["source"], "open_food_facts")
        self.assertEqual(mapped["name"], "Sample Product")
        self.assertAlmostEqual(mapped["sodium"], 400.0, places=5)
        self.assertAlmostEqual(mapped["net_carbohydrates"], 15.0, places=5)
        self.assertIsInstance(mapped["portions"], list)
        self.assertEqual(mapped["portions"][0]["unit"], "g")
        self.assertAlmostEqual(mapped["portions"][0]["amount"], 30.0, places=5)


if __name__ == "__main__":
    unittest.main()
