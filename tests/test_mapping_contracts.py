import unittest

from common.contracts import FieldKind
from nutrient_mapping import (
    AUSTRALIA_FIELD_SPECS,
    CORE_FIELD_UNITS,
    CORE_FOOD_FIELDS,
    USDA_FIELD_SPECS,
)


class MappingContractTests(unittest.TestCase):
    def test_all_core_fields_present_for_usda(self):
        self.assertEqual(set(CORE_FOOD_FIELDS), set(USDA_FIELD_SPECS.keys()))

    def test_all_core_fields_present_for_australia(self):
        self.assertEqual(set(CORE_FOOD_FIELDS), set(AUSTRALIA_FIELD_SPECS.keys()))

    def test_all_core_fields_have_unit_contract(self):
        self.assertEqual(set(CORE_FOOD_FIELDS), set(CORE_FIELD_UNITS.keys()))

    def test_usda_has_nutrient_specs(self):
        self.assertEqual(USDA_FIELD_SPECS["calories"].kind, FieldKind.NUTRIENT_ID)
        self.assertEqual(USDA_FIELD_SPECS["source"].kind, FieldKind.LITERAL)


if __name__ == "__main__":
    unittest.main()
