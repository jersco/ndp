import unittest
from pathlib import Path

from cli import build_parser
from common.cli import resolve_output_path
from parsers import merge_off_branded, normalize_raw


class CliCommandTests(unittest.TestCase):
    def test_normalize_raw_subcommand_registered(self):
        parser = build_parser()
        args = parser.parse_args(["normalize-raw"])

        self.assertEqual(args.source, "normalize-raw")
        self.assertTrue(callable(args.handler))

    def test_default_raw_output_path(self):
        output_path = resolve_output_path(None, normalize_raw.SOURCE_NAME)
        self.assertEqual(output_path, Path("data/outputs/raw-foods.jsonl"))

    def test_default_branded_output_path(self):
        output_path = resolve_output_path(None, merge_off_branded.SOURCE_NAME)
        self.assertEqual(output_path, Path("data/outputs/branded-foods.jsonl"))


if __name__ == "__main__":
    unittest.main()
