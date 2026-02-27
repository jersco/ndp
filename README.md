# NDP

Normalize food composition datasets into a common JSONL schema (`CORE_FOOD_FIELDS`).

## Install

```bash
uv sync
```

Or with pip:

```bash
pip install -e .
```

## CLI

All parsers write JSONL to `data/outputs/<source>.jsonl` by default. Use `--output -` for stdout.

```bash
ndp <source> [source options] [--output path-or--]
```

### Sources

1. Australia

```bash
ndp australia --workbook /path/to/australia.xlsx [--sheet "All solids & liquids per 100 g"]
```

2. Canada (CNF)

```bash
ndp canada --directory /path/to/cnf-fcen-csv
# or explicit files:
ndp canada --food-name-path /path/FOOD\ NAME.csv --nutrient-name-path /path/NUTRIENT\ NAME.csv --nutrient-amount-path /path/NUTRIENT\ AMOUNT.csv
```

3. UK CoFID

```bash
ndp cofid --workbook /path/to/cofid.xlsx
```

4. Netherlands NEVO

```bash
ndp nevo --workbook /path/to/nevo.xlsx [--sheet NEVO2025] [--nutrients-sheet NEVO2025_Nutrienten_Nutrients]
```

5. New Zealand

```bash
ndp new-zealand --workbook /path/to/new_zealand.xlsx [--sheet "Concisen Tables 14th Edition wi"]
```

6. USDA FoodData Central

```bash
ndp usda \
  --survey-json /path/FoodData_Central_survey_food_json.json \
  --foundation-json /path/FoodData_Central_foundation_food_json_2025-12-18.json \
  --sr-legacy-json /path/FoodData_Central_sr_legacy_food_json_2018-04.json \
  --nutrient-csv /path/nutrient.csv
```

7. Normalize Raw (Non-Branded Multi-Source)

```bash
ndp normalize-raw [--inputs-dir data/inputs] [--allow-duplicate-names]
```

Default output: `data/outputs/raw-foods.jsonl`
By default, duplicate normalized names are deduped with USDA rows preferred; otherwise the row with more populated nutrient fields is kept.

8. Merged Branded (USDA + Open Food Facts)

USDA UPCs are prioritized: if a UPC exists in both sources, only the USDA row is emitted.

```bash
ndp merge-off-branded \
  --usda-branded /path/FoodData_Central_branded_food_json_2025-12-18.zip \
  --off-parquet /path/food.parquet \
  --nutrient-csv /path/nutrient.csv
```

Default output: `data/outputs/branded-foods.jsonl`

## Outputs

Generated output is ignored by git:

- `outputs/`
- `*.jsonl`
- `data/outputs/`

Raw and branded outputs are intentionally separated:

- `normalize-raw` writes `data/outputs/raw-foods.jsonl`
- `merge-off-branded` writes `data/outputs/branded-foods.jsonl`

`merge-off-branded` output rows contain all `CORE_FOOD_FIELDS` plus two extra fields:

- `upc`
- `brand`

## Data Sources And Local Mapping

This project keeps original source files under `data/inputs/` and generated artifacts under `data/outputs/`.

### Open Food Facts (OFF)

- Source URL:
  - `https://huggingface.co/datasets/openfoodfacts/product-database/resolve/main/food.parquet?download=true`
- Local input:
  - `data/inputs/food.parquet`
- Used by:
  - `merge-off-branded` stage

### Australian Food Composition Database (AFCD)

- Source URL:
  - `https://www.foodstandards.gov.au/science-data/food-nutrient-databases/afcd/data-files`
- Local inputs:
  - `data/inputs/australian-food-composition-database.xlsx`
  - `data/inputs/AFCD Release 3 - Nutrient details.xlsx`
  - `data/inputs/AFCD Release 3 - Recipes.xlsx`
- Used by:
  - `normalize-raw` stage

### USDA FoodData Central

- Source URL:
  - `https://fdc.nal.usda.gov/`
- Local inputs:
  - `data/inputs/FoodData_Central_csv_2025-04-24/`
  - `data/inputs/FoodData_Central_csv_2025-04-24.zip`
  - `data/inputs/FoodData_Central_foundation_food_json_2025-04-24.zip`
  - `data/inputs/FoodData_Central_sr_legacy_food_json_2018-04.zip`
  - `data/inputs/FoodData_Central_survey_food_json_2024-10-31.zip`
  - `data/inputs/FoodData_Central_branded_food_json_2025-12-18.zip`
  - `data/inputs/FoodData_Central_branded_food_json_2025-12-18.json` (optional extracted file)
- Used by:
  - `normalize-raw`, `make-lean-raw`, `make-usda-branded`, `merge-off-branded`

### Canadian Nutrient File (CNF)

- Source URL:
  - `https://www.canada.ca/en/health-canada/services/food-nutrition/healthy-eating/nutrient-data/canadian-nutrient-file-2015-download-files.html`
- Local input:
  - `data/inputs/canadian-nutrient-files/`
- Used by:
  - `normalize-raw` stage

### UK CoFID

- Source URL:
  - `https://www.gov.uk/government/publications/composition-of-foods-integrated-dataset-cofid`
- Local input:
  - `data/inputs/CoFID.xlsx`
- Used by:
  - `normalize-raw` stage

### Dutch Food Composition Database (NEVO)

- Source URL:
  - `https://www.rivm.nl/en/dutch-food-composition-database`
- Local input:
  - `data/inputs/dutch-nutrient-database/`
- Used by:
  - `normalize-raw` stage

### New Zealand FOODfiles

- Source URL:
  - `https://www.foodcomposition.co.nz/foodfiles/concise-tables/`
- Local input:
  - `data/inputs/new-zealand-food-concise.xlsx`
- Used by:
  - `normalize-raw` stage

### Notes

- `data/inputs/` contains raw source material used by the pipeline.
- `data/outputs/` contains generated JSONL + SQLite outputs.
- Keep license/terms from each source in mind when redistributing derived data.

## Mapping Contract

Mappings live in `nutrient_mapping.py` and are standardized to `FieldSpec` objects with:

- `kind` (`literal`, `source`, `nutrient_id`, `computed`, `missing`)
- `source`
- optional fallbacks (`fallback_sources`, `fallback_mode`)
- optional metadata (for reference fields like nutrient number and source name)

Unit expectations for normalized outputs are declared in `CORE_FIELD_UNITS` in `nutrient_mapping.py`.
All parsers are expected to convert source values to those units before writing JSONL.

## Testing

Run all tests:

```bash
uv run python -m unittest discover -s tests -v
```

Notes:

- Unit tests run without local source data.
- Integration tests in `tests/test_integration_sources.py` automatically run when `data/inputs/` exists and skip otherwise.
