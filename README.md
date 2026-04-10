# QuadLake Indonesia

Indonesia-focused building-footprints pipeline built with Polars, GeoPandas, and notebooks.

## Overview

This repository explores how to take Microsoft's global building-footprints release and turn it into a smaller, Indonesia-focused workflow that can be inspected, rerun, and validated step by step.

The source data is not packaged as one clean Indonesia file. It comes from a global link table and many tile-level raw files, and those raw files use a `.csv.gz` extension even though they need to be parsed as NDJSON-style geospatial records rather than normal CSV. This repo works through that reality directly.

The current focus is building a simple Bronze/Silver/Gold pipeline on top of those raw files, validating each stage, and learning how Polars and geospatial tools fit together on a real dataset. I am using this project to learn by building, not by writing isolated toy examples, so the repo includes both exploratory notebooks and reusable pipeline scripts.

## Data sources

This project currently depends on two upstream sources:

- ADM boundaries source: [geoBoundaries admin boundaries for Indonesia](https://data.humdata.org/dataset/geoboundaries-admin-boundaries-for-indonesia)
- Building footprints source: [Microsoft Global ML Building Footprints](https://github.com/microsoft/globalmlbuildingfootprints)

## Current status

Implemented now:

- [x] Notebook-based Indonesia download flow from the global link table
- [x] Bronze conversion from raw `.csv.gz` files to parquet shards
- [x] Bronze validation for file mapping, metadata consistency, and geometry checks
- [x] Silver conversion with row-level identifiers and geometry normalization
- [x] Silver validation for file mapping and schema checks
- [x] Gold aggregation for country, quadkey, province, and district outputs
- [x] Gold handling for unmatched province and district assignments using nearest-boundary joins
- [x] Gold validation for file existence, total reconciliation, recovered row counts, and boundary-key integrity

In progress or not yet done:

- [ ] A clean command-line interface or packaged application entry point
- [ ] A polished reporting or presentation layer on top of Gold outputs
- [ ] More systematic benchmarking, scaling analysis, and pipeline ergonomics

## What this project currently does

Implemented:

- reads the global dataset link table from [source_csv/dataset-links.csv](/home/fal10/PycharmProjects/QuadLake%20Indonesia/source_csv/dataset-links.csv)
- filters that link table to Indonesia inside [notebooks/data_download.ipynb](/home/fal10/PycharmProjects/QuadLake%20Indonesia/notebooks/data_download.ipynb)
- downloads Indonesia raw files into `data/raw`
- converts raw files into Bronze parquet shards with `country`, `quadkey`, `upload_date`, and `source_file`
- validates Bronze outputs against raw filenames and geometry expectations
- builds Silver parquet shards with `source_row_index`, `silver_record_id`, `geometry`, and `geometry_type`
- filters Silver rows to valid `Polygon` and `MultiPolygon` geometries
- builds Gold outputs for:
  - country-level building counts
  - quadkey-level building counts
  - province-level building counts
  - district-level building counts
  - unmatched province and district rows
  - nearest-boundary recovery outputs for unmatched rows
- validates that Gold totals reconcile and that final province/district keys match the boundary reference data

Planned or still rough:

- cleaner orchestration across all stages
- better documentation around data assumptions and quality edge cases
- more analysis notebooks built directly from the Gold outputs

## Data pipeline and architecture

The current workflow is:

1. `source_csv/dataset-links.csv`
2. Notebook filter to Indonesia and download raw files into `data/raw`
3. Bronze conversion from raw NDJSON-style records into parquet shards in `data/bronze/buildings`
4. Bronze validation checks
5. Silver conversion into normalized geometry-focused parquet shards in `data/silver/buildings`
6. Silver validation checks
7. Gold aggregation into summary parquet outputs in `data/gold`
8. Gold validation checks against totals, recovered rows, and administrative boundary keys

More concretely:

- Bronze is file-level ingestion plus metadata extraction from filenames.
- Silver is row-level cleanup and schema shaping for downstream use.
- Gold is where the administrative summaries happen:
  - Polars handles tabular aggregation
  - GeoPandas handles province and district spatial joins
  - Shapely is used to derive representative points from building geometries
  - unmatched rows are written out and then assigned to the nearest boundary as a recovery step

Current Gold outputs include:

- `building_count_country.parquet`
- `building_count_quadkey.parquet`
- `building_counts_by_province.parquet`
- `building_counts_by_district.parquet`
- `building_counts_by_province_unmatched.parquet`
- `building_counts_by_district_unmatched.parquet`
- `building_counts_by_province_unmatched_nearest_adm1.parquet`
- `building_counts_by_district_unmatched_nearest_adm2.parquet`

## Repository structure

Important files and folders:

- [pipeline/bronze.py](/home/fal10/PycharmProjects/QuadLake%20Indonesia/pipeline/bronze.py)
  Bronze conversion and validation logic
- [pipeline/silver.py](/home/fal10/PycharmProjects/QuadLake%20Indonesia/pipeline/silver.py)
  Silver conversion and validation logic
- [pipeline/gold.py](/home/fal10/PycharmProjects/QuadLake%20Indonesia/pipeline/gold.py)
  Gold aggregation, nearest-boundary recovery, and Gold-stage validation logic
- [notebooks/data_download.ipynb](/home/fal10/PycharmProjects/QuadLake%20Indonesia/notebooks/data_download.ipynb)
  Filters the global link table to Indonesia and downloads raw files
- [notebooks/bronze_pipeline.ipynb](/home/fal10/PycharmProjects/QuadLake%20Indonesia/notebooks/bronze_pipeline.ipynb)
  Bronze-stage exploratory build notebook
- [notebooks/bronze_validation.ipynb](/home/fal10/PycharmProjects/QuadLake%20Indonesia/notebooks/bronze_validation.ipynb)
  Bronze-stage validation notebook
- [notebooks/silver_pipeline.ipynb](/home/fal10/PycharmProjects/QuadLake%20Indonesia/notebooks/silver_pipeline.ipynb)
  Silver-stage exploratory build notebook
- [notebooks/silver_validation.ipynb](/home/fal10/PycharmProjects/QuadLake%20Indonesia/notebooks/silver_validation.ipynb)
  Silver-stage validation notebook
- [notebooks/gold_pipeline.ipynb](/home/fal10/PycharmProjects/QuadLake%20Indonesia/notebooks/gold_pipeline.ipynb)
  Gold-stage exploratory build notebook
- [notebooks/gold_validation.ipynb](/home/fal10/PycharmProjects/QuadLake%20Indonesia/notebooks/gold_validation.ipynb)
  Gold-stage validation notebook
- `data/raw`
  downloaded Indonesia raw files
- `data/bronze/buildings`
  Bronze parquet shards
- `data/silver/buildings`
  Silver parquet shards
- `data/gold`
  Gold outputs
- `data/boundaries`
  province and district boundary GeoJSON files used in Gold spatial joins

Notes:

- [main.py](/home/fal10/PycharmProjects/QuadLake%20Indonesia/main.py) is still the default PyCharm stub and is not part of the actual workflow.
- The repo is currently driven by notebooks and stage scripts rather than a single orchestrated application entry point.

## How to run

### 1. Set up the environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Dependencies currently listed in [requirements.txt](/home/fal10/PycharmProjects/QuadLake%20Indonesia/requirements.txt):

- `polars`
- `pyarrow`
- `jupyterlab`
- `ipykernel`
- `geopandas`
- `shapely`

### 2. Prepare the data

Current assumption:

- the global link table exists at `source_csv/dataset-links.csv`
- raw Indonesia files are downloaded into `data/raw`
- administrative boundary files exist in `data/boundaries`

The current download flow is notebook-based:

- open [notebooks/data_download.ipynb](/home/fal10/PycharmProjects/QuadLake%20Indonesia/notebooks/data_download.ipynb)
- filter the source link table to Indonesia
- download the raw files into `data/raw`

### 3. Run the pipeline stages

Bronze:

```bash
python pipeline/bronze.py
```

Silver:

```bash
python pipeline/silver.py
```

Gold:

```bash
python pipeline/gold.py
```

Each script writes its outputs and runs validation checks before returning.

### 4. Inspect with notebooks

Use JupyterLab for the exploratory and validation notebooks:

```bash
jupyter lab
```

## What I’m learning through this project

This repo is a hands-on learning project, and the technical learning is the point.

Areas I am actively practicing here:

- Polars for file-oriented data processing and aggregation
- designing a simple medallion-style data flow with Bronze, Silver, and Gold stages
- working with messy real-world source formats instead of clean analytical tables
- using GeoPandas and Shapely for spatial joins and geometry-derived features
- building validation checks into each pipeline stage instead of treating QA as an afterthought
- reasoning about boundary mismatches and recovery logic in geospatial data
- keeping notebook exploration aligned with reusable pipeline code
- improving reproducibility by making stage outputs and checks explicit

## Known limitations and current gaps

- The repo does not yet have a real CLI, orchestration layer, or configuration system.
- The download step still lives in a notebook rather than a reusable script.
- `main.py` is not meaningful yet.
- The workflow depends on local data directories and boundary files already being present.
- The README and scripts describe the current pipeline, but the project is still evolving and not packaged as a finished tool.
- There is not yet a formal test suite around the pipeline modules.
- Some geospatial choices are practical rather than fully optimized, for example using a general projected CRS for nearest-boundary recovery.

## Next steps

Realistic next steps based on the current repo:

- move the Indonesia download flow out of the notebook and into a reusable script
- tighten Gold QA around recovery-distance distributions and boundary edge cases
- improve the ergonomics of running the full pipeline end to end
- document the data assumptions and stage outputs more explicitly
- build clearer analysis notebooks directly on top of the Gold outputs
- decide which exploratory notebook logic should be promoted into reusable modules
