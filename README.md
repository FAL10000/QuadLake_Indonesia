# QuadLake Indonesia

Polars-based lakehouse project for turning Microsoft's global building-footprints dataset into an Indonesia-only Bronze, Silver, and Gold workflow.

## Current status

- [x] Indonesia manifest filtered from the global metadata file
- [x] 601 Indonesia raw source tiles downloaded into `data/raw`
- [x] NDJSON parsing confirmed for the source `*.csv.gz` files
- [x] Bronze conversion module implemented in `pipeline/bronze.py`
- [x] 601 Bronze Parquet shards written to `data/bronze/buildings`
- [x] Notebook-based Bronze validation completed for counts, metadata, and row parity
- [ ] Bronze execution hardened into a proper CLI or app entrypoint
- [ ] Silver layer implemented
- [ ] Gold layer implemented

## Project summary

This repository is a solo learning project for building a small geospatial lakehouse around Microsoft's global building-footprints data. The dataset is not delivered as a clean Indonesia export, so the work here starts from the global manifest, filters Indonesia-specific download links, preserves the original tile files, and builds query-friendly layers on top.

The project is no longer at the "can Bronze be built?" stage. Bronze exists today. The next work is to harden execution and validation outside notebooks, then design and build Silver and Gold.

## Source dataset

This project uses data published through Microsoft's official `GlobalMLBuildingFootprints` repository.

- Upstream repository: <https://github.com/microsoft/GlobalMLBuildingFootprints>
- Repository summary: worldwide building footprints derived from satellite imagery

In this repo, the upstream source is treated as the authoritative input for dataset discovery, metadata filtering, and raw download selection.

## Source format findings

- The global dataset is organized by tile or quadkey, not by clean country package.
- Indonesia is assembled from 601 tile-level download links in `source_csv/dataset-links.csv`.
- Raw files follow the pattern `Indonesia_<quadkey>_<upload_date>.csv.gz`.
- Despite the `.csv.gz` suffix, the files are not normal CSV.
- `pl.read_ndjson(...)` works for these files; `read_csv(...)` does not.
- Each record is a GeoJSON-like feature with `type`, `properties`, and `geometry`.

## Current data layers

### Raw

- Location: `data/raw`
- Status: complete for the current Indonesia subset
- Current count: 601 files
- Contract: keep raw files untouched for traceability and reprocessing

### Bronze

- Location: `data/bronze/buildings`
- Status: implemented and populated
- Current count: 601 Parquet shards
- Current total rows: 62,906,428
- Current schema:
  - `type`
  - `height`
  - `confidence`
  - `geometry`
  - `country`
  - `quadkey`
  - `upload_date`
  - `source_file`

The Bronze conversion currently:

- reads each raw file with `pl.read_ndjson(...)`
- unnests `properties`
- adds `country`, `quadkey`, `upload_date`, and `source_file`
- writes one Parquet file per raw tile

Bronze validation currently exists in `notebooks/bronze_validation.ipynb` and checks:

- raw file count versus Bronze file count
- one-to-one filename mapping
- presence of required metadata columns
- metadata values derived from filenames
- row-count parity between each raw file and its Bronze shard

### Silver

- Location: `data/silver`
- Status: folder exists, transformation layer not implemented

### Gold

- Location: `data/gold`
- Status: folder exists, output layer not implemented

## Repository layout

```text
.
├── README.md
├── main.py
├── requirements.txt
├── source_csv/
│   └── dataset-links.csv
├── pipeline/
│   └── bronze.py
├── notebooks/
│   ├── data_download.ipynb
│   ├── bronze_pipeline.ipynb
│   └── bronze_validation.ipynb
├── notes/
│   └── quadlake-indonesia-next-stages.md
└── data/
    ├── raw/
    ├── bronze/
    │   └── buildings/
    ├── silver/
    └── gold/
```

## Current execution surface

The current reusable pipeline entrypoint is the `run_bronze(...)` function in `pipeline/bronze.py`.

Example:

```python
from pipeline.bronze import run_bronze

run_bronze()
```

`main.py` is still the default PyCharm placeholder script and is not part of the project workflow yet.

## What is done

1. Filtered the global manifest down to the Indonesia subset.
2. Downloaded the 601 Indonesia source tiles into `data/raw`.
3. Confirmed the source files must be parsed as NDJSON rather than CSV.
4. Built notebook logic for Bronze conversion.
5. Moved Bronze conversion into `pipeline/bronze.py`.
6. Generated 601 Bronze Parquet shards in `data/bronze/buildings`.
7. Validated Bronze coverage and raw-to-Bronze row parity in the validation notebook.

## What is next

1. Turn Bronze from "implemented" into "operationally solid" by adding a proper command entrypoint, safer rerun behavior, and persistent validation outputs.
2. Define the Silver schema and geometry-handling rules explicitly.
3. Build Silver from Bronze Parquet instead of rereading raw files.
4. Create the first Gold outputs, starting with building counts and tile-level summaries.
5. Bring the docs and execution flow to a point where the project can be rebuilt without notebook-only steps.

## Minimal setup

1. Create and activate a Python environment.
2. Install dependencies from `requirements.txt`.
3. Keep `data/raw` unchanged after download.
4. Use Polars NDJSON readers for raw inspection and Parquet readers for Bronze inspection.

Example raw inspection:

```python
import polars as pl

df = pl.read_ndjson("data/raw/Indonesia_<quadkey>_<upload_date>.csv.gz")
print(df.columns)
print(df.head())
```

## Tech stack

- Python
- Polars for main data processing
- Parquet for Bronze storage
- Jupyter notebooks for exploration and validation
- GeoPandas and Shapely for occasional spatial sanity checks

## Notes

- This repo still mixes notebook exploration with production code, so it should be treated as an active build workspace rather than a finished package.
- The Bronze layer is real, but its execution and validation are not fully productionized yet.
- Silver and Gold are still design-and-build work, not hidden incomplete code.
