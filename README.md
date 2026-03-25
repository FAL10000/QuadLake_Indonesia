# QuadLake Indonesia

Polars-based lakehouse project for extracting Indonesia from Microsoft's global building-footprints dataset and turning the raw tile downloads into Bronze, Silver, and Gold data layers.

## Current status

- [x] Indonesia manifest filtered
- [x] raw downloads completed
- [x] NDJSON parsing confirmed
- [x] sample map visualization confirmed
- [ ] Bronze pipeline in progress
- [ ] Silver not started
- [ ] Gold not started

## Short project summary

This repository is a solo learning project being built manually to understand how to structure a small lakehouse pipeline around large geospatial source data using Polars.

The current focus is not analytics yet. The project is still at the raw-ingestion / Bronze-building stage: identifying the correct Indonesia tiles from the global Microsoft Global Building Footprints dataset, preserving the raw downloads untouched, and preparing a reliable Bronze Parquet layer as the first real pipeline output.

## Why this project exists

Microsoft's building-footprints data is globally distributed and operationally closer to tiled geospatial source data than to a clean country-level export. This project exists to turn that reality into a practical, reproducible local pipeline:

- start from the global dataset rather than a simplified legacy country package
- isolate Indonesia using the dataset-links metadata
- keep raw files in their original downloaded form
- build a layered data model that is easier to validate, query, and extend
- learn Polars through a real dataset with awkward but useful source characteristics

## Dataset and source format

Source dataset: Microsoft Global Building Footprints, global version, with the Indonesia subset selected from the dataset-links metadata.

## Source credit

This project uses data published through Microsoft's official `GlobalMLBuildingFootprints` repository:

- Upstream repository: <https://github.com/microsoft/GlobalMLBuildingFootprints>
- Repository summary: worldwide building footprints derived from satellite imagery

In this repo, that upstream source is treated as the authoritative starting point for dataset discovery, metadata filtering, and raw download selection. `QuadLake Indonesia` is a separate, manual learning project focused on building a Polars-based Indonesia pipeline on top of that source data.

Important format details:

- The raw files are tile/quadkey-based, not neat administrative country files.
- Each raw file corresponds to a quadkey tile.
- Current raw filename pattern is `Indonesia_<quadkey>_<upload_date>.csv.gz`.
- Despite the `.csv.gz` extension, these files are not normal CSV files.
- In practice, the files behave like line-delimited GeoJSON / NDJSON-like GeoJSON Feature records.
- `pl.read_ndjson(...)` works for the sampled files; `read_csv(...)` does not.

Observed structure so far:

- `type`
- `properties`
- `geometry`

At the moment:

- `type` appears constant as `Feature`
- `geometry` is the important GeoJSON polygon field
- `properties` looks sparse or placeholder-like in sampled files, but is being retained for now

## Current progress

Completed so far:

1. Chose the global Microsoft dataset instead of the older single-country zip approach.
2. Filtered the dataset-links metadata down to Indonesia.
3. Identified about 601 Indonesia download links.
4. Downloaded raw source files into `data/raw/`.
5. Confirmed that CSV parsing fails because the files are not true CSV.
6. Confirmed that `pl.read_ndjson(...)` reads the raw files successfully.
7. Visualized one sample file with GeoPandas as a quick sanity check and confirmed it mapped to what appears to be an Aceh-area tile.

What is not done yet:

- Bronze Parquet generation is not finished yet.
- Silver transformations have not started.
- Gold outputs have not started.

## Confirmed findings so far

- The global source is organized by quadkey/tile, so Indonesia has to be assembled from many tile-level files rather than consumed as a single country file.
- Each sampled row appears to represent one building footprint feature.
- The `.csv.gz` extension is misleading and should not drive the parser choice.
- For this dataset, Polars NDJSON ingestion is the correct starting point.
- Raw files should remain untouched as original downloads for traceability and reprocessing.
- GeoPandas is useful here as a quick visualization and sanity-check tool, but not the main processing layer.
- The main processing library for this project is Polars.

## Current folder layout

Current repository layout:

```text
.
├── README.md
├── main.py
├── data_download.ipynb
├── bronze_convert.ipynb
├── source_csv/
│   └── dataset-links.csv
└── data/
    └── raw/
        └── Indonesia_<quadkey>_<upload_date>.csv.gz
```

Planned next-layer folders:

```text
data/
├── raw/      # original downloaded source files, kept untouched
├── bronze/   # planned: one parquet shard per raw tile file
├── silver/   # planned
└── gold/     # planned
```

## Bronze / Silver / Gold plan

### Bronze

Bronze is the next active build step.

Planned Bronze approach:

- read each raw file with `pl.read_ndjson(...)`
- apply only minimal transformation
- preserve the original feature payload as faithfully as possible
- add source metadata columns such as `country`, `quadkey`, `upload_date`, and `source_file`
- optionally unnest `properties` if that proves useful and stable
- write one Bronze Parquet file per raw file into a Bronze folder

Bronze intent:

- raw remains the untouched source-of-truth download
- Bronze becomes the first normalized, query-friendly layer in Parquet

### Silver

Silver has not started.

Likely Silver responsibilities later:

- schema cleanup
- column normalization
- geometry handling decisions
- stronger validation and consistency checks across tiles
- deduplication or quality checks if needed

### Gold

Gold has not started.

Likely Gold responsibilities later:

- analysis-ready aggregated outputs
- Indonesia-focused derived datasets
- reporting or summary tables built from Silver

## Immediate next steps

1. Build the first Bronze conversion pass from raw tile files to Parquet shards.
2. Add source metadata columns during Bronze write-out.
3. Decide whether `properties` should remain nested or be partially unnested in Bronze.
4. Validate that one raw file maps cleanly to one Bronze shard.
5. Check that the Bronze outputs remain easy to trace back to the original raw filenames.

## Longer-term roadmap

- formalize the Bronze conversion code outside exploratory notebooks
- define a stable Bronze schema
- design the first Silver transformation layer
- determine how geometry should be represented through later layers
- add repeatable validation checks for tile coverage and schema consistency
- build first Gold outputs once Silver exists
- document lineage and data-layer decisions as the pipeline becomes real

## Notes / caveats

- This repository is being built manually as a learning project, so the README reflects current understanding rather than a finished architecture.
- The source files should not be treated as normal CSV just because they end in `.csv.gz`.
- Raw files are intentionally kept untouched in `data/raw/`.
- Bronze is planned as Parquet; raw remains the original download format.
- The current understanding is based on sampled files and early inspection, so some schema details may evolve once all Bronze shards are generated.
- GeoPandas was used only for a quick sanity-check visualization, not as the main data-processing engine.

## Minimal getting started

The repository is currently best treated as an exploration and pipeline-building workspace rather than a packaged application.

1. Create and activate a local Python environment.
2. Install the libraries you need for the current stage.
   - Required for core work: `polars`
   - Useful for notebooks: `jupyter`
   - Optional for quick visual sanity checks only: `geopandas`
3. Keep raw files in `data/raw/` exactly as downloaded.
4. When inspecting a raw file, use Polars NDJSON reading, not CSV reading.

Example:

```python
import polars as pl

df = pl.read_ndjson("data/raw/Indonesia_<quadkey>_<upload_date>.csv.gz")
print(df.columns)
print(df.head())
```

## Tech stack

- Python
- Polars for main data processing
- Parquet as the planned Bronze storage format
- Jupyter notebooks for current exploration
- GeoPandas for quick map-based sanity checks only
