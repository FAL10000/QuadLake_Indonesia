# QuadLake Indonesia

A solo Polars and geospatial engineering project focused on turning Microsoft's global building-footprints dataset into a reproducible, Indonesia-only lakehouse workflow.

This repository is where I am learning by building. The project starts from a global, tile-based building-footprint dataset and works toward a practical Bronze/Silver/Gold pipeline that can be rerun, validated, and extended into GIS-style analysis.

## Why this project exists

I wanted a project that is bigger and messier than a normal dataframe exercise.

This repo is my way to practice:

- working with large real-world data instead of classroom-sized CSVs
- using Polars for actual pipeline work, not just small notebook demos
- handling geospatial-like source data that does not arrive in a clean analyst-ready format
- designing a lakehouse-style workflow from raw files to reusable analytical outputs
- building something that can grow into a more serious GIS project later

In short, this project is for learning **Polars**, **data pipeline design**, and **geospatial data handling** through a real Indonesia-focused use case.

## What this project is about

Microsoft's Global Building Footprints dataset is not packaged as one neat Indonesia file in the current global release. It is distributed as many tile or quadkey-based files discovered through a metadata link table. The raw source files also use a misleading `.csv.gz` extension even though they must be parsed as NDJSON/line-delimited GeoJSON-style records rather than normal CSV. :contentReference[oaicite:1]{index=1}

This project takes that source and builds an Indonesia-only processing flow:

1. filter the global metadata down to Indonesia tiles
2. download the Indonesia raw files
3. preserve raw files untouched
4. convert raw source into Bronze Parquet shards
5. later build cleaner Silver transformations
6. later build Gold summaries and GIS-ready outputs

## What I am doing here

This repository documents my work as I go, including the mistakes, format discoveries, and pipeline decisions.

What I am actively doing in this repo:

- filtering the upstream global dataset down to Indonesia
- validating how the raw files are actually structured
- converting raw tile files into Bronze Parquet with source metadata
- checking row parity and file parity between raw and Bronze
- using notebooks first for exploration, then moving logic into reusable pipeline code
- keeping the project reproducible enough to rerun from source

This is intentionally a **manual solo project**. The point is to learn the workflow, understand the data, and make the architecture decisions myself.

## Current status

- [x] Indonesia manifest filtered from the upstream metadata
- [x] Indonesia raw tile downloads completed
- [x] Confirmed that raw `*.csv.gz` files are not normal CSV
- [x] Confirmed that Polars NDJSON parsing works on the raw files
- [x] Built Bronze conversion into Parquet shards
- [x] Added Bronze validation checks for file mapping and row parity
- [ ] Silver layer implemented
- [ ] Gold layer implemented
- [ ] GIS-style enrichment and analysis layer implemented

## Source dataset

This project uses Microsoft's global building-footprints release as the upstream source. The global dataset is tile-based, updated through a metadata link table, and includes building-footprint records that can be filtered down to Indonesia-specific tiles. The upstream README describes the global release and its update history. :contentReference[oaicite:2]{index=2}

For this repo, the upstream data is treated as the authoritative source for:

- dataset discovery
- Indonesia tile selection
- raw download links
- source lineage

## Key source-format findings

A big part of the learning value in this project is understanding the source format correctly.

Confirmed so far:

- the current global dataset is organized by tile/quadkey, not by neat country export files
- Indonesia is assembled from many tile-level source files
- raw filenames follow an Indonesia + quadkey + upload-date pattern in this repo
- the raw files use a `.csv.gz` suffix but are not true CSV files
- `read_csv(...)` is the wrong parser for these files
- Polars NDJSON reading works for the raw source
- each parsed row behaves like one building-footprint feature with fields such as `type`, `properties`, and `geometry`
- a quick GeoPandas sanity check on one sample tile plotted an Aceh-area footprint cluster, confirming that the tile-based structure behaves as expected

## Repository structure

```text
.
├── README.md
├── requirements.txt
├── main.py
├── source_csv/
│   └── dataset-links.csv
├── pipeline/
│   └── bronze.py
├── notebooks/
│   ├── data_download.ipynb
│   ├── bronze_pipeline.ipynb
│   └── bronze_validation.ipynb
└── data/
    ├── raw/
    ├── bronze/
    ├── silver/
    └── gold/