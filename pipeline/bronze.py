from pathlib import Path
import polars as pl
import re

pattern = re.compile(
    r'(?P<country>.+)_(?P<quadkey>\d+)_(?P<upload_date>\d{4}-\d{2}-\d{2})\.csv\.gz$'
)

def bronze_convert(path: Path, bronze_dir: Path) -> Path:
    match = pattern.match(path.name)

    meta = match.groupdict()

    df = (
        pl.read_ndjson(path)
        .unnest('properties')
        .with_columns(
            pl.lit(meta['country']).alias('country'),
            pl.lit(meta['quadkey']).alias('quadkey'),
            pl.lit(meta['upload_date']).alias('upload_date'),
            pl.lit(path.name).alias('source_file'),
        )
    )

    out_path = bronze_dir / path.name.replace('.csv.gz', '.parquet')
    df.write_parquet(out_path)

    return out_path


def run_bronze(raw_dir: str = 'data/raw', bronze_dir: str = 'data/bronze/buildings') -> None:
    raw_path = Path(raw_dir)
    bronze_path = Path(bronze_dir)
    bronze_path.mkdir(parents=True, exist_ok=True)

    for path in raw_path.glob('*.csv.gz'):
        out = bronze_convert(path, bronze_path)
        print(f'Wrote: {out}')