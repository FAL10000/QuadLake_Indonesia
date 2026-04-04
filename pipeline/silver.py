import json
from pathlib import Path
import polars as pl


def silver_convert(path: Path, silver_dir: Path) -> Path:
    out_path = silver_dir / path.name

    lf = (
        pl.scan_parquet(path)
        .with_row_index('source_row_index')
        .with_columns(
            pl.col('upload_date').str.to_date('%Y-%m-%d'),
            pl.concat_str(
                [pl.col('source_file'), pl.lit(':'), pl.col('source_row_index').cast(pl.Utf8)]
            ).alias('silver_record_id'),
            pl.col('geometry').struct.field('type').alias('geometry_type'),
            pl.col('geometry')
            .map_elements(json.dumps, return_dtype=pl.Utf8)
            .alias('geometry_geojson'),
        )
        .filter(
            pl.col('geometry').is_not_null()
            & pl.col('geometry_type').is_in(['Polygon', 'MultiPolygon'])
        )
        .select(
            'country',
            'quadkey',
            'upload_date',
            'source_file',
            'source_row_index',
            'silver_record_id',
            'geometry',
            'geometry_type',
            'geometry_geojson',
        )
    )

    lf.sink_parquet(out_path)

    return out_path


def run_silver(bronze_dir: str = 'data/bronze/buildings', silver_dir: str = 'data/silver/buildings') -> None:
    silver_path = Path(silver_dir)
    bronze_path = Path(bronze_dir)
    silver_path.mkdir(parents=True, exist_ok=True)

    for path in sorted(bronze_path.glob('*.parquet')):
        out = silver_convert(path, silver_path)
        print(f'Wrote: {out}')


if __name__ == '__main__':
    run_silver()
