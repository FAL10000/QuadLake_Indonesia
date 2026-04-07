from __future__ import annotations

import json
from pathlib import Path

import polars as pl


def list_files(path: Path, pattern: str) -> list[Path]:
    return sorted(path.glob(pattern))


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


def validate_file_mapping(bronze_files: list[Path], silver_files: list[Path]) -> list[str]:
    errors: list[str] = []
    bronze_names = {file.name for file in bronze_files}
    silver_names = {file.name for file in silver_files}

    if len(silver_names) != len(bronze_names):
        errors.append(f'File count mismatch: bronze={len(bronze_names)}, silver={len(silver_names)}')

    missing_in_silver = sorted(bronze_names - silver_names)
    unexpected_in_silver = sorted(silver_names - bronze_names)

    for name in missing_in_silver:
        errors.append(f'Missing in silver: {name}')

    for name in unexpected_in_silver:
        errors.append(f'Unexpected in silver: {name}')

    return errors


def validate_duckdb_ready_columns(silver_files: list[Path]) -> list[str]:
    errors: list[str] = []

    for file in silver_files:
        schema = pl.scan_parquet(file).collect_schema()

        if 'geometry_geojson' not in schema.names():
            errors.append(f'Missing geometry_geojson in {file.name}')
            continue

        result = (
            pl.scan_parquet(file)
            .select(
                pl.len().alias('rows'),
                pl.col('geometry_geojson').null_count().alias('geometry_geojson_nulls'),
            )
            .collect()
            .row(0, named=True)
        )

        if result['rows'] == 0:
            errors.append(f'Empty silver file: {file.name}')
            continue

        if result['geometry_geojson_nulls'] > 0:
            errors.append(
                f"{file.name}: geometry_geojson has {result['geometry_geojson_nulls']} null rows"
            )

    return errors


def run_silver_validation(
    bronze_dir: Path = Path('data/bronze/buildings'),
    silver_dir: Path = Path('data/silver/buildings'),
) -> dict[str, object]:
    bronze_files = list_files(bronze_dir, '*.parquet')
    silver_files = list_files(silver_dir, '*.parquet')

    if not bronze_files:
        errors = [f'No bronze files found in {bronze_dir}']
        return {'checks': [('input files', errors)], 'errors': errors}

    if not silver_files:
        errors = [f'No silver files found in {silver_dir}']
        return {'checks': [('output files', errors)], 'errors': errors}

    checks = [
        ('file mapping', validate_file_mapping(bronze_files, silver_files)),
        ('duckdb-ready geometry_geojson', validate_duckdb_ready_columns(silver_files)),
    ]
    errors = [error for _, check_errors in checks for error in check_errors]
    return {'checks': checks, 'errors': errors}


def print_validation_report(report: dict[str, object]) -> None:
    for label, errors in report['checks']:
        if errors:
            print(f'[FAIL] {label}')
            for error in errors:
                print(f'  - {error}')
        else:
            print(f'[OK] {label}')


def run_silver(
    bronze_dir: str = 'data/bronze/buildings',
    silver_dir: str = 'data/silver/buildings',
) -> dict[str, object]:
    silver_path = Path(silver_dir)
    bronze_path = Path(bronze_dir)
    silver_path.mkdir(parents=True, exist_ok=True)

    for path in sorted(bronze_path.glob('*.parquet')):
        out = silver_convert(path, silver_path)
        print(f'Wrote: {out}')

    report = run_silver_validation(bronze_path, silver_path)
    print_validation_report(report)
    if report['errors']:
        raise AssertionError('\n'.join(report['errors']))
    return report


if __name__ == '__main__':
    run_silver()
