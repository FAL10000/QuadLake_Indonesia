from __future__ import annotations

import re
from pathlib import Path

import polars as pl


RAW_FILENAME_PATTERN = re.compile(
    r'(?P<country>.+)_(?P<quadkey>\d+)_(?P<upload_date>\d{4}-\d{2}-\d{2})\.csv\.gz$'
)
BRONZE_FILENAME_PATTERN = re.compile(
    r'(?P<country>.+)_(?P<quadkey>\d+)_(?P<upload_date>\d{4}-\d{2}-\d{2})\.parquet$'
)
VALID_GEOMETRY_TYPES = {'Polygon', 'MultiPolygon'}


def list_files(path: Path, pattern: str) -> list[Path]:
    return sorted(path.glob(pattern))


def bronze_convert(path: Path, bronze_dir: Path) -> Path:
    match = RAW_FILENAME_PATTERN.fullmatch(path.name)
    if match is None:
        raise ValueError(f'Unexpected raw filename: {path.name}')

    meta = match.groupdict()
    out_path = bronze_dir / path.name.replace('.csv.gz', '.parquet')

    (
        pl.scan_ndjson(path)
        .unnest('properties')
        .with_columns(
            pl.lit(meta['country']).alias('country'),
            pl.lit(meta['quadkey']).alias('quadkey'),
            pl.lit(meta['upload_date']).alias('upload_date'),
            pl.lit(path.name).alias('source_file'),
        )
        .sink_parquet(out_path)
    )

    return out_path


def validate_file_count_and_mapping(raw_files: list[Path], bronze_files: list[Path]) -> list[str]:
    errors: list[str] = []
    expected_bronze_names = {raw_file.name.replace('.csv.gz', '.parquet') for raw_file in raw_files}
    actual_bronze_names = {bronze_file.name for bronze_file in bronze_files}

    missing_bronze = sorted(expected_bronze_names - actual_bronze_names)
    unexpected_bronze = sorted(actual_bronze_names - expected_bronze_names)

    if len(raw_files) != len(bronze_files):
        errors.append(
            f'raw/bronze file count mismatch: raw={len(raw_files)} bronze={len(bronze_files)}'
        )

    if missing_bronze:
        errors.append(f'missing bronze files: {missing_bronze[:5]}')

    if unexpected_bronze:
        errors.append(f'unexpected bronze files: {unexpected_bronze[:5]}')

    return errors


def validate_metadata_values(bronze_files: list[Path]) -> list[str]:
    errors: list[str] = []

    for file in bronze_files:
        match = BRONZE_FILENAME_PATTERN.fullmatch(file.name)
        if match is None:
            errors.append(f'{file.name}: invalid bronze filename')
            continue

        expected = match.groupdict()
        expected['source_file'] = file.name.replace('.parquet', '.csv.gz')

        result = (
            pl.scan_parquet(file)
            .select(
                pl.len().alias('rows'),
                pl.col('country').n_unique().alias('country_n_unique'),
                pl.col('quadkey').n_unique().alias('quadkey_n_unique'),
                pl.col('upload_date').n_unique().alias('upload_date_n_unique'),
                pl.col('source_file').n_unique().alias('source_file_n_unique'),
                pl.col('country').drop_nulls().first().alias('country'),
                pl.col('quadkey').drop_nulls().first().alias('quadkey'),
                pl.col('upload_date').drop_nulls().first().alias('upload_date'),
                pl.col('source_file').drop_nulls().first().alias('source_file'),
            )
            .collect()
            .row(0, named=True)
        )

        if result['rows'] == 0:
            errors.append(f'{file.name}: empty bronze file')
            continue

        for column in ('country', 'quadkey', 'upload_date', 'source_file'):
            distinct_key = f'{column}_n_unique'
            if result[distinct_key] != 1:
                errors.append(f'{file.name}: {column} has {result[distinct_key]} distinct values')
                continue

            if result[column] != expected[column]:
                errors.append(
                    f'{file.name}: {column} mismatch expected={expected[column]!r} got={result[column]!r}'
                )

    return errors


def validate_geometry(bronze_files: list[Path]) -> list[str]:
    errors: list[str] = []

    for file in bronze_files:
        geometry_type = pl.col('geometry').struct.field('type')

        result = (
            pl.scan_parquet(file)
            .select(
                pl.col('geometry').null_count().alias('geometry_nulls'),
                geometry_type.null_count().alias('geometry_type_nulls'),
                geometry_type
                .filter(~geometry_type.is_in(list(VALID_GEOMETRY_TYPES)))
                .len()
                .alias('invalid_geometry_type_rows'),
                geometry_type.drop_nulls().unique().sort().alias('geometry_types'),
            )
            .collect()
            .row(0, named=True)
        )

        if result['geometry_nulls'] > 0:
            errors.append(f"{file.name}: geometry has {result['geometry_nulls']} null rows")

        if result['geometry_type_nulls'] > 0:
            errors.append(f"{file.name}: geometry.type has {result['geometry_type_nulls']} null rows")

        if result['invalid_geometry_type_rows'] > 0:
            errors.append(
                f"{file.name}: found {result['invalid_geometry_type_rows']} rows with unexpected geometry types "
                f"{result['geometry_types']}"
            )

    return errors


def run_bronze_validation(
    raw_dir: Path = Path('data/raw'),
    bronze_dir: Path = Path('data/bronze/buildings'),
) -> dict[str, object]:
    raw_files = list_files(raw_dir, '*.csv.gz')
    bronze_files = list_files(bronze_dir, '*.parquet')

    if not raw_files:
        errors = [f'No raw files found in {raw_dir}']
        return {'checks': [('input files', errors)], 'errors': errors}

    if not bronze_files:
        errors = [f'No bronze files found in {bronze_dir}']
        return {'checks': [('output files', errors)], 'errors': errors}

    checks = [
        ('file count and filename mapping', validate_file_count_and_mapping(raw_files, bronze_files)),
        ('metadata values', validate_metadata_values(bronze_files)),
        ('geometry', validate_geometry(bronze_files)),
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


def run_bronze(
    raw_dir: str = 'data/raw',
    bronze_dir: str = 'data/bronze/buildings',
) -> dict[str, object]:
    raw_path = Path(raw_dir)
    bronze_path = Path(bronze_dir)
    bronze_path.mkdir(parents=True, exist_ok=True)

    for path in sorted(raw_path.glob('*.csv.gz')):
        out = bronze_convert(path, bronze_path)
        print(f'Wrote: {out}')

    report = run_bronze_validation(raw_path, bronze_path)
    print_validation_report(report)
    if report['errors']:
        raise AssertionError('\n'.join(report['errors']))
    return report


if __name__ == '__main__':
    run_bronze()
