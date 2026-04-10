from pathlib import Path
import polars as pl
import re


def validate_file_count_and_mapping(raw_dir: Path, bronze_dir: Path) -> list[str]:
    raw_files = sorted(raw_dir.glob('*.csv.gz'))
    bronze_files = sorted(bronze_dir.glob('*.parquet'))

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


def validate_metadata_values(bronze_dir: Path) -> list[str]:
    bronze_files = sorted(bronze_dir.glob('*.parquet'))
    pattern = re.compile(
        r'(?P<country>.+)_(?P<quadkey>\d+)_(?P<upload_date>\d{4}-\d{2}-\d{2})\.parquet$'
    )

    errors: list[str] = []

    for file in bronze_files:
        match = pattern.match(file.name)
        if match is None:
            errors.append(f'{file.name}: invalid bronze filename')
            continue

        meta = match.groupdict()
        source_file = file.name.replace('.parquet', '.csv.gz')

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

        expected = {
            'country': meta['country'],
            'quadkey': meta['quadkey'],
            'upload_date': meta['upload_date'],
            'source_file': source_file,
        }

        for column in ('country', 'quadkey', 'upload_date', 'source_file'):
            if result[f'{column}_n_unique'] != 1:
                errors.append(f'{file.name}: {column} has {result[f"{column}_n_unique"]} distinct values')
                continue

            if result[column] != expected[column]:
                errors.append(
                    f'{file.name}: {column} mismatch expected={expected[column]!r} got={result[column]!r}'
                )

    return errors


def validate_geometry(bronze_dir: Path) -> list[str]:
    bronze_files = sorted(bronze_dir.glob('*.parquet'))
    errors: list[str] = []

    for file in bronze_files:
        geometry_type = pl.col('geometry').struct.field('type')
        result = (
            pl.scan_parquet(file)
            .select(
                pl.col('geometry').null_count().alias('geometry_nulls'),
                geometry_type.null_count().alias('geometry_type_nulls'),
                geometry_type
                .filter(~geometry_type.is_in(['Polygon', 'MultiPolygon']))
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


def print_validation_report(checks: list[tuple[str, list[str]]]) -> None:
    for label, errors in checks:
        if errors:
            print(f'[FAIL] {label}')
            for error in errors:
                print(f'  - {error}')
        else:
            print(f'[OK] {label}')


def run_bronze(
    raw_dir: str = 'data/raw',
    bronze_dir: str = 'data/bronze/buildings',
) -> list[tuple[str, list[str]]]:
    raw_dir = Path(raw_dir)
    bronze_dir = Path(bronze_dir)
    bronze_dir.mkdir(parents=True, exist_ok=True)

    pattern = re.compile(
        r'(?P<country>.+)_(?P<quadkey>\d+)_(?P<upload_date>\d{4}-\d{2}-\d{2})\.csv\.gz$'
    )

    for path in raw_dir.glob('*.csv.gz'):
        match = pattern.match(path.name)
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

        print(f'Wrote: {out_path}')

    checks = [
        ('file count and filename mapping', validate_file_count_and_mapping(raw_dir, bronze_dir)),
        ('metadata values', validate_metadata_values(bronze_dir)),
        ('geometry', validate_geometry(bronze_dir)),
    ]
    print_validation_report(checks)

    errors = [error for _, check_errors in checks for error in check_errors]
    if errors:
        raise AssertionError('\n'.join(errors))

    return checks


if __name__ == '__main__':
    run_bronze()