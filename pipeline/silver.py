from pathlib import Path
import polars as pl


def validate_file_mapping(bronze_dir: Path, silver_dir: Path) -> list[str]:
    bronze_files = sorted(bronze_dir.glob('*.parquet'))
    silver_files = sorted(silver_dir.glob('*.parquet'))

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


def validate_geometry_columns(silver_dir: Path) -> list[str]:
    silver_files = sorted(silver_dir.glob('*.parquet'))
    errors: list[str] = []
    expected_columns = {
        'country',
        'quadkey',
        'upload_date',
        'source_file',
        'source_row_index',
        'silver_record_id',
        'geometry',
        'geometry_type',
    }

    for file in silver_files:
        schema_names = set(pl.scan_parquet(file).collect_schema().names())

        if 'geometry' not in schema_names:
            errors.append(f'Missing geometry in {file.name}')
            continue

        if 'geometry_type' not in schema_names:
            errors.append(f'Missing geometry_type in {file.name}')
            continue

        unexpected_columns = sorted(schema_names - expected_columns)
        if unexpected_columns:
            errors.append(f'{file.name}: unexpected columns {unexpected_columns}')

        result = (
            pl.scan_parquet(file)
            .select(
                pl.len().alias('rows'),
                pl.col('geometry').null_count().alias('geometry_nulls'),
                pl.col('geometry_type').null_count().alias('geometry_type_nulls'),
            )
            .collect()
            .row(0, named=True)
        )

        if result['rows'] == 0:
            errors.append(f'Empty silver file: {file.name}')
            continue

        if result['geometry_nulls'] > 0:
            errors.append(f"{file.name}: geometry has {result['geometry_nulls']} null rows")

        if result['geometry_type_nulls'] > 0:
            errors.append(f"{file.name}: geometry_type has {result['geometry_type_nulls']} null rows")

    return errors


def print_validation_report(checks: list[tuple[str, list[str]]]) -> None:
    for label, errors in checks:
        if errors:
            print(f'[FAIL] {label}')
            for error in errors:
                print(f'  - {error}')
        else:
            print(f'[OK] {label}')


def run_silver(
    bronze_dir: str = 'data/bronze/buildings',
    silver_dir: str = 'data/silver/buildings',
) -> list[tuple[str, list[str]]]:
    bronze_dir = Path(bronze_dir)
    silver_dir = Path(silver_dir)
    silver_dir.mkdir(parents=True, exist_ok=True)

    for file in sorted(bronze_dir.glob('*.parquet')):
        out = silver_dir / file.name

        lf = (
            pl.scan_parquet(file)
            .with_row_index('source_row_index')
            .with_columns(
                pl.col('upload_date').str.to_date('%Y-%m-%d'),
                pl.concat_str(
                    [pl.col('source_file'), pl.lit(':'), pl.col('source_row_index').cast(pl.Utf8)]
                ).alias('silver_record_id'),
                pl.col('geometry').struct.field('type').alias('geometry_type'),
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
            )
        )

        lf.sink_parquet(out)
        print(f'Wrote: {out}')

    checks = [
        ('file mapping', validate_file_mapping(bronze_dir, silver_dir)),
        ('geometry columns', validate_geometry_columns(silver_dir)),
    ]
    print_validation_report(checks)

    errors = [error for _, check_errors in checks for error in check_errors]
    if errors:
        raise AssertionError('\n'.join(errors))

    return checks


if __name__ == '__main__':
    run_silver()
