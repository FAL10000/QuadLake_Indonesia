import argparse
from pathlib import Path


def list_files(path: Path, pattern: str) -> list[Path]:
    return sorted(path.glob(pattern))


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


def main() -> int:
    bronze_dir = Path('data/bronze/buildings')
    silver_dir = Path('data/silver/buildings')

    bronze_files = list_files(bronze_dir, '*.parquet')
    silver_files = list_files(silver_dir, '*.parquet')

    if not bronze_files:
        print(f'No bronze files found in {bronze_dir}')
        return 1

    if not silver_files:
        print(f'No silver files found in {silver_dir}')
        return 1

    errors = validate_file_mapping(bronze_files, silver_files)

    if errors:
        for error in errors:
            print(error)
        return 1

    print('File count OK')
    print('Bronze and Silver filename mapping OK')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())