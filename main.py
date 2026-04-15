import argparse
from pathlib import Path

from pipeline.bronze import run_bronze
from pipeline.gold import run_gold
from pipeline.silver import run_silver


DEFAULT_RAW_DIR = Path('data/raw')
DEFAULT_BRONZE_DIR = Path('data/bronze/buildings')
DEFAULT_SILVER_DIR = Path('data/silver/buildings')
DEFAULT_GOLD_DIR = Path('data/gold')
DEFAULT_ADM1_PATH = Path('data/boundaries/geoBoundaries-IDN-ADM1-provinces.geojson')
DEFAULT_ADM2_PATH = Path('data/boundaries/geoBoundaries-IDN-ADM2-districts.geojson')


def build_stage_checks(stage: str, args: argparse.Namespace) -> tuple[list[str], list[str]]:
    errors: list[str] = []
    details: list[str] = []

    raw_count = len(list(args.raw_dir.glob('*.csv.gz'))) if args.raw_dir.exists() else 0
    bronze_count = len(list(args.bronze_dir.glob('*.parquet'))) if args.bronze_dir.exists() else 0
    silver_count = len(list(args.silver_dir.glob('*.parquet'))) if args.silver_dir.exists() else 0

    details.append(f'raw files: {raw_count} in {args.raw_dir}')
    details.append(f'bronze files: {bronze_count} in {args.bronze_dir}')
    details.append(f'silver files: {silver_count} in {args.silver_dir}')
    details.append(f'adm1 exists: {args.adm1_path.exists()} at {args.adm1_path}')
    details.append(f'adm2 exists: {args.adm2_path.exists()} at {args.adm2_path}')

    if stage in {'bronze', 'all'} and raw_count == 0:
        errors.append(f'No raw files found in {args.raw_dir}')

    if stage == 'silver' and bronze_count == 0:
        errors.append(f'No bronze parquet files found in {args.bronze_dir}')

    if stage == 'gold' and silver_count == 0:
        errors.append(f'No silver parquet files found in {args.silver_dir}')

    if stage in {'gold', 'all'}:
        if not args.adm1_path.exists():
            errors.append(f'ADM1 boundary file is missing: {args.adm1_path}')
        if not args.adm2_path.exists():
            errors.append(f'ADM2 boundary file is missing: {args.adm2_path}')

    return errors, details


def run_preflight(stage: str, args: argparse.Namespace) -> None:
    errors, details = build_stage_checks(stage, args)

    print(f'[CHECK] stage={stage}')
    for detail in details:
        print(f'  - {detail}')

    if errors:
        for error in errors:
            print(f'[FAIL] {error}')
        raise SystemExit(1)

    print('[OK] preflight checks passed')


def add_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument('--raw-dir', type=Path, default=DEFAULT_RAW_DIR)
    parser.add_argument('--bronze-dir', type=Path, default=DEFAULT_BRONZE_DIR)
    parser.add_argument('--silver-dir', type=Path, default=DEFAULT_SILVER_DIR)
    parser.add_argument('--gold-dir', type=Path, default=DEFAULT_GOLD_DIR)
    parser.add_argument('--adm1-path', type=Path, default=DEFAULT_ADM1_PATH)
    parser.add_argument('--adm2-path', type=Path, default=DEFAULT_ADM2_PATH)
    parser.add_argument('--max-files', type=int, default=None)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='Run the QuadLake Indonesia pipeline from raw inputs onward.'
    )
    subparsers = parser.add_subparsers(dest='command', required=True)

    for command in ('bronze', 'silver', 'gold', 'all'):
        subparser = subparsers.add_parser(command)
        add_common_args(subparser)

    check_inputs = subparsers.add_parser('check-inputs')
    add_common_args(check_inputs)
    check_inputs.add_argument(
        '--stage',
        choices=('bronze', 'silver', 'gold', 'all'),
        default='all',
    )

    return parser


def run_command(args: argparse.Namespace) -> None:
    if args.command == 'check-inputs':
        run_preflight(args.stage, args)
        return

    if args.command == 'bronze':
        run_preflight('bronze', args)
        run_bronze(
            raw_dir=args.raw_dir.as_posix(),
            bronze_dir=args.bronze_dir.as_posix(),
            max_files=args.max_files,
        )
        return

    if args.command == 'silver':
        run_preflight('silver', args)
        run_silver(
            bronze_dir=args.bronze_dir.as_posix(),
            silver_dir=args.silver_dir.as_posix(),
            max_files=args.max_files,
        )
        return

    if args.command == 'gold':
        run_preflight('gold', args)
        run_gold(
            silver_dir=args.silver_dir.as_posix(),
            gold_dir=args.gold_dir.as_posix(),
            adm1_path=args.adm1_path.as_posix(),
            adm2_path=args.adm2_path.as_posix(),
            max_files=args.max_files,
        )
        return

    if args.command == 'all':
        run_preflight('all', args)
        run_bronze(
            raw_dir=args.raw_dir.as_posix(),
            bronze_dir=args.bronze_dir.as_posix(),
            max_files=args.max_files,
        )
        run_silver(
            bronze_dir=args.bronze_dir.as_posix(),
            silver_dir=args.silver_dir.as_posix(),
            max_files=args.max_files,
        )
        run_gold(
            silver_dir=args.silver_dir.as_posix(),
            gold_dir=args.gold_dir.as_posix(),
            adm1_path=args.adm1_path.as_posix(),
            adm2_path=args.adm2_path.as_posix(),
            max_files=args.max_files,
        )


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    run_command(args)


if __name__ == '__main__':
    main()