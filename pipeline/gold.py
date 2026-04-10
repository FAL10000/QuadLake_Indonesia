from pathlib import Path
import geopandas as gpd
import pandas as pd
import polars as pl
from shapely.geometry import shape


def load_building_points(silver_path: Path, crs):
    frame = (
        pl.read_parquet(silver_path)
        .select('silver_record_id', 'quadkey', 'source_file', 'geometry')
        .to_pandas()
    )
    frame['building_point'] = [
        shape(geometry).representative_point()
        for geometry in frame['geometry']
    ]
    return gpd.GeoDataFrame(
        frame[['silver_record_id', 'quadkey', 'source_file', 'building_point']],
        geometry='building_point',
        crs=crs,
    )


def summarize_spatial_join(building_points, boundaries, name_columns):
    joined = (
        gpd.sjoin(
            building_points,
            boundaries[name_columns + ['geometry']],
            how='left',
            predicate='within',
        )
        .drop(columns='index_right', errors='ignore')
    )
    counts = (
        joined.dropna(subset=[name_columns[0]])
        .groupby(name_columns)
        .size()
        .reset_index(name='building_count')
    )
    unmatched = joined.loc[
        joined[name_columns[0]].isna(),
        ['silver_record_id', 'quadkey', 'source_file', 'building_point'],
    ].copy()
    return counts, unmatched


def nearest_boundary_join(unmatched_gdf, boundary_gdf, keep_columns):
    joined = (
        gpd.sjoin_nearest(
            unmatched_gdf,
            boundary_gdf[keep_columns + ['geometry']],
            how='left',
            distance_col='distance_m',
        )
    )

    joined = (
        joined
        .sort_values(['silver_record_id', 'distance_m', *keep_columns], kind='stable')
        .drop_duplicates(subset='silver_record_id', keep='first')
        .drop(columns=['index_right', '__index_level_0__'], errors='ignore')
        .to_crs('EPSG:4326')
        .reset_index(drop=True)
    )

    return joined


def run_gold(
    silver_dir: str = 'data/silver/buildings',
    gold_dir: str = 'data/gold',
    adm1_path: str = 'data/boundaries/geoBoundaries-IDN-ADM1-provinces.geojson',
    adm2_path: str = 'data/boundaries/geoBoundaries-IDN-ADM2-districts.geojson',
) -> dict[str, int]:
    silver_dir = Path(silver_dir)
    gold_dir = Path(gold_dir)
    gold_dir.mkdir(parents=True, exist_ok=True)
    adm1_path = Path(adm1_path)
    adm2_path = Path(adm2_path)

    silver_glob = (silver_dir / '*.parquet').as_posix()
    all_silver = pl.scan_parquet(silver_glob)

    building_count_quadkey = (
        all_silver
        .group_by('quadkey')
        .agg(
            pl.len().alias('building_count')
        )
        .collect()
    )

    building_count_quadkey.write_parquet(gold_dir / 'building_count_quadkey.parquet')

    building_count_country = (
        all_silver
        .select(
            pl.col('country').first(),
            pl.len().alias('building_count')
        )
        .collect()
    )

    building_count_country.write_parquet(gold_dir / 'building_count_country.parquet')

    silver_files = sorted(silver_dir.glob('*.parquet'))
    provinces = (
        gpd.read_file(adm1_path)
        [['shapeName', 'shapeISO', 'geometry']]
        .rename(columns={'shapeName': 'province_name', 'shapeISO': 'province_code'})
    )
    districts = (
        gpd.read_file(adm2_path)
        [['shapeName', 'shapeID', 'geometry']]
        .rename(columns={'shapeName': 'district_name', 'shapeID': 'district_code'})
    )

    print(f'Loaded {len(silver_files)} silver files')
    print(provinces[['province_name', 'province_code']].head())
    print(districts[['district_name', 'district_code']].head())

    province_count_frames = []
    province_unmatched_frames = []

    for i, silver_path in enumerate(silver_files, start=1):
        building_points = load_building_points(silver_path, provinces.crs)
        province_counts, province_unmatched = summarize_spatial_join(
            building_points,
            provinces,
            ['province_name', 'province_code'],
        )
        province_count_frames.append(province_counts)
        province_unmatched_frames.append(province_unmatched)

        if i % 50 == 0 or i == len(silver_files):
            print(f'Processed {i}/{len(silver_files)} silver files for provinces')

    province_counts = (
        pd.concat(province_count_frames, ignore_index=True)
        .groupby(['province_name', 'province_code'], as_index=False)['building_count']
        .sum()
        .sort_values(['province_name', 'province_code'])
    )
    province_unmatched = pd.concat(province_unmatched_frames, ignore_index=True)

    pl.from_pandas(province_counts).write_parquet(
        gold_dir / 'building_counts_by_province.parquet'
    )
    gpd.GeoDataFrame(
        province_unmatched,
        geometry='building_point',
        crs=provinces.crs,
    ).to_parquet(
        gold_dir / 'building_counts_by_province_unmatched.parquet'
    )

    print(gold_dir / 'building_counts_by_province.parquet')
    print(gold_dir / 'building_counts_by_province_unmatched.parquet')

    district_count_frames = []
    district_unmatched_frames = []

    for i, silver_path in enumerate(silver_files, start=1):
        building_points = load_building_points(silver_path, districts.crs)
        district_counts, district_unmatched = summarize_spatial_join(
            building_points,
            districts,
            ['district_name', 'district_code'],
        )
        district_count_frames.append(district_counts)
        district_unmatched_frames.append(district_unmatched)

        if i % 50 == 0 or i == len(silver_files):
            print(f'Processed {i}/{len(silver_files)} silver files for districts')

    district_counts = (
        pd.concat(district_count_frames, ignore_index=True)
        .groupby(['district_name', 'district_code'], as_index=False)['building_count']
        .sum()
        .sort_values(['district_name', 'district_code'])
    )
    district_unmatched = pd.concat(district_unmatched_frames, ignore_index=True)

    pl.from_pandas(district_counts).write_parquet(
        gold_dir / 'building_counts_by_district.parquet'
    )
    gpd.GeoDataFrame(
        district_unmatched,
        geometry='building_point',
        crs=districts.crs,
    ).to_parquet(
        gold_dir / 'building_counts_by_district_unmatched.parquet'
    )

    print(gold_dir / 'building_counts_by_district.parquet')
    print(gold_dir / 'building_counts_by_district_unmatched.parquet')

    province_unmatched_gdf = gpd.read_parquet(
        gold_dir / 'building_counts_by_province_unmatched.parquet'
    )
    district_unmatched_gdf = gpd.read_parquet(
        gold_dir / 'building_counts_by_district_unmatched.parquet'
    )

    province_unmatched_metric = province_unmatched_gdf.to_crs('EPSG:3857')
    district_unmatched_metric = district_unmatched_gdf.to_crs('EPSG:3857')
    provinces_metric = provinces.to_crs('EPSG:3857')
    districts_metric = districts.to_crs('EPSG:3857')

    print(f'Province unmatched rows: {len(province_unmatched_gdf):,}')
    print(f'District unmatched rows: {len(district_unmatched_gdf):,}')

    nearest_province_for_province_unmatched = nearest_boundary_join(
        province_unmatched_metric,
        provinces_metric,
        ['province_name', 'province_code'],
    )

    nearest_province_for_province_unmatched.to_parquet(
        gold_dir / 'building_counts_by_province_unmatched_nearest_adm1.parquet'
    )

    print(gold_dir / 'building_counts_by_province_unmatched_nearest_adm1.parquet')

    nearest_district_for_district_unmatched = nearest_boundary_join(
        district_unmatched_metric,
        districts_metric,
        ['district_name', 'district_code'],
    )

    nearest_district_for_district_unmatched.to_parquet(
        gold_dir / 'building_counts_by_district_unmatched_nearest_adm2.parquet'
    )

    print(gold_dir / 'building_counts_by_district_unmatched_nearest_adm2.parquet')

    prov_match = pl.from_pandas(province_counts)
    prov_unmatch = (
        pl.read_parquet(gold_dir / 'building_counts_by_province_unmatched_nearest_adm1.parquet')
        .select('province_name', 'province_code')
        .group_by(['province_name', 'province_code'])
        .len()
        .rename({'len': 'building_count'})
    )

    prov_final = (
        prov_match
        .join(
            prov_unmatch,
            on=['province_name', 'province_code'],
            how='full',
            suffix='_unmatched',
        )
        .with_columns(
            pl.col('building_count').fill_null(0),
            pl.col('building_count_unmatched').fill_null(0),
        )
        .with_columns(
            (pl.col('building_count') + pl.col('building_count_unmatched')).alias('building_count'),
            pl.coalesce('province_name', 'province_name_unmatched').alias('province_name'),
            pl.coalesce('province_code', 'province_code_unmatched').alias('province_code'),
        )
        .select(['province_name', 'province_code', 'building_count'])
        .sort(['province_name', 'province_code'])
    )

    prov_final.write_parquet(gold_dir / 'building_counts_by_province.parquet')

    dis_match = pl.from_pandas(district_counts)
    dis_unmatch = (
        pl.read_parquet(gold_dir / 'building_counts_by_district_unmatched_nearest_adm2.parquet')
        .select('district_name', 'district_code')
        .group_by(['district_name', 'district_code'])
        .len()
        .rename({'len': 'building_count'})
    )

    dis_final = (
        dis_match
        .join(
            dis_unmatch,
            on=['district_name', 'district_code'],
            how='full',
            suffix='_unmatched',
        )
        .with_columns(
            pl.col('building_count').fill_null(0),
            pl.col('building_count_unmatched').fill_null(0),
        )
        .with_columns(
            (pl.col('building_count') + pl.col('building_count_unmatched')).alias('building_count'),
            pl.coalesce('district_name', 'district_name_unmatched').alias('district_name'),
            pl.coalesce('district_code', 'district_code_unmatched').alias('district_code'),
        )
        .select(['district_name', 'district_code', 'building_count'])
        .sort(['district_name', 'district_code'])
    )

    dis_final.write_parquet(gold_dir / 'building_counts_by_district.parquet')

    province_boundary_keys = {
        tuple(row)
        for row in provinces[['province_name', 'province_code']]
        .drop_duplicates()
        .itertuples(index=False, name=None)
    }
    district_boundary_keys = {
        tuple(row)
        for row in districts[['district_name', 'district_code']]
        .drop_duplicates()
        .itertuples(index=False, name=None)
    }

    country_total = int(building_count_country['building_count'][0])
    quadkey_total = int(building_count_quadkey['building_count'].sum())
    province_total = int(prov_final['building_count'].sum())
    district_total = int(dis_final['building_count'].sum())

    province_unmatched_rows = len(province_unmatched_gdf)
    district_unmatched_rows = len(district_unmatched_gdf)
    province_recovered_rows = len(nearest_province_for_province_unmatched)
    district_recovered_rows = len(nearest_district_for_district_unmatched)

    province_recovered_duplicate_ids = int(
        nearest_province_for_province_unmatched['silver_record_id'].duplicated().sum()
    )
    district_recovered_duplicate_ids = int(
        nearest_district_for_district_unmatched['silver_record_id'].duplicated().sum()
    )

    province_null_key_rows = prov_final.filter(
        pl.any_horizontal([pl.col('province_name').is_null(), pl.col('province_code').is_null()])
    ).height
    district_null_key_rows = dis_final.filter(
        pl.any_horizontal([pl.col('district_name').is_null(), pl.col('district_code').is_null()])
    ).height

    province_duplicate_key_rows = (
        prov_final.group_by(['province_name', 'province_code']).len().filter(pl.col('len') > 1).height
    )
    district_duplicate_key_rows = (
        dis_final.group_by(['district_name', 'district_code']).len().filter(pl.col('len') > 1).height
    )

    province_negative_rows = prov_final.filter(pl.col('building_count') < 0).height
    district_negative_rows = dis_final.filter(pl.col('building_count') < 0).height

    province_observed_keys = {
        tuple(row) for row in prov_final.select(['province_name', 'province_code']).iter_rows()
    }
    district_observed_keys = {
        tuple(row) for row in dis_final.select(['district_name', 'district_code']).iter_rows()
    }

    province_unexpected_keys = sorted(province_observed_keys - province_boundary_keys)
    district_unexpected_keys = sorted(district_observed_keys - district_boundary_keys)

    errors: list[str] = []

    if country_total != quadkey_total:
        errors.append(f'country/quadkey mismatch: country={country_total} quadkey={quadkey_total}')
    if country_total != province_total:
        errors.append(f'country/province mismatch: country={country_total} province={province_total}')
    if country_total != district_total:
        errors.append(f'country/district mismatch: country={country_total} district={district_total}')

    if province_unmatched_rows != province_recovered_rows:
        errors.append(
            'province unmatched/recovered row mismatch: '
            f'unmatched={province_unmatched_rows} recovered={province_recovered_rows}'
        )
    if district_unmatched_rows != district_recovered_rows:
        errors.append(
            'district unmatched/recovered row mismatch: '
            f'unmatched={district_unmatched_rows} recovered={district_recovered_rows}'
        )

    if province_recovered_duplicate_ids > 0:
        errors.append(
            f'province recovered assignments contain {province_recovered_duplicate_ids} duplicate silver_record_id values'
        )
    if district_recovered_duplicate_ids > 0:
        errors.append(
            f'district recovered assignments contain {district_recovered_duplicate_ids} duplicate silver_record_id values'
        )

    if province_null_key_rows > 0:
        errors.append(f'province output contains {province_null_key_rows} rows with null boundary keys')
    if district_null_key_rows > 0:
        errors.append(f'district output contains {district_null_key_rows} rows with null boundary keys')
    if province_duplicate_key_rows > 0:
        errors.append(f'province output contains {province_duplicate_key_rows} duplicate boundary key rows')
    if district_duplicate_key_rows > 0:
        errors.append(f'district output contains {district_duplicate_key_rows} duplicate boundary key rows')
    if province_negative_rows > 0:
        errors.append(f'province output contains {province_negative_rows} negative building_count rows')
    if district_negative_rows > 0:
        errors.append(f'district output contains {district_negative_rows} negative building_count rows')
    if province_unexpected_keys:
        errors.append(f'province output contains unexpected boundary keys: {province_unexpected_keys[:5]}')
    if district_unexpected_keys:
        errors.append(f'district output contains unexpected boundary keys: {district_unexpected_keys[:5]}')

    if errors:
        raise AssertionError('\n'.join(errors))

    return {
        'country_total': country_total,
        'quadkey_total': quadkey_total,
        'province_total': province_total,
        'district_total': district_total,
        'province_unmatched_rows': province_unmatched_rows,
        'district_unmatched_rows': district_unmatched_rows,
        'province_recovered_rows': province_recovered_rows,
        'district_recovered_rows': district_recovered_rows,
        'province_recovered_duplicate_ids': province_recovered_duplicate_ids,
        'district_recovered_duplicate_ids': district_recovered_duplicate_ids,
        'province_output_rows': prov_final.height,
        'district_output_rows': dis_final.height,
    }


if __name__ == '__main__':
    totals = run_gold()
    for key, value in totals.items():
        print(f'{key}: {value}')
