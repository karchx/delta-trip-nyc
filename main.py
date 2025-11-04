import os
import polars as pl
from deltalake import DeltaTable, write_deltalake
import duckdb

def write_approach():
    taxi_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-06.parquet"

    data_source = pl.read_parquet(taxi_url).head(10000)
    write_deltalake("data/taxi_delta_table", data_source)
    print("Create Delta Table")

    late_data = pl.read_parquet(taxi_url)

    df = late_data[10000:10050]
    print(f"New data to add {len(df)} rows")

    complete_df = pl.concat([data_source, df])
    complete_df.write_parquet("data/complete_taxi_data.parquet")

    print(f"Processed {len(complete_df)} rows in total")

    write_deltalake("data/taxi_delta_table", df, mode="append")

    dt = DeltaTable("data/taxi_delta_table")
    print(f"Added {len(df)} rows to Delta Table")
    print(f"Table version: {dt.version()}")

def get_v0():
    dt_v0 = DeltaTable("data/taxi_delta_table", version=0)
    df_v0 = pl.read_delta(dt_v0)
    current_dt = DeltaTable("data/taxi_delta_table")

    current_df = pl.read_delta(current_dt)

    print(f"Version 0: {len(df_v0)} records")
    print(f"Current Version: {len(current_df)} records")
    print(f"Available versions: {current_dt.version() + 1}")

def history(dt: DeltaTable):
    history = dt.history()
    for entry in history[:2]:
        print(f"Version: {entry['version']}, Timestamp: {entry['timestamp']}, Operation: {entry['operation']}")

def schema_evolution(taxi_url: str):
    data_source = pl.read_parquet(taxi_url).head(10000)
    # copy existing data
    enhanced_data = pl.read_parquet(taxi_url)[20000:20100].clone()

    weather_options = [
        'clear',
        'rain',
        'snow',
        'cloudy',
    ]
    surge_options = [1.0, 1.2, 1.5, 2.0]
    weather_col = [weather_options[i % 4] for i in range(len(enhanced_data))]
    surge_options_col = [surge_options[i % 4] for i in range(len(enhanced_data))]

    enhanced_data = enhanced_data.with_columns([
        pl.Series("weather_condition", weather_col),
        pl.Series("surge_multiplier", surge_options_col),
    ])

    print(f"Enhance data: {len(enhanced_data)} records with {len(enhanced_data.columns)} columns")
    print(f"New columns: {[col for col in enhanced_data.columns if col not in data_source.columns]}")

    # Schema evolution with automatic versioning
    write_deltalake(
        "data/taxi_delta_table",
        enhanced_data,
        mode="append",
        schema_mode="merge",
    )

    dt = DeltaTable("data/taxi_delta_table")
    dt_to_polars = pl.read_delta(dt)
    print(f"Schema evolved: {len(dt_to_polars.columns)} columns | Version: {dt.version()}")

def taxi_trips():
    # create initial delta
    data = {
        "trip_id": [1, 2, 3, 4, 5],
        "fare_amount": [15.5, 20.0, 18.3, 12.5, 25.0],
        "payment_type": [1, 1, 2, 1, 2],
    }
    trips = pl.DataFrame(data)
    write_deltalake("data/trips_merge_demo", trips, mode="overwrite")

    changes = pl.DataFrame({
        "trip_id": [2, 4, 6, 7],
        "fare_amount": [22.0, 13.8, 30.0, 16.5],
        "payment_type": [2, 2, 1, 1]
    })

    # Load Delta Table
    dt = DeltaTable("data/trips_merge_demo")
    (
        dt.merge(
            source=changes,
            predicate="target.trip_id = source.trip_id",
            source_alias="source",
            target_alias="target",
        )
        .when_matched_update(
            updates={
                "fare_amount": "source.fare_amount",
                "payment_type": "source.payment_type",
            }
        )
        .when_not_matched_insert(
            updates={
                "trip_id": "source.trip_id",
                "fare_amount": "source.fare_amount",
                "payment_type": "source.payment_type",
            }
        )
        .execute()
    )
    return pl.read_delta(dt)

def duckdb_source(df: pl.DataFrame):
    df.write_parquet("data/duckdb_optimized.parquet", compression="zstd")
    duckdb_result = duckdb.execute("""
        SELECT COUNT(*) AS trips, ROUND(AVG(fare_amount), 2) AS avg_fare
        FROM 'data/duckdb_optimized.parquet'
    """).fetchone()
    if duckdb_result is not None:
        print(f"DuckDB - Total Trips: {duckdb_result[0]}, Average Fare: ${duckdb_result[1]}")

def get_size(path: str) -> float:
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(path):
        for filename in filenames:
            fp = os.path.join(dirpath, filename)
            total_size += os.path.getsize(fp)
    return total_size / (1024 * 1024)  # size in MB

def main():
    taxi_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-06.parquet"
    dt = DeltaTable("data/taxi_delta_table")
    # write_approach()
    # get_v0()
    schema_evolution(taxi_url)
    df = taxi_trips()
    duckdb_source(df)

    before_size = get_size("data/taxi_delta_table")
    dt.vacuum(retention_hours=168)
    after_size = get_size("data/taxi_delta_table")
    print(f"Delta Table size before vacuum: {before_size:.1f} MB")
    print(f"Delta Table size after vacuum: {after_size:.1f} MB")
    print(f"Space reclaimed: {before_size - after_size:.1f} MB")

if __name__ == "__main__":
    main()
