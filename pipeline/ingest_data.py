import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm


dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]


@click.command()
@click.option("--pg-user", default="root", help="Postgres user")
@click.option("--pg-pass", default="root", help="Postgres password")
@click.option("--pg-host", default="localhost", help="Postgres host")
@click.option("--pg-port", type=int, default=5432, help="Postgres port")
@click.option("--pg-db", default="ny_taxi", help="Postgres database")
@click.option("--year", type=int, default=2021, help="Data year")
@click.option("--month", type=int, default=1, help="Data month (1-12)")
@click.option(
    "--chunksize",
    type=int,
    default=100_000,
    help="Number of rows per chunk when reading the CSV",
)
@click.option(
    "--target-table",
    default="yellow_taxi_data",
    help="Destination table name in Postgres",
)
def run(
    pg_user: str,
    pg_pass: str,
    pg_host: str,
    pg_port: int,
    pg_db: str,
    year: int,
    month: int,
    chunksize: int,
    target_table: str,
) -> None:
    first = True

    prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow'
    url = f'{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz'

    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')


    df_iter = pd.read_csv(
    url,
    dtype=dtype,
    parse_dates=parse_dates,
    iterator=True,
    chunksize=chunksize
    )

    for df_chunk in tqdm(df_iter):

        if first:
            df_chunk.head(0).to_sql(
                name=target_table, 
                con=engine, 
                if_exists='replace'
                )
            first = False
        
        df_chunk.to_sql(
            name=target_table, 
            con=engine, 
            if_exists='append'
            )


if __name__ == '__main__':
    run()


# print(pd.io.sql.get_schema(df,name='yellow_taxi_data',con=engine))




