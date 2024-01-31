import polars as pl
from uuid import uuid4

def staging_to_prepared():
  df_jan = pl.read_parquet('./data/staging/yellow_tripdata_2023-01.parquet')
  df_location = pl.read_parquet('./data/staging/zone_data.parquet')

  df_payment = df_jan[['payment_type', 'payment_type_name']].unique().sort(by='payment_type', descending=False)

  df_ratecode = df_jan[['rate_code_id', 'rate_code_type']].unique()

  df_ratecode = df_ratecode.with_columns(pl.col('rate_code_id').cast(pl.Int16)).sort(by='rate_code_id', descending=False)

  df_vendor = df_jan[['vendor_id', 'vendor_name']].unique().sort(by='vendor_id', descending=False)

  df_trip = df_jan.drop(['vendor_id', 'vendor_name', 'payment_type_name', 'pickup_borough', 'pickup_zone', 'pickup_service_zone',
                         'dropout_borough', 'dropout_zone', 'dropout_service_zone', 'rate_code_type'])
  
  df_trip = df_trip.with_row_index(name='trip_id')\
    .with_columns(pl.concat_str([pl.col('trip_id'), pl.col('pickup_datetime').str.replace(r'\s', '_')]).alias('trip_id'))

  df_trip.write_parquet('./data/prepared/fact_trip.parquet')
  df_payment.write_parquet('./data/prepared/dim_payment.parquet')
  df_ratecode.write_parquet('./data/prepared/dim_ratecode.parquet')
  df_vendor.write_parquet('./data/prepared/dim_vendor.parquet')
  df_location.write_parquet('./data/prepared/dim_location.parquet')

staging_to_prepared()