import polars as pl

df_jan = pl.read_parquet('./data/yellow_tripdata_2023-01.parquet')
df_zones = pl.read_csv('./data/zone_lookup.csv')

df_jan = df_jan.drop_nulls(subset=['passenger_count'])

# df_jan = df_jan.with_columns(
#   (pl.col('total_amount') - pl.col('congestion_surcharge') - pl.col('airport_fee') - pl.col('improvement_surcharge') -
#     pl.col('mta_tax') - pl.col('tolls_amount') - pl.col('extra'))\
#   .alias('total_amount_without_taxes')
# )

df_jan = df_jan.with_columns(
  pl.when(pl.col('VendorID') == 1).then(pl.lit('Creative Mobile Technologies, LLC')).otherwise(pl.lit('VeriFone Inc.'))\
    .alias('vendor_name')
)

df_jan = df_jan.with_columns(
  pl.when(pl.col('RatecodeID') == 1).then(pl.lit('Standard rate')).when(pl.col('RatecodeID') == 2).then(pl.lit('JFK'))\
    .when(pl.col('RatecodeID') == 3).then(pl.lit('Newark')).when(pl.col('RatecodeID') == 4).then(pl.lit('Nassau or Westchester'))\
    .when(pl.col('RatecodeID') == 5).then(pl.lit('Negotiated fare')).when(pl.col('RatecodeID') == 6).then(pl.lit('Group ride'))\
    .otherwise(pl.lit('Other')).alias('rate_code_type')
)

df_jan = df_jan.with_columns(
  pl.when(pl.col('payment_type') == 1).then(pl.lit("Credit card")).when(pl.col('payment_type') == 2).then(pl.lit("Cash"))\
    .when(pl.col("payment_type") == 3).then(pl.lit("No charg")).when(pl.col("payment_type") == 4).then(pl.lit("Dispute"))\
    .when(pl.col("payment_type") == 5).then(pl.lit("Unknown")).when(pl.col("payment_type") == 6).then(pl.lit("Voided trip"))\
    .otherwise(pl.lit("Other")).alias("payment_type_name")
)

df_joined = df_jan.join(df_zones, left_on="PULocationID", right_on="LocationID")

df_joined = df_joined.drop(['LocationID', 'store_and_fwd_flag'])

df_joined = df_joined.rename({"VendorID": "vendor_id", "tpep_pickup_datetime": "pickup_datetime", "tpep_dropoff_datetime": "dropoff_datetime",
                              "RatecodeID": "rate_code_id", "PULocationID": "pickup_location_id", "DOLocationID": "dropout_location_id",
                              "Borough": "pickup_borough", "Zone": "pickup_zone", "service_zone": "pickup_service_zone"})

df_joined = df_joined.join(df_zones, left_on="dropout_location_id", right_on="LocationID")

df_joined = df_joined.rename({"Borough": "dropout_borough", "Zone": "dropout_zone", "service_zone": "dropout_service_zone"})

print(df_joined.columns, df_joined.dtypes)