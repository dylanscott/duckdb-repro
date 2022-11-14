import duckdb
import pandas

conn = duckdb.connect(database=':memory:')

repro_df = pandas.read_parquet('repro.parquet', engine='fastparquet')
conn.register('repro', repro_df)

all_converted_df = conn.execute("""
select make_date(date_part(['year', 'month', 'day'], "ACTUAL_DATE")) as "ACTUAL_DATE" from repro
""").df()

less_filtered_df = conn.execute("""
select * from repro
where "FLAG" = true
      AND make_date(date_part(['year', 'month', 'day'], "ACTUAL_DATE")) >= '2021-01-01'
""").df()

erroring_df = conn.execute("""
select * from repro
where "FLAG" = true
      AND "HASH" = 'be763d263e00fe8a7c1e37ef441c5519'
      AND make_date(date_part(['year', 'month', 'day'], "ACTUAL_DATE")) >= '2021-01-01'
""").df()