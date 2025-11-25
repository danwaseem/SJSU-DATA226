-- Simple calendar table covering ~2 years from the min date in staging data.

with bounds as (
  select
    min(date) as min_date
  from USER_DB_HEDGEHOG.ANALYTICS.stg_market_data
),
base as (
  select
    dateadd(day, seq4(), min_date) as date
  from bounds,
       table(generator(rowcount => 730)) -- 2 years
)

select
  date,
  year(date)              as year,
  month(date)             as month,
  day(date)               as day_of_month,
  dayname(date)           as day_name,
  week(date)              as week_of_year,
  to_char(date, 'YYYY-MM-DD') as date_key
from base