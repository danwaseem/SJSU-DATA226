-- Staging view over RAW.MARKET_DATA
-- Keep it simple to avoid casting issues: RAW table already has proper types.

with src as (
  select * from {{ source('raw', 'market_data') }}
)

select
  date::date        as date,
  upper(symbol)     as symbol,
  open              as open,
  high              as high,
  low               as low,
  close             as close,
  volume            as volume
from src
