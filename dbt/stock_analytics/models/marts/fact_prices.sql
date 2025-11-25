-- Fact table: daily prices per symbol.

select
  m.date,
  m.symbol,
  m.open,
  m.high,
  m.low,
  m.close,
  m.volume
from {{ ref('stg_market_data') }} m
