-- Fact table: daily prices per symbol.

select
  m.date,
  m.symbol,
  m.open,
  m.high,
  m.low,
  m.close,
  m.volume
from USER_DB_HEDGEHOG.ANALYTICS.stg_market_data m