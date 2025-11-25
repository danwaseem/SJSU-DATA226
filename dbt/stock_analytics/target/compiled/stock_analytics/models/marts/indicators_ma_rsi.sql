-- Indicators: Moving Averages and RSI (14-day)
--
-- This is used for the BI dashboard (Lab 2 requirement).

with src as (
  select * from USER_DB_HEDGEHOG.ANALYTICS.stg_market_data
),
ma as (
  select
    date,
    symbol,
    close,
    avg(close) over (
      partition by symbol
      order by date
      rows between 6 preceding and current row
    ) as ma_7,
    avg(close) over (
      partition by symbol
      order by date
      rows between 13 preceding and current row
    ) as ma_14,
    avg(close) over (
      partition by symbol
      order by date
      rows between 29 preceding and current row
    ) as ma_30,
    lag(close) over (
      partition by symbol
      order by date
    ) as prev_close
  from src
),
gains_losses as (
  select
    date,
    symbol,
    close,
    ma_7,
    ma_14,
    ma_30,
    iff(prev_close is null, null, greatest(close - prev_close, 0)) as gain,
    iff(prev_close is null, null, greatest(prev_close - close, 0)) as loss
  from ma
),
rsi_window as (
  select
    date,
    symbol,
    close,
    ma_7,
    ma_14,
    ma_30,
    avg(gain) over (
      partition by symbol
      order by date
      rows between 13 preceding and current row
    ) as avg_gain_14,
    avg(loss) over (
      partition by symbol
      order by date
      rows between 13 preceding and current row
    ) as avg_loss_14
  from gains_losses
)

select
  date,
  symbol,
  close,
  ma_7,
  ma_14,
  ma_30,
  case
    when avg_loss_14 is null or avg_loss_14 = 0 then null
    else 100 - (100 / (1 + (avg_gain_14 / avg_loss_14)))
  end as rsi_14
from rsi_window