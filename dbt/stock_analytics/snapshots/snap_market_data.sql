{% snapshot snap_market_data %}

{{
  config(
    target_schema = env_var('SF_ANALYTICS_SCHEMA', 'ANALYTICS'),
    unique_key    = 'snapshot_key',
    strategy      = 'check',
    check_cols    = ['close', 'volume']
  )
}}

select
  concat(to_char(date, 'YYYY-MM-DD'), '::', symbol) as snapshot_key,
  date,
  symbol,
  close,
  volume,
  current_timestamp() as updated_at
from {{ source('raw', 'market_data') }}

{% endsnapshot %}
