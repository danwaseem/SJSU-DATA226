select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select symbol
from USER_DB_HEDGEHOG.ANALYTICS.indicators_ma_rsi
where symbol is null



      
    ) dbt_internal_test