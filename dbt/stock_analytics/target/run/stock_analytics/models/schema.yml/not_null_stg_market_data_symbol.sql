select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select symbol
from USER_DB_HEDGEHOG.ANALYTICS.stg_market_data
where symbol is null



      
    ) dbt_internal_test