select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select snapshot_key
from USER_DB_HEDGEHOG.ANALYTICS.snap_market_data
where snapshot_key is null



      
    ) dbt_internal_test