
      begin;
    merge into "USER_DB_HEDGEHOG"."ANALYTICS"."SNAP_MARKET_DATA" as DBT_INTERNAL_DEST
    using "USER_DB_HEDGEHOG"."ANALYTICS"."SNAP_MARKET_DATA__dbt_tmp" as DBT_INTERNAL_SOURCE
    on DBT_INTERNAL_SOURCE.dbt_scd_id = DBT_INTERNAL_DEST.dbt_scd_id

    when matched
     and DBT_INTERNAL_DEST.dbt_valid_to is null
     and DBT_INTERNAL_SOURCE.dbt_change_type in ('update', 'delete')
        then update
        set dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to

    when not matched
     and DBT_INTERNAL_SOURCE.dbt_change_type = 'insert'
        then insert ("SNAPSHOT_KEY", "DATE", "SYMBOL", "CLOSE", "VOLUME", "UPDATED_AT", "DBT_UPDATED_AT", "DBT_VALID_FROM", "DBT_VALID_TO", "DBT_SCD_ID")
        values ("SNAPSHOT_KEY", "DATE", "SYMBOL", "CLOSE", "VOLUME", "UPDATED_AT", "DBT_UPDATED_AT", "DBT_VALID_FROM", "DBT_VALID_TO", "DBT_SCD_ID")

;
    commit;
  