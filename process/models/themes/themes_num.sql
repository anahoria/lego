with aux1 as (
    select
        concat(grandparent_theme_id, '-', grandparent_theme_name) as source_name,
        concat(parent_theme_id, '-', parent_theme_name) as target_name
    from {{ ref("inventory_detailed") }}
    where grandparent_theme_id is not null
    union all
    select
        concat(parent_theme_id, '-', parent_theme_name) as source_name,
        concat(theme_id, '-', theme_name) as target_name
    from {{ ref("inventory_detailed") }}
    where parent_theme_id is not null
    
),
aux2 as (
    select
        count(*) as n_items,
        source_name,
        target_name
    from 
        aux1
    group by
        source_name,
        target_name
),
source_data as (
    select 
        n_items,
        source_name,
        target_name
    from aux2
)

select * from source_data