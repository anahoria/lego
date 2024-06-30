with aux as (
    select
        year,
        theme_id,
        theme_name,
        'son' as relative 
    from {{ ref("inventory_detailed") }}
    union all
    select
        year,
        parent_theme_id as theme_id,
        parent_theme_name as theme_name,
        'father' as relative
    from {{ ref("inventory_detailed") }}
    union all
    select
        year,
        grandparent_theme_id as theme_id,
        grandparent_theme_name as theme_name,
        'grandfather' as relative
    from {{ ref("inventory_detailed") }}
),
source_data as (
    select 
        distinct
        year, 
        theme_id, 
        theme_name,
        relative 
    from aux
    where theme_id is not null
)

select * from source_data