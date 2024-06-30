with source_data as (
    select
        year,
        count(distinct id) as n_items
    from 
        {{ ref("inventory_nums") }}
    where 
        year is not null
    group by 
        year
)

select * from source_data