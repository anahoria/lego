with source_data as (
    select 
        sum(quantity) as quantity,
        'color' as agg_type,
        year,
        color_id,
        is_spare
    from 
        {{ ref("inventory_detailed_with_parts")}}
    group by 
        year,
        color_id,
        is_spare
    union all
    select 
        sum(t1.quantity) as quantity,
        'color_group' as agg_type,
        t1.year,
        t2.color_group_id as color_id,
        t1.is_spare
    from 
        {{ ref("inventory_detailed_with_parts")}} t1
    left join
        {{ ref("colors_and_groups")}} t2
    on
        t1.color_id = t2.color_id
    where 
        quantity is not null
    group by
        t1.year,
        t2.color_group_id,
        t1.is_spare
)

select * from source_data