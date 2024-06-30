with aux1color as (
    select 
        sum(coalesce(quantity, 0)) as quantity,
        color_id
    from 
        {{ ref("inventory_detailed_with_parts")}}
    group by 
        color_id
),
aux1colorgroup as (
    select
        sum(coalesce(t1.quantity)) as quantity,
        t2.color_group
    from 
        {{ ref("inventory_detailed_with_parts")}} t1
    left join
        {{ source("main", "color_simplified")}} t2
    on 
        t1.color_id = t2.id
    group by
        t2.color_group
),
aux2color as (
    select 
        rank() over(order by quantity desc, color_id asc) as color_id_by_quantity,
        color_id
    from
        aux1color
),
aux2colorgroup as (
    select 
        rank() over(order by quantity desc) as color_group_id_by_quantity,
        color_group
    from
        aux1colorgroup
),
source_data as (
    select 
        t1.id as color_id,
        t1.color_name,
        t1.rgb as color_rgb,
        t3.color_group_id_by_quantity as color_group_id,
        t1.color_group as color_group_name,
        t1.group_rgb as color_group_rgb,
        t1.is_trans,
        t2.color_id_by_quantity,
        t3.color_group_id_by_quantity as color_group_id_by_quantity
    from
        {{ source("main", "color_simplified") }} t1
    left join 
        aux2color t2
    on 
        t1.id = t2.color_id
    left join 
        aux2colorgroup t3
    on 
        t1.color_group = t3.color_group
)

select * from source_data