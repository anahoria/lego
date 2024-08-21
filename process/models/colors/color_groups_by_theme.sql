with aux1 as (
    select
        case when 
            t1.grandparent_theme_name is not null
        then 
            t1.grandparent_theme_name
        when 
            t1.parent_theme_name is not null 
        then
            t1.parent_theme_name
        else 
            t1.theme_name
        end as theme_name,
        t1.quantity,
        t2.color_group_id,
        t2.color_group_name,
        t2.color_group_rgb
    from 
        {{ ref("inventory_detailed_with_parts") }} t1
    left join
        {{ ref("colors_and_groups") }} t2
    on
        t1.color_id = t2.color_id
    where t1.color_id is not null    
),
aux2 as (
    select 
        case when 
            lower(theme_name) like '%town%'
        or
            theme_name = 'Town'
        then
            'City'
        else 
            theme_name
        end as
            theme_name,
        quantity,
        color_group_id,
        color_group_name,
        color_group_rgb
    from
        aux1
),
aux3 as (
    select
        theme_name,
        sum(quantity) as quantity,
        color_group_id,
        color_group_name,
        color_group_rgb
    from
        aux2
    group by
        theme_name,
        color_group_id,
        color_group_name,
        color_group_rgb
),
aux4 as (
    select
        theme_name,
        max(quantity) over(partition by theme_name) as max_quantity,
        quantity,
        color_group_id,
        color_group_name,
        color_group_rgb
    from
        aux3
),
source_data as (
    select
        theme_name,
        quantity,
        color_group_id,
        color_group_name,
        color_group_rgb
    from
        aux4
    where quantity = max_quantity
)
select * from source_data