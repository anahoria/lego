
with aux1 as (
    select
        year,
        case when 
            grandparent_theme_name is not null
        then 
            grandparent_theme_name
        when 
            parent_theme_name is not null 
        then
            parent_theme_name
        else 
            theme_name
        end as theme_name,
        case when 
            num_parts is null
        then
            0
        else 
            num_parts
        end as num_parts
    from 
        {{ ref("inventory_detailed") }}
    where 
        theme_name is not null
    and 
        year is not null
),
aux2 as (
    select 
        year,
        case when 
            lower(theme_name) like '%town%'
        then
            'City'
        else 
            theme_name
        end as
            theme_name,
        num_parts
    from 
        aux1
),
aux3 as (
    select
        year, 
        theme_name,
        count(*) as n_sets,
        sum(num_parts) as total_parts
    from 
            aux2 
    group by 
        year, theme_name
),
source_data as (
    select
        t1.year,
        t1.theme_name,
        t1.n_sets,
        t1.total_parts,
        t2.color_group_id,
        t2.color_group_name,
        t2.color_group_rgb
    from
        aux3 t1
    left join 
        {{ ref("color_groups_by_theme") }} t2
    on
        t1.theme_name = t2.theme_name
)

select * from source_data where color_group_id is not null