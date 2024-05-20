with weighted_groups as (

    select 
        sum(n_pieces) as group_pieces,
        color_group as color_group
    from
        {{ ref("cumsum_colors_simplified_by_year") }}
    group by color_group

),
groups as (

    select 
        distinct 
            color_name, 
            color_group,
            group_rgb
    from 
        {{ ref("cumsum_colors_simplified_by_year") }}

),
source_data as (

    select 
        t1.id as color_id,
        t1.name as color_name,
        t1.rgb as color_rgb,
        t1.is_trans as is_trans,
        rank() over(order by t3.group_pieces desc) as color_group_id,
        t2.color_group as color_group_name,
        t2.group_rgb as color_group_rgb    
    from
        {{ source("raw", "colors") }} t1
    left join 
        groups t2
    on
        t1.name = t2.color_name    
    left join 
        weighted_groups t3
    on
        t2.color_group = t3.color_group

)

select * from source_data