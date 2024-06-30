with weighted_groups as (

    select 
        sum(set_qtty_in_kit*set_part_quantity) as group_pieces,
        set_part_color_group_name as color_group_name,
        set_part_color_group_rgb as color_group_rgb
    from
        {{ ref("inventory_detailed") }}
    group by color_group_name, color_group_rgb

),
source_data as (

    select 
        rank() over(order by group_pieces desc) as color_group_id,
        color_group_name as color_group_name,
        color_group_rgb as color_group_rgb
    from
        weighted_groups

)

select * from source_data