with weighted_groups as (

    select 
        sum(set_qtty_in_kit*set_part_quantity) as group_pieces,
        set_part_color_group as color_group
    from
        {{ ref("inventory_detailed") }}
    group by color_group

),
source_data as (

    select 
        rank() over(order by group_pieces desc) as color_group_id,
        color_group as color_group 
    from
        weighted_groups

)

select * from source_data