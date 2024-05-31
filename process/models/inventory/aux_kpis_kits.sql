with aux1 as (

    select
        distinct
        t1.kit_year as year, 
        count(distinct t1.kit_num) over(partition by t1.kit_year) as n_kits,
        max(t1.kit_num_parts) over(partition by t1.kit_year) as bigger_kit,
        t1.kit_id as kit_id,
        t1.kit_num as kit_num,
        t1.kit_name as kit_name,
        t1.kit_num_parts as kit_num_parts,
        t1.kit_theme_id as kit_theme_id,
        t1.kit_theme_name as kit_theme_name,
        t1.kit_img_url as kit_img_url,
        sum(t1.set_qtty_in_kit*t1.set_part_quantity) over(partition by t1.kit_year, t1.kit_id, t1.set_part_color_id) as sum_part_color,
        t1.set_part_color_id as set_part_color_id,
        t1.set_part_color_name as set_part_color_name,
        t1.set_part_rgb as set_part_rgb,
        t2.color_group_id as set_part_color_group_id,
        t1.set_part_color_group as set_part_color_group,
        t1.set_part_group_rgb as set_part_group_rgb,
        t1.set_part_color_is_trans as set_part_color_is_trans,
        t1.set_part_img_url as set_part_img_url
    from 
        {{ ref("inventory_detailed") }} t1
    left join 
        {{ ref("color_groups") }} t2
    on 
        t1.set_part_color_group = t2.color_group
    where 
        type = 'kit'
    and set_part_is_spare = false

),
aux2 as (

    select 
        year as year, 
        n_kits as n_kits,
        bigger_kit as bigger_kit,
        kit_id as kit_id,
        kit_num as kit_num,
        kit_name as kit_name,
        kit_theme_id as kit_theme_id,
        kit_theme_name as kit_theme_name,
        kit_img_url as kit_img_url,
        sum_part_color as sum_part_color,
        max(sum_part_color) over(partition by year, kit_id) as max_color_qtty,
        set_part_color_id as set_part_color_id,
        set_part_color_name as set_part_color_name,
        set_part_rgb as set_part_rgb,
        set_part_color_group_id as set_part_color_group_id,
        set_part_color_group as set_part_color_group,
        set_part_group_rgb as set_part_group_rgb,
        set_part_color_is_trans as set_part_color_is_trans
    from
        aux1
    where
        bigger_kit = kit_num_parts
),
source_data as (
    select
        distinct 
        year as year, 
        n_kits as n_kits,
        bigger_kit as bigger_kit,
        kit_id as kit_id,
        kit_num as kit_num,
        kit_name as kit_name,
        kit_theme_id as kit_theme_id,
        kit_theme_name as kit_theme_name,
        kit_img_url as kit_img_url,
        max_color_qtty as max_color_qtty,
        set_part_color_id as set_part_color_id,
        set_part_color_name as set_part_color_name,
        set_part_rgb as set_part_rgb,
        set_part_color_group_id as set_part_color_group_id,
        set_part_color_group as set_part_color_group,
        set_part_group_rgb as set_part_group_rgb,
        set_part_color_is_trans as set_part_color_is_trans,
    from
        aux2
    where
        sum_part_color = max_color_qtty
)

select * from source_data