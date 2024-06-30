with aux1 as (

    select
        distinct
        t1.kit_year as year, 
        count(distinct t1.kit_id) over(partition by t1.kit_year) as n_kits,
        sum(t1.set_qtty_in_kit*t1.set_part_quantity) over(partition by t1.kit_year, t1.kit_id) as big_kit_n_parts_1, 
        max(t1.kit_num_parts) over(partition by t1.kit_year) as big_kit_n_parts_2,
        t1.kit_id as kit_id,
        t1.kit_set_num as kit_set_num,
        t1.kit_name as kit_name,
        t1.kit_num_parts as kit_num_parts,
        t1.kit_theme_id as kit_theme_id,
        t1.kit_theme_name as kit_theme_name,
        t1.kit_img_url as kit_img_url,
        sum(t1.set_qtty_in_kit*t1.set_part_quantity) over(partition by t1.kit_year, t1.kit_id, t1.set_part_color_id) as kit_n_color_parts,
        t1.set_part_color_id as set_part_color_id,
        t1.set_part_color_name as set_part_color_name,
        t1.set_part_rgb as set_part_rgb,
        sum(t1.set_qtty_in_kit*t1.set_part_quantity) over(partition by t1.kit_year, t1.kit_id, t1.set_part_color_group_name) as kit_n_color_group_parts,
        t2.color_group_id as set_part_color_group_id,
        t1.set_part_color_group_name as set_part_color_group_name,
        t1.set_part_color_group_rgb as set_part_color_group_rgb,
        t1.set_part_color_is_trans as set_part_color_is_trans
    from 
        {{ ref("inventory_detailed") }} t1
    left join 
        {{ ref("color_groups") }} t2
    on 
        t1.set_part_color_group_name = t2.color_group_name
    where 
        t1.type = 'kit'
    and 
        t1.set_part_is_spare = false

),
aux2 as (

    select 
        year as year, 
        n_kits as n_kits,
        greatest(big_kit_n_parts_1, big_kit_n_parts_2) as big_kit_n_parts,
        kit_id as big_kit_id,
        kit_set_num as big_kit_set_num,
        kit_name as big_kit_name,
        kit_theme_id as big_kit_theme_id,
        kit_theme_name as big_kit_theme_name,
        kit_img_url as big_kit_img_url,
        kit_n_color_parts as big_kit_n_color_parts,
        max(kit_n_color_parts) over(partition by year, kit_id) as big_kit_max_n_color_parts,
        set_part_color_id as set_part_color_id,
        set_part_color_name as set_part_color_name,
        set_part_rgb as set_part_rgb,
        max(kit_n_color_group_parts) over(partition by year, kit_id) as big_kit_max_n_color_group_parts,
        set_part_color_group_id as set_part_color_group_id,
        set_part_color_group_name as set_part_color_group_name,
        set_part_color_group_rgb as set_part_color_group_rgb,
        set_part_color_is_trans as set_part_color_is_trans
    from
        aux1
    where
        big_kit_n_parts = kit_num_parts
),
source_data as (
    select
        distinct 
        year as year, 
        n_kits as n_kits,
        big_kit_n_parts as big_kit_n_parts,
        big_kit_id as big_kit_id,
        big_kit_set_num as big_kit_set_num,
        big_kit_name as big_kit_name,
        big_kit_theme_id as big_kit_theme_id,
        big_kit_theme_name as big_kit_theme_name,
        big_kit_img_url as big_kit_img_url,
        big_kit_max_n_color_parts as big_kit_max_n_color_parts,
        set_part_color_id as set_part_color_id,
        set_part_color_name as set_part_color_name,
        set_part_rgb as set_part_rgb,
        big_kit_max_n_color_group_parts as big_kit_max_n_color_group_parts,
        set_part_color_group_id as set_part_color_group_id,
        set_part_color_group_name as set_part_color_group_name,
        set_part_color_group_rgb as set_part_color_group_rgb,
        set_part_color_is_trans as set_part_color_is_trans,
        rank() over(partition by big_kit_max_n_color_parts order by set_part_color_id asc) as aux
    from
        aux2
    where
        big_kit_n_color_parts = big_kit_max_n_color_parts
)

select * from source_data where aux = 1