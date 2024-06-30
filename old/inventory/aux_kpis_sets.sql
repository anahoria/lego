with aux1 as (

    select
        distinct
        t1.set_year as year, 
        count(distinct t1.set_id) over(partition by t1.set_year) as n_sets,
        sum(t1.set_part_quantity) over(partition by t1.set_year, t1.set_id) as big_set_n_parts_1,
        max(t1.set_num_parts) over(partition by t1.set_year) as big_set_n_parts_2,
        t1.set_id as set_id,
        t1.set_num as set_num,
        t1.set_name as set_name,
        t1.set_num_parts as set_num_parts,
        t1.set_theme_id as set_theme_id,
        t1.set_theme_name as set_theme_name,
        t1.set_img_url as set_img_url,
        sum(t1.set_part_quantity) over(partition by t1.set_year, t1.set_id, t1.set_part_color_id) as set_n_color_parts,
        t1.set_part_color_id as set_part_color_id,
        t1.set_part_color_name as set_part_color_name,
        t1.set_part_rgb as set_part_rgb,
        sum(t1.set_part_quantity) over(partition by t1.set_year, t1.set_id, t1.set_part_color_group_name) as set_n_color_group_parts,
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
        t1.type = 'set'
    and t1.set_part_is_spare = false

),
aux2 as (
    select 
        year as year, 
        n_sets as n_sets,
        max(big_set_n_parts_1) over(partition by year) as aux,
        greatest(aux, big_set_n_parts_2) as big_set_n_parts,
        set_id as big_set_id,
        set_num as big_set_num,
        set_name as big_set_name,
        set_theme_id as big_set_theme_id,
        set_theme_name as big_set_theme_name,
        set_img_url as big_set_img_url,
        set_n_color_parts as big_set_n_color_parts,
        max(set_n_color_parts) over(partition by year, set_id) as big_set_max_n_color_parts,
        set_part_color_id as set_part_color_id,
        set_part_color_name as set_part_color_name,
        set_part_rgb as set_part_rgb,
        max(set_n_color_group_parts) over(partition by year, set_id) as big_set_max_n_color_group_parts,
        set_part_color_group_name as set_part_color_group_name,
        set_part_color_group_rgb as set_part_color_group_rgb,
        set_part_color_is_trans as set_part_color_is_trans
    from
        aux1
    where
        big_set_n_parts = coalesce(set_num_parts,big_set_n_parts)
),
source_data as (
    select
        distinct 
        year as year, 
        n_sets as n_sets,
        big_set_n_parts as big_set_n_parts,
        big_set_id as big_set_id,
        big_set_num as big_set_num,
        big_set_name as big_set_name,
        big_set_theme_id as big_set_theme_id,
        big_set_theme_name as big_set_theme_name,
        big_set_img_url as big_set_img_url,
        big_set_max_n_color_parts as big_set_max_n_color_parts,
        big_set_max_n_color_group_parts as big_set_max_n_color_group_parts,
        set_part_color_id as set_part_color_id,
        set_part_color_name as set_part_color_name,
        set_part_rgb as set_part_rgb,
        set_part_color_group_name as set_part_color_group_name,
        set_part_color_group_rgb as set_part_color_group_rgb,
        set_part_color_is_trans as set_part_color_is_trans,
        rank() over(partition by set_part_color_id order by big_set_max_n_color_parts desc) as aux
    from
        aux2
    where
        big_set_n_color_parts = big_set_max_n_color_parts
)

select * from source_data 