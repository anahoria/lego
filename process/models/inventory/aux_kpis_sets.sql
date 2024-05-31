with aux1 as (

    select
        distinct
        t1.set_year as year, 
        count(distinct t1.set_num) over(partition by t1.set_year) as n_sets,
        max(t1.set_num_parts) over(partition by t1.set_year) as bigger_set,
        t1.set_id as set_id,
        t1.set_num as set_num,
        t1.set_name as set_name,
        t1.set_num_parts as set_num_parts,
        t1.set_theme_id as set_theme_id,
        t1.set_theme_name as set_theme_name,
        t1.set_img_url as set_img_url,
        sum(t1.set_part_quantity) over(partition by t1.set_year, t1.set_id, t1.set_part_color_id) as sum_part_color,
        t1.set_part_color_id as set_part_color_id,
        t1.set_part_color_name as set_part_color_name,
        t1.set_part_rgb as set_part_rgb,
        t2.color_group_id as set_part_color_group_id,
        t1.set_part_color_group as set_part_color_group,
        t1.set_part_group_rgb as set_part_group_rgb,
        t1.set_part_color_is_trans as set_part_color_is_trans,
    from 
        {{ ref("inventory_detailed") }} t1
    left join
        {{ ref("color_groups") }} t2
    on 
        t1.set_part_color_group = t2.color_group
    where 
        t1.type = 'kit'
    and t1.set_part_is_spare = false

),
aux2 as (

    select 
        year as year, 
        n_sets as n_sets,
        bigger_set as bigger_set,
        set_id as set_id,
        set_num as set_num,
        set_name as set_name,
        set_theme_id as set_theme_id,
        set_theme_name as set_theme_name,
        set_img_url as set_img_url,
        sum_part_color as sum_part_color,
        max(sum_part_color) over(partition by year, set_id) as max_color_qtty,
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
        bigger_set = set_num_parts
),
source_data as (
    select
        distinct 
        year as year, 
        n_sets as n_sets,
        bigger_set as bigger_set,
        set_id as set_id,
        set_num as set_num,
        set_name as set_name,
        set_theme_id as set_theme_id,
        set_theme_name as set_theme_name,
        set_img_url as set_img_url,
        max_color_qtty as max_color_qtty,
        set_part_color_id as set_part_color_id,
        set_part_color_name as set_part_color_name,
        set_part_rgb as set_part_rgb,
        set_part_color_group_id as set_part_color_group_id,
        set_part_color_group as set_part_color_group,
        set_part_group_rgb as set_part_group_rgb,
        set_part_color_is_trans as set_part_color_is_trans,
        rank() over(partition by max_color_qtty order by set_part_color_id asc) as aux
    from
        aux2
    where
        sum_part_color = max_color_qtty
)

select * from source_data where aux = 1