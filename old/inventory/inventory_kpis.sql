with source_data as (
    select  
        coalesce(t1.year, t2.year) as year, 
        t1.n_kits+t2.n_sets as n_items,
        greatest(t1.big_kit_n_parts, t2.big_set_n_parts) as big_item_n_parts,
        big_kit_n_parts,
        big_set_n_parts, 
        t1.big_kit_id as big_kit_id,
        t1.big_kit_set_num as big_kit_set_num,
        t1.big_kit_name as big_kit_name,
        t1.big_kit_theme_id as big_kit_theme_id,
        t1.big_kit_theme_name as big_kit_theme_name,
        t1.big_kit_img_url as big_kit_img_url,
        t1.big_kit_max_n_color_parts as big_kit_max_n_color_parts,
        t1.set_part_color_id as big_kit_predominant_color_id,
        t1.set_part_color_name as big_kit_predominant_color_name,
        t1.set_part_rgb as big_kit_predominant_color_rgb,
        t1.set_part_color_is_trans as big_kit_predominant_color_is_trans,
        t1.big_kit_max_n_color_group_parts as big_kit_max_n_color_group_parts,
        t1.set_part_color_group_name as big_kit_predominant_color_group_name,
        t1.set_part_color_group_rgb as big_kit_predominant_color_group_rgb,
        t2.big_set_id as big_set_id,
        t2.big_set_num as big_set_num,
        t2.big_set_name as big_set_name,
        t2.big_set_theme_id as big_set_theme_id,
        t2.big_set_theme_name as big_set_theme_name,
        t2.big_set_img_url as big_set_img_url,
        t2.big_set_max_n_color_parts as big_set_max_n_color_parts,
        t2.set_part_color_id as big_set_predominant_color_id,
        t2.set_part_color_name as big_set_predominant_color_name,
        t2.set_part_rgb as big_set_predominant_color_rgb,
        t2.set_part_color_is_trans as big_set_predominant_color_is_trans,
        t2.big_set_max_n_color_group_parts as big_set_max_n_color_group_parts,
        t2.set_part_color_group_name as big_set_predominant_color_group_name,
        t2.set_part_color_group_rgb as big_set_predominant_color_group_rgb
    from
        {{ ref("aux_kpis_kits") }} t1
    full outer join 
        {{ ref("aux_kpis_sets") }} t2
    on
        t1.year = t2.year
)
select * from source_data