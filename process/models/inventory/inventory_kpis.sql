with source_data as (
    select
        'kit' as type,  
        year as year, 
        n_kits as n_items,
        bigger_kit as bigger_item,
        kit_id as item_id,
        kit_num as item_num,
        kit_name as item_name,
        kit_theme_id as item_theme_id,
        kit_theme_name as item_theme_name,
        kit_img_url as item_img_url,
        max_color_qtty as max_color_qtty,
        set_part_color_id as item_part_color_id,
        set_part_color_name as item_part_color_name,
        set_part_rgb as item_part_rgb,
        set_part_color_group_id as item_part_color_group_id,
        set_part_color_group as item_part_color_group,
        set_part_group_rgb as item_part_group_rgb,
        set_part_color_is_trans as item_part_color_is_trans,
    from
        aux_kpis_kits
    union all
    select
        'set' as type,
        year as year, 
        n_sets as n_items,
        bigger_set as bigger_item,
        set_id as item_id,
        set_num as item_num,
        set_name as item_name,
        set_theme_id as item_theme_id,
        set_theme_name as item_theme_name,
        set_img_url as item_img_url,
        max_color_qtty as max_color_qtty,
        set_part_color_id as item_color_id,
        set_part_color_name as item_color_name,
        set_part_rgb as item_part_rgb,
        set_part_color_group_id as item_part_color_group_id,
        set_part_color_group as item_part_color_group,
        set_part_group_rgb as item_part_group_rgb,
        set_part_color_is_trans as item_part_color_is_trans,
    from
        aux_kpis_sets
)

select * from source_data