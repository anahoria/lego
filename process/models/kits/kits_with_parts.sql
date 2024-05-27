with source_data as (
    select 
		t1.id as id,
		t1.version as version,
		t1.kit_num as kit_num,
		t1.kit_name as kit_name,
		t1.kit_year as kit_year,
		t1.kit_theme_id as kit_theme_id,
		t1.kit_theme_name as kit_theme_name,
		t1.kit_num_parts as kit_num_parts,
		t1.kit_img_url as kit_img_url,
		t1.set_num as set_num,
		t1.set_qtty_in_kit as set_qtty_in_kit,
		t1.set_name as set_name,
		t1.set_year as set_year,
		t1.set_theme_id as set_theme_id,
		t1.set_theme_name as set_theme_name,
		t1.set_num_parts as set_num_parts,
		t1.set_img_url as set_img_url,
        t3.part_num as part_num,
        t3.color_id as color_id,
        t4.name as color_name,
        t4.rgb as color_rgb,
        t4.is_trans as is_trans,
        t3.quantity as quantity,
        t3.is_spare as is_spare,
        t3.img_url as part_img_url
    from {{ ref("kits_detailed") }} t1
    left join {{ source("raw", "inventories")}} t2
    on t1.set_num = t2.set_num
    left join {{ source("raw","inventory_parts") }} t3
    on t2.id = t3.inventory_id
    left join {{ source("raw","colors") }} t4
    on t3.color_id = t4.id
) 

select * from source_data