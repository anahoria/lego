with source_data as (
    select 
        t1.id as id,
		t1.version as version,
		t1.year as year,
		t1.set_num as set_num,
		t1.name as name,
        t1.theme_id as theme_id,
		t1.theme_name as theme_name,
        t1.parent_theme_id as parent_theme_id,
        t1.parent_theme_name,
		t1.num_parts as num_parts,
		t1.img_url as img_url,
        t2.part_num as part_num,
        t2.quantity as quantity,
        t2.color_id as color_id,
        t3.name as color_name,
        t3.rgb as rgb,
        t3.is_trans as is_trans,
        t4.color_group as color_group,
        t4.group_rgb as group_rgb,
        t2.is_spare as is_spare,
        t2.img_url as part_url
    from {{ ref("sets_detailed") }} t1
    left join {{ source("raw","inventory_parts") }} t2
    on t1.id = t2.inventory_id
    left join {{ source("raw","colors") }} t3
    on t2.color_id = t3.id
    left join {{ source("main","color_treemap_simplified") }} t4
    on t2.color_id = t4.id
) 

select * from source_data