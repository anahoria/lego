with source_data as (

	select 
		t1.id,
		t1.version,
		t1.year,
		t1.set_num,
		t1.type,
		t1.num_parts,
		t1.name,
        t1.theme_id,
        t1.img_url as item_img_url,
        t2.part_num,
        t2.color_id,
        t2.quantity,
        t2.is_spare,
        t2.img_url as part_img_url,
        t1.theme_name,
        t1.parent_theme_id, 
        t1.parent_theme_name,
        t1.grandparent_theme_id,
        t1.grandparent_theme_name		
	from  {{ ref("inventory_detailed") }} t1
	left join
        {{ source("raw", "inventory_parts") }} t2
    on
        t1.id = t2.inventory_id
)

select *
from source_data
