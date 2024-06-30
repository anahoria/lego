with source_data as (

	select 
		t1.id as id,
		t1.version as version,
		t1.set_num as set_num,
		t2.name as set_name,
		t2.year as set_year,
		t2.theme_id as set_theme_id,
		t3.name as set_theme_name,
		t2.num_parts as set_num_parts,
		t2.img_url as set_img_url
	from {{ ref("inventory_categories") }} t1
	left join {{ source("raw","sets") }} t2
	on t1.set_num = t2.set_num
	left join {{ source("raw","themes") }} t3
	on t2.theme_id = t3.id
	where t1.type = 'set'
)

select *
from source_data
