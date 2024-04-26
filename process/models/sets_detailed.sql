with source_data as (

	select 
		t1.id as id,
		t1.version as version,
		t2.year as year,
		t1.set_num as set_num,
		t2.name as name,
		t3.name as theme_name,
		case when t4.name is null
		then t3.name 
		else t4.name end as parent_theme_name,
		t2.num_parts as num_parts,
		t2.img_url as img_url
	from {{ ref("inventory_categories") }} t1
	left join {{ source("raw","sets") }} t2
	on t1.set_num = t2.set_num
	left join {{ source("raw","themes") }} t3
	on t2.theme_id = t3.id
	left join {{ source("raw","themes") }} t4
	on t3.parent_id = t4.id
	where t1.type = 'set'
)

select *
from source_data
