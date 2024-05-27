with aux as (

	select 
		t1.id as id,
		t1.version as version,
		t1.set_num as kit_num,
		t3.name as kit_name,
		t3.year as kit_year,
		t3.theme_id as kit_theme_id,
		t5.name as kit_theme_name,
		t3.num_parts as kit_num_parts,
		sum(t2.quantity * t4.num_parts) over(partition by t1.set_num) as calc_kit_num_parts,
		t3.img_url as kit_img_url,
		t2.set_num as set_num,
		t2.quantity as set_qtty_in_kit,
		t4.name as set_name,
		t4.year as set_year,
		t4.theme_id as set_theme_id,
		t6.name as set_theme_name,
		t4.num_parts as set_num_parts,
		t4.img_url as set_img_url
	from {{ ref("inventory_categories") }} t1
	left join {{ source("raw","inventory_sets") }} t2
	on t1.id = t2.inventory_id
	left join {{ source("raw","sets") }} t3
	on t1.set_num = t3.set_num
	left join {{ source("raw","sets") }} t4
	on t2.set_num = t4.set_num
	left join {{ source("raw","themes") }} t5
	on t3.theme_id = t5.id
	left join {{ source("raw","themes") }} t6
	on t4.theme_id = t6.id
	where t1.type = 'kit'

),
source_data as (

	select 
		id as id,
		version as version,
		kit_num as kit_num,
		kit_name as kit_name,
		kit_year as kit_year,
		kit_theme_id as kit_theme_id,
		kit_theme_name as kit_theme_name,
		greatest(kit_num_parts, calc_kit_num_parts) as kit_num_parts,
		kit_img_url as kit_img_url,
		set_num as set_num,
		set_qtty_in_kit as set_qtty_in_kit,
		set_name as set_name,
		set_year as set_year,
		set_theme_id as set_theme_id,
		set_theme_name as set_theme_name,
		set_num_parts as set_num_parts,
		set_img_url as set_img_url
	from aux

)

select * from source_data
