with aux as (

	select 
		t1.id as id,
		t1.version as version,
		t5.year as year,
		t1.kit_num as kit_num,
		t5.name as kit_name,
		t5.num_parts as kit_num_parts,
		sum(t2.num_parts) over(partition by t1.kit_num) as calc_kit_num_parts,
		t5.img_url as kit_img_url,
		t2.year as set_year,
		t1.set_num as set_num,
		t2.name as set_name,
		t2.num_parts as set_num_parts,
		t2.img_url as set_img_url,
		t3.id as kit_theme_id,
		t3.name as kit_theme_name,
		t4.id as kit_parent_theme_id,
		t4.name as kit_parent_theme_name
	from {{ ref("inventory_categories") }} t1
	left join {{ source("raw","sets") }} t2
	on t1.set_num = t2.set_num
	left join {{ source("raw","themes") }} t3
	on t2.theme_id = t3.id
	left join {{ source("raw","themes") }} t4
	on t3.parent_id = t4.id
	left join {{ source("raw","sets") }} t5
	on t1.kit_num = t5.set_num
	where t1.type = 'kit'
),
source_data as (
	select 
		t1.id as id,
		t1.version as version,
		t1.year as year,
		t1.kit_num as kit_num,
		t1.kit_name as kit_name,
		t1.kit_num_parts as kit_num_parts,
		greatest(t1.kit_num_parts, t1.calc_kit_num_parts) as kit_num_parts,
		t1.kit_img_url as kit_img_url,
		t1.set_year as set_year,
		t1.set_num as set_num,
		t1.set_name as set_name,
		t1.set_num_parts as set_num_parts,
		t1.set_img_url as set_img_url,
		t1.kit_theme_id as kit_theme_id,
		t1.kit_theme_name as kit_theme_name,
		t1.kit_parent_theme_id as kit_parent_theme_id,
		t1.kit_parent_theme_name as kit_parent_theme_name
	from
		aux t1
)

select * from source_data
