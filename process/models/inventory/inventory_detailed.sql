with aux1 as (

	select
		'kit' as type, 
		id as id,
		version as version,
		kit_num as kit_num,
		kit_name as kit_name,
		kit_year as kit_year,
		kit_theme_id as kit_theme_id,
		kit_theme_name as kit_theme_name,
		kit_num_parts as kit_num_parts,
		kit_img_url as kit_img_url,
		set_num as set_num,
		set_qtty_in_kit as set_qtty_in_kit,
		set_name as set_name,
		set_year as set_year,
		set_theme_id as set_theme_id,
		set_theme_name as set_theme_name,
		set_num_parts as set_num_parts,
		set_img_url as set_img_url,
		null as fig_num,
		null as fig_name,
		null as fig_num_parts,
		null as fig_img_url
	from 
		{{ ref("kits_detailed") }}
	union all
	select
		'set' as type,
		id as id,
		version as version,
		null as kit_num,
		null as kit_name,
		null as kit_year,
		null as kit_theme_id,
		null as kit_theme_name,
		null as kit_num_parts,
		null as kit_img_url,
		set_num as set_num,
		null as set_qtty_in_kit,
		set_name as set_name,
		set_year as set_year,
		set_theme_id as set_theme_id,
		set_theme_name as set_theme_name,
		set_num_parts as set_num_parts,
		set_img_url as set_img_url,
		null as fig_num,
		null as fig_name,
		null as fig_num_parts,
		null as fig_img_url
	from
		{{ ref("sets_detailed") }}
	union all
	select
		'minifig' as type,
		id as id,
		version as version,
		null as kit_num,
		null as kit_name,
		null as kit_year,
		null as kit_theme_id,
		null as kit_theme_name,
		null as kit_num_parts,
		null as kit_img_url,
		null as set_num,
		null as set_qtty_in_kit,
		null as set_name,
		null as set_year,
		null as set_theme_id,
		null as set_theme_name,
		null as set_num_parts,
		null as set_img_url,
		fig_num as fig_num,
		fig_name as fig_name,
		fig_num_parts as fig_num_parts,
		fig_img_url as fig_img_url
	from
		{{ ref("minifigs_detailed") }}
),
aux2 as (
	
	select
		t1.type as type,
		case when t1.type = 'kit'
			then t1.id 
			else null 
		end as kit_id,
		case when t1.type = 'kit'
			then t1.version 
			else null 
		end as kit_version,
		t1.kit_num as kit_num,
		t1.kit_name as kit_name,
		t1.kit_year as kit_year,
		t1.kit_theme_id as kit_theme_id,
		t1.kit_theme_name as kit_theme_name,
		t1.kit_num_parts as kit_num_parts,
		t1.kit_img_url as kit_img_url,
		case when t1.type = 'kit'
			then t2.id
			else t1.id
		end as set_id,
		case when t1.type = 'kit'
			then t2.version
			else t1.version
		end as set_version,
		t1.set_num as set_num,
		t1.set_qtty_in_kit as set_qtty_in_kit,
		t1.set_name as set_name,
		t1.set_year as set_year,
		t1.set_theme_id as set_theme_id,
		t1.set_theme_name as set_theme_name,
		t1.set_num_parts as set_num_parts,
		t1.set_img_url as set_img_url
	from 
		aux1 t1
	left join 
		{{ source("raw", "inventories") }} t2
	on	t1.set_num = t2.set_num
	
),
source_data as (

	select
		t1.type as type,
		t1.kit_id as kit_id,
		t1.kit_version as kit_version,
		t1.kit_num as kit_num,
		t1.kit_name as kit_name,
		t1.kit_year as kit_year,
		t1.kit_theme_id as kit_theme_id,
		t1.kit_theme_name as kit_theme_name,
		t1.kit_num_parts as kit_num_parts,
		t1.kit_img_url as kit_img_url,
		t1.set_id as set_id,
		t1.set_version as set_version,
		t1.set_num as set_num,
		t1.set_qtty_in_kit as set_qtty_in_kit,
		t1.set_name as set_name,
		t1.set_year as set_year,
		t1.set_theme_id as set_theme_id,
		t1.set_theme_name as set_theme_name,
		t1.set_num_parts as set_num_parts,
		t1.set_img_url as set_img_url,
		t2.part_num as set_part_num,
		t2.color_id as set_part_color_id,
		t3.color_name as set_part_color_name,
		t3.rgb as set_part_rgb,
		t3.color_group as set_part_color_group,
		t3.group_rgb as set_part_group_rgb,
		t3.is_trans as set_part_color_is_trans,
		t2.quantity as set_part_quantity,
		t2.is_spare as set_part_is_spare,
		t2.img_url as set_part_img_url
	from
		aux2 t1
	left join {{ source("raw", "inventory_parts") }} t2
		on t1.set_id = t2.inventory_id
	left join {{ source("main", "color_simplified") }} t3
		on t2.color_id = t3.id
)

select * from source_data
