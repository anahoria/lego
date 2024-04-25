with source_data as (

	select 	
		t1.id as id,
		t1.version as version,
		case when t1.set_num != t4.set_num 
		then t4.set_num
		else t1.set_num end as set_num,
		case when t1.set_num != t4.set_num 
		then t1.set_num
		else null end as set_set_num,
		case when t2.name is null
		then t3.name 
		else t2.name end as name,
		t2.year as year,
		t2.theme_id as theme_id,
		case when t2.num_parts is null
		then t3.num_parts
		when t2.num_parts = 0
		then t5.num_parts
		else t2.num_parts end as num_parts,
		case when t2.img_url is null
		then t3.img_url
		else t2.img_url end as img_url,
		t5.img_url as subset_img_url,
		t4.quantity as quantity_sets 
	from {{ source("raw","inventories") }} t1
	left join {{ source("raw","sets") }} t2
	on t1.set_num = t2.set_num 
	left join {{ source("raw","minifigs") }} t3
	on t1.set_num = t3.fig_num
	left join {{ source("raw", "inventory_sets") }} t4
	on t1.id = t4.inventory_id
	left join {{ source("raw", "sets") }} t5
	on t4.set_num = t5.set_num

)

select *
from source_data
