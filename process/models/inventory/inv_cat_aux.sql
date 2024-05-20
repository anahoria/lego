with source_data as (

	select 
		id,
		version,
		case when t2.inventory_id is not null
		then 'kit'
		when t1.set_num like 'fig%'
		then 'minifig'  
		else 'set' end as type,
		case when t2.inventory_id is not null
		then t1.set_num
		else null end as kit_num,
		case when t2.inventory_id is not null
		then t2.set_num 
		else t1.set_num end as set_num,
		t2.quantity as set_qtty_in_kit
	from  {{ source("raw","inventories") }} t1
	left join {{ source("raw","inventory_sets") }} t2
	on t1.id = t2.inventory_id
)

select *
from source_data
