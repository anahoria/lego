with source_data as (

	select 
		t1.id as id,
		t1.version as version,
		t1.set_num as set_num,
		case when t2.inventory_id is not null
			then 'kit'
			when t1.set_num like 'fig%'
			then 'minifig'  
			else 'set'
		end as type		
	from  {{ source("raw","inventories") }} t1
	left join {{ source("raw","inventory_sets") }} t2
	on t1.id = t2.inventory_id
	group by t1.id, t1.version, t1.set_num, type
)

select *
from source_data
