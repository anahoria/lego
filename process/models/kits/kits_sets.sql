with source_data as (

	select 
		t1.id,
		t1.version,
		t3.year,
		t1.set_num as kit_set_num,
		t3.num_parts as kit_num_parts,
		sum(t2.quantity * t4.num_parts) over(partition by t1.id) as calc_kit_num_parts,
		t2.quantity as set_qtty_in_kit,
		t2.set_num,
		t4.num_parts as set_num_parts
	from 
		{{ ref("inventory_categories") }} t1
	left join 
		{{ source("raw","inventory_sets") }} t2
	on 
		t1.id = t2.inventory_id
	left join 
		{{ source("raw","sets") }} t3
	on 
		t1.set_num = t3.set_num
	left join 
		{{ source("raw","sets") }} t4
	on 
		t2.set_num = t4.set_num
	where 
		t1.type = 'kit'

)

select * from source_data
