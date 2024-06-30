with source_data as (

	select 
		t1.id,
		t1.version,
		t2.year,
		t1.set_num,
		t2.num_parts
	from 
		{{ ref("inventory_categories") }} t1
	left join 
		{{ source("raw","sets") }} t2
	on 
		t1.set_num = t2.set_num
	where 
		t1.type = 'set'

)

select * from source_data
