with source_data as (

	select 
		t1.id,
		t1.version,
		t1.set_num,
		t2.num_parts
	from 
		{{ ref("inventory_categories") }} t1
	left join 
		{{ source("raw","minifigs") }} t2
	on 
		t1.set_num = t2.fig_num
	where 
		t1.type = 'minifig'

)

select * from source_data
