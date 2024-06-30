with source_data as (

	select 
		t1.id,
		t1.version,
		t1.set_num as kit_set_num,
		sum(t2.quantity * t3.num_parts) over(partition by t1.id) as calc_kit_num_parts,
		t2.quantity as fig_qtty_in_kit,
		t2.fig_num,
		t3.num_parts as fig_num_parts
	from 
		{{ ref("inventory_categories") }} t1
	left join 
		{{ source("raw","inventory_minifigs") }} t2
	on 
		t1.id = t2.inventory_id
	left join 
		{{ source("raw","minifigs") }} t3
	on 
		t2.fig_num = t3.fig_num
	where
		t1.type = 'kit'

)

select * from source_data
