with source_data as (

	select
		t1.id as id,
		t1.max_quantity as max_qtty_color,
		t1.color_id as color_id,
		t1.color_name as color_name,
		t1.color_rgb as color_rgb,
		t2.max_quantity as max_qtty_color_group,
		t2.color_group_id as color_group_id,
		t2.color_group_name as color_group_name,
		t2.color_group_rgb as color_group_rgb
	from 
		{{ ref("sets_with_max_color") }} t1
	left join 
		{{ ref("sets_with_max_color_group") }} t2
	on
		t1.id = t2.id
)

select * from source_data