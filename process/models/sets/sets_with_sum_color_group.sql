with aux as (

    select
		id,
		sum(quantity) as sum_quantity,
		color_group,
        group_rgb
	from {{ ref("sets_with_parts") }}
    group by 
    	id,
		color_group,
        group_rgb

),
color_groups as (

	select 
		color_group_id,
		color_group_name,
		color_group_rgb
	from 
		{{ ref("color_groups") }}
	group by
		color_group_id,
		color_group_name,
		color_group_rgb

),
source_data as (

	select 
		t1.id as id,
		t1.sum_quantity as sum_quantity,
		t2.color_group_id as color_group_id,
		t1.color_group as color_group_name,
        t1.group_rgb as color_group_rgb
	from 
		aux t1
	left join 
		color_groups t2
	on t1.color_group = t2.color_group_name

)

select * from source_data