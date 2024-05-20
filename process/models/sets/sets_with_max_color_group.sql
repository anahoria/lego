with aux as (

    select
		rank() over(partition by id, sum_quantity order by color_group_id asc) as first_selected,
		id as id,
		max(sum_quantity) over(partition by id) as max_quantity,
		sum_quantity as sum_quantity,
		color_group_id as color_group_id,
		color_group_name as color_group_name,
		color_group_rgb as color_group_rgb
	from {{ ref("sets_with_sum_color_group") }}
    group by 
    	id,
		sum_quantity,
    	color_group_id,
		color_group_name,
		color_group_rgb

),
source_data as (

	select
		id as id,
		max_quantity,
		color_group_id as color_group_id,
		color_group_name as color_group_name,
		color_group_rgb as color_group_rgb
	from aux
	where
		max_quantity = sum_quantity
	and
		first_selected = 1
    
)

select * from source_data