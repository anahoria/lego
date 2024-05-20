with aux as (

    select
		rank() over(partition by id, sum_quantity order by color_id asc) as first_selected,
		id as id,
		max(sum_quantity) over(partition by id) as max_quantity,
		sum_quantity as sum_quantity,
		color_id as color_id,
		color_name as color_name,
		color_rgb as color_rgb
	from {{ ref("sets_with_sum_color") }}
    group by 
    	id,
		sum_quantity,
    	color_id,
		color_name,
		color_rgb

),
source_data as (

	select
		id as id,
		max_quantity,
		color_id as color_id,
		color_name as color_name,
		color_rgb as color_rgb
	from aux
	where
		max_quantity = sum_quantity
	and
		first_selected = 1
    
)

select * from source_data