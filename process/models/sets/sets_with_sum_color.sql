with source_data as (

    select
		id as id,
		sum(quantity) as sum_quantity,
		color_id as color_id,
        color_name as color_name,
        rgb as color_rgb
	from {{ ref("sets_with_parts") }}
    group by 
    	id,
		color_id,
        color_name,
        rgb

)

select * from source_data