with source_data as (

    select 
	    t1.year as year,
	    t1.color_name as color_name,
		t1.rgb as color_rgb,
	    t2.color_group as color_group,
	    t2.group_rgb as group_rgb,
	    t1.n_pieces as n_pieces  
	from 
        {{ ref("cumsum_colors_by_year") }} t1
	left join 
        {{ source("main", "color_simplified") }} t2
	on
		t1.color_name = t2.color_name

)

select * from source_data