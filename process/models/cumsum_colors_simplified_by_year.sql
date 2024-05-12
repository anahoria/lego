with source_data as (

    select 
	    a.year as year,
	    a.color_name as color_name,
	    b.color_group as color_group,
	    b.group_rgb as group_rgb,
	    a.n_pieces as n_pieces  
	from 
        {{ ref("cumsum_colors_by_year") }} a
	left join 
        {{ source("main", "color_treemap_simplified") }} b
	on
		a.color_name = b.color_name

)

select * from source_data