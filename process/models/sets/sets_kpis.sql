with source_data as (

	select 
		year as year,
		count(distinct id ) over(partition by year) as n_sets_year, 
		max(set_num_parts) over(partition by year) as max_set_mun_parts,
		count(distinct id) over(partition by year, set_theme_name) as n_sets_year_theme, 
		count(distinct id) over(partition by year, set_parent_theme_name) as n_sets_year_parent,
		set_theme_id,
		set_theme_name,
		set_parent_theme_id,
		set_parent_theme_name,
		set_img_url,
		set_num_parts
	from 
		{{ ref("sets_detailed") }}
)

select *
from source_data
