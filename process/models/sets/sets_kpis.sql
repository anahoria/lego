with source_data as (

	select 
	year,
	version,
	max(version) over(partition by set_num) as max_version,
	count(distinct id ) over(partition by year) as n_sets_year, 
	max(num_parts) over(partition by year) as bigger_set,
	count(distinct id ) over(partition by year, theme_name) as n_sets_year_theme, 
	count(distinct id ) over(partition by year, parent_theme_name) as n_sets_year_parent,
	case when theme_name is null
	then 'unknown'
	else theme_name end as theme_name,
	case when parent_theme_name is null and theme_name not null
	then theme_name
	when parent_theme_name is null and theme_name is null
	then 'unknown'
	else parent_theme_name end as parent_theme_name,
	img_url,
	num_parts
from {{ ref("sets_detailed") }}
)

select *
from source_data
