with source_data as (

select 
	year, 
	max(num_sets) as num_sets,
	max(bigger_set) as bigger_set,
	max(set_url) as set_url 
	from (
	SELECT 
		year,
		count(distinct id) over(partition by year) as num_sets ,
		max(num_parts) over(partition by year) as bigger_set,
		num_parts,
		set_url
FROM main.sets_detailed)
where num_parts = bigger_set
group by year

)

select *
from source_data

