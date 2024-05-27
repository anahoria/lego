with source_data as (

	select 
		set_year as set_year,
		count(distinct id) as n_sets_year, 
		max(set_num_parts) as max_set_num_parts
	from 
		{{ ref("sets_detailed") }}
	group by set_year
)

select * from source_data
