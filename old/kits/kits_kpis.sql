with source_data as (

	select 
		kit_year as kit_year,
		count(distinct id) as n_kits_year, 
		max(kit_num_parts) as max_kit_mun_parts
	from 
		{{ ref("kits_detailed") }}
	group by kit_year

)

select * from source_data
