with source_data as (

	select 
		year,
		count(distinct id) over(partition by year) as n_kits_year, 
		max(kit_num_parts) over(partition by year) as max_kit_mun_parts,
		count(distinct id) over(partition by year, kit_theme_name) as n_kits_year_theme, 
		count(distinct id) over(partition by year, kit_parent_theme_name) as n_kits_year_parent_theme,
		kit_theme_id,
		kit_theme_name,
		kit_parent_theme_id,
		kit_parent_theme_name 
	from 
		{{ ref("kits_detailed") }}

)

select * from source_data
