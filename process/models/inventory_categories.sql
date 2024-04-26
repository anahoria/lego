with source_data as (

	select 
		id,
		version,
		type,
		kit_num,
		case 
			when type = 'minifig_set'
			then null
			else set_num 
		end as set_num,
		set_qtty_in_kit,
		case 
			when type = 'minifig_set'
			then set_num 
			else null 
		end as fig_num
	from {{ ref("inv_cat_aux") }} 
)

select *
from source_data
