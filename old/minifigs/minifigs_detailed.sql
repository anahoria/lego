with source_data as (

	select 
		t1.id as id,
		t1.version as version,
		t1.set_num as fig_num,
		t2.name as fig_name,
		t2.num_parts as fig_num_parts,
		t2.img_url as fig_img_url
	from {{ ref("inventory_categories") }} t1
	left join {{ source("raw", "minifigs") }} t2
	on t1.set_num = t2.fig_num
	where t1.type = 'minifig'
)

select * from source_data
