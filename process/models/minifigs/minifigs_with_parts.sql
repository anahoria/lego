with source_data as (

	select 
		t1.id as id,
		t1.version as version,
		t1.fig_num as fig_num,
		t1.fig_name as fig_name,
		t1.fig_num_parts as fig_num_parts,
		t2.img_url as fig_img_url,
		t2.part_num as part_num,
        t2.color_id as color_id,
        t3.name as color_name,
        t3.rgb as rgb,
        t3.is_trans as is_trans,
        t2.quantity as quantity,
        t2.is_spare as is_spare,
        t2.img_url as part_img_url
	from {{ ref("minifigs_detailed") }} t1
	left join {{ source("raw", "inventory_parts") }} t2
	on t1.id = t2.inventory_id
	left join {{ source("raw","colors") }} t3
    on t2.color_id = t3.id
)

select * from source_data
