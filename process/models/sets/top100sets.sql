with source_data as (

    select
        t1.id as id,
		t1.version as version,
		t1.year as year,
		t1.set_num as set_num,
		t1.name as name,
		t1.id as theme_id,
		t1.name as theme_name,
		t1.id as parent_theme_id,
		t1.name parent_theme_name,
		t1.num_parts as num_parts,
		t1.img_url as img_url,
        t2.max_qtty_color as max_qtty_color,
		t2.color_id as color_id,
		t2.color_name as color_name,
		t2.color_rgb as color_rgb,
		t2.max_qtty_color_group as max_qtty_color_group,
		t2.color_group_id as color_group_id,
		t2.color_group_name as color_group_name,
		t2.color_group_rgb as color_group_rgb
	from 
        {{ ref("sets_detailed") }} t1
    left join 
        {{ ref("sets_with_max_color_and_color_group") }} t2
    on
        t1.id = t2.id
    order by num_parts desc
    limit 100

)
select * from source_data