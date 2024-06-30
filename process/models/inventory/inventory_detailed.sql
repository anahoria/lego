with source_data as (

	select 
		t1.id,
		t1.version,
		t1.year,
		t1.set_num,
		t1.type,
		t1.num_parts,
		t2.name,
        t2.theme_id,
        t2.img_url,
        t3.name as theme_name,
        t3.parent_id as parent_theme_id, 
        t4.name as parent_theme_name,
        t4.parent_id as grandparent_theme_id,
        t5.name as grandparent_theme_name		
	from  {{ ref("inventory_nums") }} t1
	left join
        {{ source("raw", "sets") }} t2
    on
        t1.set_num = t2.set_num
    and
        t1.year = t2.year
    left join
        {{ source("raw", "themes") }} t3
    on
        t2.theme_id = t3.id
    left join
        {{ source("raw", "themes") }} t4
    on
        t3.parent_id = t4.id
    left join
        {{ source("raw", "themes") }} t5
    on
        t4.parent_id = t5.id
)

select *
from source_data
