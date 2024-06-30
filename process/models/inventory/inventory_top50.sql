with source_data as (
    select
        id,
		version,
		year,
		set_num,
		type,
		num_parts,
		name,
        img_url,
        theme_id,
        theme_name,
        parent_theme_id, 
        parent_theme_name
    from 
        {{ ref("inventory_detailed") }}
    where
        theme_id != 507
    and 
        theme_id != 509
    and 
        theme_id != 398
    and 
        version = 1
    order by 
        num_parts 
    desc
    limit 50
)
select * from source_data