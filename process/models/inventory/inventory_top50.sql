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
    where (
        grandparent_theme_id is null  
            or 
        grandparent_theme_id not in (497, 501, 507, 277, 709)
        )
    and (
        parent_theme_id is null  
            or 
        parent_theme_id not in (497, 501, 507, 277, 709)
        )
    and (
        theme_id is null  
            or 
        theme_id not in (497, 501, 507, 277, 709)
        )
    and
        year is not null
    and 
        lower(name) not like '%mosaic%'
    and 
        version = 1
    order by 
        num_parts 
    desc
    limit 50
)
select * from source_data