with source_data as (
    select 
	    a.id as id,
	    a.name as color_name,
	    a.rgb as rgb,
	    a.color_group as color_group,
	    b.rgb as group_rgb,
	    a.is_trans as is_trans
	from (
        select 
	        id as id,
	        name as name,
	        rgb as rgb,
	        case when upper(regexp_extract(name, '\b(\w+)$', 1))
	                not in ('OPAQUE','METAL', 'CLEAR','TRANS', 'LENS', 'TITANIUM')
	            then regexp_extract(name, '\b(\w+)$', 1)
	            else name
	        end as color_group,
	        is_trans as is_trans
        from {{ source("raw","colors") }}) a
    left join raw.colors b
    on a.color_group = b.name

)

select * from source_data