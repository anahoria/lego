with aux1 as (
    select
	    distinct
	    id,
	    version,
	    year,
	    kit_set_num as set_num,
	    greatest(coalesce(calc_kit_num_parts, 0), coalesce(kit_num_parts, 0)) as num_parts
    from 
        {{ ref("kits_sets") }}
),
aux2 as (
    select
	    distinct
	    id,
	    version,
	    kit_set_num as set_num, 
	    greatest(coalesce(calc_kit_num_parts, 0), coalesce(fig_num_parts, 0)) as num_parts
    from 
        {{ ref("kits_minifigs") }}
),
source_data as (
    select
        t1.id,
        t1.version,
        t1.year,
        t1.set_num,
        t1.num_parts + t2.num_parts as num_parts
    from
        aux1 t1
    left join
        aux2 t2
    on  
        t1.id = t2.id
    and
        t1.version = t2.version
    and 
        t1.set_num = t2.set_num
)

select * from source_data