with source_data as (

    select 
        t3."year" as year,  
        t1.id as id,
        t1.set_num as set_num,
        t3.num_parts as num_parts,
        t2.part_num as part_num,
        t2.quantity as quantity,
        t2.is_spare as is_spare,
        t2.img_url as part_url,
        t3.name as set_name,
        t3.theme_id as theme_id,
        t3.img_url as set_url,
        t2.color_id as color_id,
        t4.name as color_name,
        t4.rgb as rgb,
        t4.is_trans as is_trans,
        t5.id as theme_id,
        t5.name as theme_name,
        t5.parent_id as parent_theme_id,
        t6.name as parent_theme_name,
        t1.version as set_version
    from {{ ref("only_sets_no_minifigs") }} t1
    left join 
        {{ source("raw", "inventory_parts") }} t2
    on 
        t1.id = t2.inventory_id 
    left join 
        {{ source("raw", "sets") }} t3
    on 
        t1.set_num = t3.set_num
    left join 
        {{ source("raw", "colors") }} t4
    on 
        t2.color_id = t4.id 
    left join 
        {{ source("raw", "themes") }} t5
    on 
        t3.theme_id = t4.id 
    left join 
        {{ source("raw", "themes") }} t6
    on t5.parent_id = t6.id
    
)

select *
from source_data

