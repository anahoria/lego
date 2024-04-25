with source_data as (

    select *
    from 
	    {{ source("raw", "inventories") }} 
    where id in (select inventory_id from {{ source("raw", "inventory_parts") }})
    and id not in (select inventory_id from {{ source("raw", "inventory_minifigs") }})
    and id not in (select inventory_id from {{ source("raw", "inventory_sets") }})
    and set_num not like 'fig%'
)

select *
from source_data

