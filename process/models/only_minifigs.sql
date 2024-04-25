with source_data as (

    select *
    from 
	    {{ source("raw", "inventories") }} 
    where id in (select inventory_id from {{ source("raw", "inventory_parts") }})
    and id not in (select inventory_id from {{ source("raw", "inventory_minifigs") }})

)

select *
from source_data

