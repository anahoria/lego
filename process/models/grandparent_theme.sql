with source_data as (

    select 
        id,
        name
    from {{ source("raw","themes") }} 
    where parent_id is null

)

select * from source_data