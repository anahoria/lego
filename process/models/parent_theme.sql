with source_data as (

    select 
        t1.id as grandparent_id,
        t1.name as grandparent_name,
        t2.id as parent_id,
        t2.name as parent_name
    from {{ ref("grandparent_theme") }} t1
    left join {{ source("raw","themes") }} t2
        on t1.id = t2.parent_id

)

select * from source_data