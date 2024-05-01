with source_data as (

    select 
        t1.grandparent_id as grandparent_id,
        t1.grandparent_name as grandparent_name,
        t1.parent_id as parent_id,
        t1.parent_name as parent_name,
        t2.id as child_id,
        t2.name as child_name
    from {{ ref("parent_theme") }} t1
    left join {{ source("raw","themes") }} t2
    on t1.parent_id = t2.parent_id

)

select * from source_data