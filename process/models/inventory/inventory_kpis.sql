with source_data as (
    select
        t2.id,
        t1.year,
        t1.n_items,
        t2.set_num,
        t2.num_parts
    from 
        {{ ref("inventory_count") }} t1
    left join
        {{ ref("inventory_max") }} t2
    on 
        t1.year = t2.year
)

select * from source_data