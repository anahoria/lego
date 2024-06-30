with aux1 as (
    select 
        t1.id,
        t1.version,
        t1.year,
        t1.type,
        t1.set_num,
        t1.num_parts,
        t2.theme_id
    from 
        {{ ref("inventory_nums") }} t1
    left join
        {{ source("raw", "sets") }} t2
    on
        t1.set_num = t2.set_num
    and
        t1.year = t2.year
), 
aux2 as (
    select
        max(id) over(partition by year, num_parts) as max_id,
        id,
        year,
        max(num_parts) over(partition by year) as max_num_parts,
        max(set_num) over(partition by year, num_parts) as set_num,
        num_parts    
    from 
        aux1
    where 
        theme_id not in (507, 510, 511, 517, 520, 529, 533, 534 )
),
source_data as (
    select
        id,
        year,
        num_parts,
        set_num
    from 
        aux2
    where 
        max_num_parts = num_parts
    and    
        max_id = id

)

select * from source_data