with source_data as (
    select 
        t1.id,
        t1.version,
        case when type = 'kit'
            then t2.year
            when type = 'set'
            then t3.year
            else null
        end as year,
        t1.set_num,
        t1.type,
        case when type = 'kit'
            then t2.num_parts
            when type = 'set'
            then t3.num_parts
            when type = 'minifig'
            then t4.num_parts
            else 0
        end as num_parts
    from
        {{ ref("inventory_categories") }} t1
    left join 
        {{ ref("kits_nums") }} t2
    on 
        t1.id = t2.id
    and 
        t1.version = t2.version
    and
        t1.set_num = t2.set_num
    left join 
        {{ ref("sets_nums") }} t3
    on 
        t1.id = t3.id
    and 
        t1.version = t3.version
    and
        t1.set_num = t3.set_num   
    left join 
        {{ ref("figs_nums") }} t4
    on 
        t1.id = t4.id
    and 
        t1.version = t4.version
    and
        t1.set_num = t4.set_num 
)

select * from source_data