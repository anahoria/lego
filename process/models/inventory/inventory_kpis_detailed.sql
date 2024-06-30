with aux1 as (
    select
        t1.id,
        t1.year,
        t1.n_items,
        t1.num_parts,
        t1.set_num,
        t2.name,
        t2.img_url,
        t2.theme_id,
        t3.name as theme_name,
        t3.parent_id as parent_theme_id, 
        t4.name as parent_theme_name,
        t4.parent_id as grandparent_theme_id,
        t5.name as grandparent_theme_name
    from 
        {{ ref("inventory_kpis") }} t1
    left join
        {{ source("raw", "sets") }} t2
    on
        t1.set_num = t2.set_num
    and
        t1.year = t2.year
    left join
        {{ source("raw", "themes") }} t3
    on
        t2.theme_id = t3.id
    left join
        {{ source("raw", "themes") }} t4
    on
        t3.parent_id = t4.id
    left join
        {{ source("raw", "themes") }} t5
    on
        t4.parent_id = t5.id
    where
        t1.year is not null
),
aux2 as (
    select
        id, 
        year,
        n_items,
        num_parts,
        set_num,
        name,
        img_url,
        case when parent_theme_id is null
                then theme_id 
            when (parent_theme_id is not null and grandparent_theme_id is null)
                then parent_theme_id
            else grandparent_theme_id
        end as first_theme_id,
        case when (parent_theme_id is not null and grandparent_theme_id is null)
                then theme_id
            else parent_theme_id
        end as second_theme_id,
        case when grandparent_theme_id is not null
                then theme_id
            else null
        end as third_theme_id,
        case when parent_theme_id is null
                then theme_name
            when (parent_theme_id is not null and grandparent_theme_id is null)
                then parent_theme_name
            else grandparent_theme_name
        end as first_theme_name,
        case when (parent_theme_id is not null and grandparent_theme_id is null)
                then theme_name
            else parent_theme_name
        end as second_theme_name,
        case when grandparent_theme_id is not null
                then theme_name
            else null
        end as third_theme_name,
        theme_id,
        theme_name,
        parent_theme_id, 
        parent_theme_name,
        grandparent_theme_id,
        grandparent_theme_name
    from
        aux1
),
aux3 as (
    select
        distinct
        t1.id,
        t1.year,
        t1.n_items,
        t1.num_parts,
        t1.set_num,
        t1.name,
        t1.img_url as set_img_url,
        t1.first_theme_id,
        t1.second_theme_id,
        t1.third_theme_id,
        t1.first_theme_name,
        t1.second_theme_name,
        t1.third_theme_name,
        t2.color_id,
        t3.color_name,
        t3.color_rgb,
        t3.color_group_id,
        t3.color_group_name,
        t3.color_group_rgb,
        sum(t2.quantity) over(partition by t2.color_id, t1.id) as aux_n_color_parts,
        sum(t2.quantity) over(partition by t3.color_group_id, t1.id) as aux_n_color_group_parts,
        t3.color_id_by_quantity,
        t3.color_group_id_by_quantity
    from 
        aux2 t1
    left join
        {{ source("raw", "inventory_parts") }} t2
    on
        t1.id = t2.inventory_id
    left join
        {{ ref("colors_and_groups") }} t3
    on
        t2.color_id = t3.color_id
    where 
        t2.is_spare is not null
),
aux4 as (
    select
        id,
        year,
        n_items,
        num_parts,
        set_num,
        name,
        set_img_url,
        first_theme_id,
        second_theme_id,
        third_theme_id,
        first_theme_name,
        second_theme_name,
        third_theme_name,
        color_id,
        color_name,
        color_rgb,
        color_group_id,
        color_group_name,
        color_group_rgb,
        aux_n_color_parts,
        aux_n_color_group_parts,
        max(aux_n_color_parts) over(partition by id) as n_color_parts,
        max(aux_n_color_group_parts) over(partition by id) as n_color_group_parts,
        color_id_by_quantity,
        color_group_id_by_quantity 
    from
        aux3 
),
aux5 as (
    select
        distinct
        id,
        year,
        n_items,
        num_parts,
        set_num,
        name,
        set_img_url,
        first_theme_id,
        second_theme_id,
        third_theme_id,
        first_theme_name,
        second_theme_name,
        third_theme_name,
        'color' as agg_type,
        color_id,
        color_name,
        color_rgb,
        n_color_parts,
        color_id_by_quantity
    from
        aux4
    where 
        n_color_parts = aux_n_color_parts
    union all
    select
        distinct
        id,
        year,
        n_items,
        num_parts,
        set_num,
        name,
        set_img_url,
        first_theme_id,
        second_theme_id,
        third_theme_id,
        first_theme_name,
        second_theme_name,
        third_theme_name,
        'color_group' as agg_type,
        color_group_id as color_id,
        color_group_name as color_name,
        color_group_rgb as color_rgb,
        n_color_group_parts as n_color_parts,
        color_group_id_by_quantity as color_id_by_quantity
    from
        aux4
    where    
        n_color_group_parts = aux_n_color_group_parts       
),
aux6 as (
    select 
        id,
        year,
        n_items,
        num_parts,
        set_num,
        name,
        set_img_url,
        first_theme_id,
        second_theme_id,
        third_theme_id,
        first_theme_name,
        second_theme_name,
        third_theme_name,
        agg_type,
        color_id,
        color_name,
        color_rgb,
        n_color_parts,
        rank() over(partition by id, agg_type order by color_id_by_quantity asc) as rank_color
    from
        aux5
),
source_data as (
    select
        id,
        year,
        n_items,
        num_parts,
        set_num,
        name,
        set_img_url,
        first_theme_id,
        second_theme_id,
        third_theme_id,
        first_theme_name,
        second_theme_name,
        third_theme_name,
        agg_type,
        color_id,
        color_name,
        color_rgb,
        n_color_parts
    from 
        aux6
    where
        rank_color = 1
)
select * from source_data