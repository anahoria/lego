with aux as (
    select 
	    rank() over(partition by theme_id order by year asc) as aux,
	    year,
	    theme_id,
	    theme_name,
        relative
	from {{ ref("themes_by_year")}}
),
source_data as (
    select 
        t1.year, 
        t1.theme_id, 
        t1.theme_name,
        t1.relative,
        t2.parent_id as parent_theme_id,
        t3.name as parent_theme_name
    from aux t1
    left join {{ source("raw", "themes") }} t2
    on t1.theme_id = t2.id
    left join {{ source("raw", "themes") }} t3
    on t2.parent_id = t3.id
    where aux = 1
)
select * from source_data