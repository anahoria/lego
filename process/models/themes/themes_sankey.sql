with aux1 as (

    select 
        count(*) as weight,
        grandparent_theme_name as source,
        parent_theme_name as target,
        0 as step_from,
        1 as step_to
    from 
        {{ ref("inventory_detailed") }}
    where 
        grandparent_theme_name is not null
    group by
        grandparent_theme_name,  
        parent_theme_name
    union all
    select 
        count(*) as weight,
        parent_theme_name as source,
        theme_name as target,
        0 as step_from,
        1 as step_to
    from 
        {{ ref("inventory_detailed") }}
    where
        grandparent_theme_name is null
    and 
        parent_theme_name is not null
    and 
        lower(parent_theme_name) in 
            ('educational and dacta',
            'duplo',
            'seasonal',
            'super heroes dc',
            'super heroes marvel')
    group by
        parent_theme_name,
        theme_name
    union all
    select 
        count(*) as weight,
        parent_theme_name as source,
        theme_name as target,
        1 as step_from,
        2 as step_to
    from 
        {{ ref("inventory_detailed") }}
    where 
        parent_theme_name is not null
        and 
        lower(parent_theme_name) not in 
            ('educational and dacta',
            'duplo',
            'seasonal',
            'super heroes dc',
            'super heroes marvel')
    group by
        parent_theme_name,
        theme_name
    union all
    select 
        count(*) as weight,
        'only_child' as source,
        theme_name as target,
        1 as step_from,
        2 as step_to
    from 
        {{ ref("inventory_detailed") }}
    where 
        parent_theme_name is null
    and 
        theme_name is not null 
    group by
        theme_name
),
aux2 as (
    select 
        sum(weight) as weight,
        source,
        target,
        step_from,
        step_to
    from
        aux1
    group by
        source,
        target,
        step_from,
        step_to
),
aux3 as (
    select 
        weight,
        source,
        case 
            when 
                weight < 100
            then
                'other'
        else 
            target 
        end as target,
        step_from,
        step_to
    from 
        aux2
),
source_data as (
       select 
        sum(weight) as weight,
        source,
        target,
        step_from,
        step_to
    from
        aux3
    group by
        source,
        target,
        step_from,
        step_to 
)

select * from source_data