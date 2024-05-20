with source_data as (
    
    select  name || ': #' || rgb as color_tag
    from 
        raw.colors 

)

select * from source_data