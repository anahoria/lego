with source_data as (

    select 
	    year,
	    color_name,
		rgb,
	    sum(n_pieces) over(partition by color_name order by year) as n_pieces 
    from (
        select 
	        t1.year as year, 
	        t1.color_name as color_name, 
			t1.rgb as rgb,
	        sum(coalesce(t2.n_pieces,0)) as n_pieces
        from	
	        (
		    select distinct 
			    t1.year as year, 
			    t2.name as color_name,
				t2.rgb as rgb
		    from 
			    {{ source("raw", "sets") }} t1,
			    {{ source("raw", "colors") }} t2
		    where t2.is_trans=false
	        ) t1
    left join 
	    (
		    select 
			    year,
			    color_name,
			    sum(quantity) as n_pieces
		    from {{ ref("sets_with_parts") }}
		    group by 
			    year, 
                color_name
	    ) t2
    on t1.year = t2.year
    and t1.color_name = t2.color_name
    group by 
        t1.year, 
        t1.color_name,
		t1.rgb
    )

) 

select * from source_data