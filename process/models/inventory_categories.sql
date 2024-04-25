with source_data as (

	select 
	count(distinct id) as n_items,
	parts,
	minifigs,
	sets,
	parts_desc,
	minifigs_desc,
	sets_desc,
	(parts+minifigs+sets) as category
	from (
		select 
			id,
			case when inv_id_parts is null
				then 'not in parts'
				else 'in parts' 
			end as parts_desc,
			case when inv_id_minifigs is null
				then 'not in minifigs'
				else 'in minifigs' 
			end as minifigs_desc,
			case when inv_id_sets is null
				then 'not in sets'
				else 'in sets' 
			end as sets_desc,
			case when inv_id_parts is null
				then 0
				else 4 
			end as parts,
			case when inv_id_minifigs is null
				then 0
				else 2 
			end as minifigs,
			case when inv_id_sets is null
				then 0
				else 1
			end as sets
		from (
			select 
				t1.id as id,
				t2.inventory_id as inv_id_parts,
				t3.inventory_id as inv_id_minifigs,
				t4.inventory_id as inv_id_sets
			from 
				{{ source("raw", "inventories") }} t1
			left join 
				{{ source("raw", "inventory_parts") }} t2
			on t1.id = t2.inventory_id 
			left join
				{{ source("raw", "inventory_minifigs") }} t3
			on t1.id = t3.inventory_id
			left join {{ source("raw", "inventory_sets") }} t4
			on t1.id = t4.inventory_id
		)
	)
	group by parts, minifigs, sets, parts_desc, minifigs_desc, sets_desc

)

select *
from source_data
