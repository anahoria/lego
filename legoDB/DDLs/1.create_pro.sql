CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE legoPRO.raw.themes (
	id BIGINT PRIMARY KEY,
	name VARCHAR,
	parent_id BIGINT
);


COPY legoPRO.raw.themes
FROM '/Users/anamorenobarrado/lego/legoDB/raw/gz/PRO/themes.csv.gz' (HEADER);


CREATE TABLE legoPRO.raw."sets" (
	set_num VARCHAR PRIMARY KEY,
	name VARCHAR,
	"year" BIGINT,
	theme_id BIGINT REFERENCES raw.themes(id),
	num_parts BIGINT,
	img_url VARCHAR
);


COPY legoPRO.raw."sets" 
FROM '/Users/anamorenobarrado/lego/legoDB/raw/gz/PRO/sets.csv.gz'
(HEADER);


CREATE TABLE legoPRO.raw.inventories (
	id BIGINT PRIMARY KEY,
	version BIGINT,
	set_num VARCHAR
);


COPY legoPRO.raw.inventories 
FROM '/Users/anamorenobarrado/lego/legoDB/raw/gz/PRO/inventories.csv.gz'
(HEADER);


CREATE TABLE legoPRO.raw.inventory_sets (
	inventory_id BIGINT REFERENCES raw.inventories(id),
	set_num VARCHAR REFERENCES raw.sets(set_num),
	quantity BIGINT
);


COPY legoPRO.raw.inventory_sets 
FROM '/Users/anamorenobarrado/lego/legoDB/raw/gz/PRO/inventory_sets.csv.gz'
(HEADER);


CREATE TABLE legoPRO.raw.part_categories (
	id BIGINT PRIMARY KEY,
	name VARCHAR
);


COPY legoPRO.raw.part_categories 
FROM '/Users/anamorenobarrado/lego/legoDB/raw/gz/PRO/part_categories.csv.gz'
(HEADER);


CREATE TABLE legoPRO.raw.colors (
	id BIGINT PRIMARY KEY,
	name VARCHAR,
	rgb VARCHAR,
	is_trans BOOLEAN
);


COPY legoPRO.raw.colors 
FROM '/Users/anamorenobarrado/lego/legoDB/raw/gz/PRO/colors.csv.gz'
(HEADER);


CREATE TABLE legoPRO.raw.parts (
	part_num VARCHAR PRIMARY KEY,
	name VARCHAR,
	part_cat_id BIGINT REFERENCES raw.part_categories,
	part_material VARCHAR
);


COPY legoPRO.raw.parts 
FROM '/Users/anamorenobarrado/lego/legoDB/raw/gz/PRO/parts.csv.gz'
(HEADER);


CREATE TABLE legoPRO.raw.elements (
	element_id VARCHAR,
	part_num VARCHAR REFERENCES raw.parts(part_num),
	color_id BIGINT REFERENCES raw.colors(id),
	design_id BIGINT
);


COPY legoPRO.raw.elements 
FROM '/Users/anamorenobarrado/lego/legoDB/raw/gz/PRO/elements.csv.gz'
(HEADER);


CREATE TABLE legoPRO.raw.inventory_parts (
	inventory_id BIGINT REFERENCES raw.inventories(id),
	part_num VARCHAR REFERENCES raw.parts(part_num),
	color_id BIGINT REFERENCES raw.colors(id),
	quantity BIGINT,
	is_spare BOOLEAN,
	img_url VARCHAR
);


COPY legoPRO.raw.inventory_parts 
FROM '/Users/anamorenobarrado/lego/legoDB/raw/gz/PRO/inventory_parts.csv.gz'
(HEADER);


CREATE TABLE legoPRO.raw.part_relationships (
	rel_type VARCHAR,
	child_part_num VARCHAR REFERENCES raw.parts(part_num),
	parent_part_num VARCHAR REFERENCES raw.parts(part_num)
);


COPY legoPRO.raw.part_relationships 
FROM '/Users/anamorenobarrado/lego/legoDB/raw/gz/PRO/part_relationships.csv.gz'
(HEADER);


CREATE TABLE legoPRO.raw.minifigs (
	fig_num VARCHAR PRIMARY KEY,
	name VARCHAR,
	num_parts BIGINT,
	img_url VARCHAR
);


COPY legoPRO.raw.minifigs
FROM '/Users/anamorenobarrado/lego/legoDB/raw/gz/PRO/minifigs.csv.gz' (HEADER);


CREATE TABLE legoPRO.raw.inventory_minifigs (
	inventory_id BIGINT REFERENCES raw.inventories(id),
	fig_num VARCHAR REFERENCES raw.minifigs(fig_num),
	quantity BIGINT
);

COPY legoPRO.raw.inventory_minifigs 
FROM '/Users/anamorenobarrado/lego/legoDB/raw/gz/PRO/inventory_minifigs.csv.gz'
(HEADER);


-- Modificada a mano, no viene de rebrickable
CREATE TABLE legoPRO.main.color_simplified (
	id BIGINT,
	color_name VARCHAR,
	rgb VARCHAR,
	color_group VARCHAR,
	group_rgb VARCHAR,
	is_trans BOOLEAN
);

COPY legoPRO.main.color_simplified
FROM '/Users/anamorenobarrado/lego/legoDB/raw/csv/color_simplified.csv'
(HEADER);