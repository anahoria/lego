CREATE TABLE lego.raw.themes (
	id BIGINT PRIMARY KEY,
	name VARCHAR,
	parent_id BIGINT
);


COPY lego.raw.themes
FROM '/Users/anamorenobarrado/lego/legoDB/raw/gz/themes.csv.gz' (HEADER);


CREATE TABLE lego.raw."sets" (
	set_num VARCHAR PRIMARY KEY,
	name VARCHAR,
	"year" BIGINT,
	theme_id BIGINT REFERENCES raw.themes(id),
	num_parts BIGINT,
	img_url VARCHAR
);


COPY lego.raw."sets" 
FROM '/Users/anamorenobarrado/lego/legoDB/raw/gz/sets.csv.gz'
(HEADER);


CREATE TABLE lego.raw.inventories (
	id BIGINT PRIMARY KEY,
	version BIGINT,
	set_num VARCHAR
);


COPY lego.raw.inventories 
FROM '/Users/anamorenobarrado/lego/legoDB/raw/gz/inventories.csv.gz'
(HEADER);


CREATE TABLE lego.raw.inventory_sets (
	inventory_id BIGINT REFERENCES raw.inventories(id),
	set_num VARCHAR REFERENCES raw.sets(set_num),
	quantity BIGINT
);


COPY lego.raw.inventory_sets 
FROM '/Users/anamorenobarrado/lego/legoDB/raw/gz/inventory_sets.csv.gz'
(HEADER);


CREATE TABLE lego.raw.part_categories (
	id BIGINT PRIMARY KEY,
	name VARCHAR
);


COPY lego.raw.part_categories 
FROM '/Users/anamorenobarrado/lego/legoDB/raw/gz/part_categories.csv.gz'
(HEADER);


CREATE TABLE lego.raw.colors (
	id BIGINT PRIMARY KEY,
	name VARCHAR,
	rgb VARCHAR,
	is_trans BOOLEAN
);


COPY lego.raw.colors 
FROM '/Users/anamorenobarrado/lego/legoDB/raw/gz/colors.csv.gz'
(HEADER);


CREATE TABLE lego.raw.parts (
	part_num VARCHAR PRIMARY KEY,
	name VARCHAR,
	part_cat_id BIGINT REFERENCES raw.part_categories,
	part_material VARCHAR
);


COPY lego.raw.parts 
FROM '/Users/anamorenobarrado/lego/legoDB/raw/gz/parts.csv.gz'
(HEADER);


CREATE TABLE lego.raw.elements (
	element_id VARCHAR,
	part_num VARCHAR REFERENCES raw.parts(part_num),
	color_id BIGINT REFERENCES raw.colors(id),
	design_id BIGINT
);


COPY lego.raw.elements 
FROM '/Users/anamorenobarrado/lego/legoDB/raw/gz/elements.csv.gz'
(HEADER);


CREATE TABLE lego.raw.inventory_parts (
	inventory_id BIGINT REFERENCES raw.inventories(id),
	part_num VARCHAR REFERENCES raw.parts(part_num),
	color_id BIGINT REFERENCES raw.colors(id),
	quantity BIGINT,
	is_spare BOOLEAN,
	img_url VARCHAR
);


COPY lego.raw.inventory_parts 
FROM '/Users/anamorenobarrado/lego/legoDB/raw/gz/inventory_parts.csv.gz'
(HEADER);


CREATE TABLE lego.raw.part_relationships (
	rel_type VARCHAR,
	child_part_num VARCHAR REFERENCES raw.parts(part_num),
	parent_part_num VARCHAR REFERENCES raw.parts(part_num)
);


COPY lego.raw.part_relationships 
FROM '/Users/anamorenobarrado/lego/legoDB/raw/gz/part_relationships.csv.gz'
(HEADER);


CREATE TABLE lego.raw.minifigs (
	fig_num VARCHAR PRIMARY KEY,
	name VARCHAR,
	num_parts BIGINT,
	img_url VARCHAR
);


COPY lego.raw.minifigs
FROM '/Users/anamorenobarrado/lego/legoDB/raw/gz/minifigs.csv.gz' (HEADER);


CREATE TABLE lego.raw.inventory_minifigs (
	inventory_id BIGINT REFERENCES raw.inventories(id),
	fig_num VARCHAR REFERENCES raw.minifigs(fig_num),
	quantity BIGINT
);


COPY lego.raw.inventory_minifigs 
FROM '/Users/anamorenobarrado/lego/legoDB/raw/gz/inventory_minifigs.csv.gz'
(HEADER);