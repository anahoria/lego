# lego
Data storytelling how-to from lego data.

## Data
We are going to create a relational database to store data from

[rebrickable](https://rebrickable.com/)

wich is an open source database about all sets of lego sold in the world from the beggining. You can finde all data stored in *csv files and they share the data model with the relationships between tables.

![alt text](img/lego.webp)

For this use case we are going to create an identical datamodel in duckDB database.

The files corresponding to ecah table are in the url

[rawDataRebrickable](https://rebrickable.com/downloads/)

and in this repository folder

```
legoDB/raw
```
### Database

The chosen database for this proyect is 

[duckDB](https://duckdb.org/)

and all the DDLs to crecreate the model are stored in this repository

```
legoDB/DDLs
```

## Hands on the model, understanding the tables

There is 1 table with the global inventory which contains a single row for each inventory item. This table relates to other three with the detail about the parts, the sets and the minifig.

![Tabla items por categoría](./img/tabla_inventory_categories.png)

Following the list in order of volume, there is a tree that clasifies the inventory items in 8 categories.

![Árbol items por categoría](./img/inventory_categories.svg)

### Only sets

This table contains all the inventory items in inventory_parts and not in inventory_minifigs or inventory_sets.

28858 unique items in the inventory.




