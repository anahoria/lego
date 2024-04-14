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
