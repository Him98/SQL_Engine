# SQL Engine
A SQL engine to parse and execute simple sql queries implemented in python

## Description
- .csv files contains the database and its schema.
- ![metadata.txt](metadata.txt) contains the schema of the database. Each table's schema is given between `<begin_table>` and `<end_table>`. The first line is the name of the table and others lines are field(column) names.
- Data stored in each table is given as csv file in current directory of the same name as of the table. For example, the data of table 'table1' is given as 'table1.csv'.
- Main source code is in ![20161120.py](20161120.py)
- Database contains only integer data.
- Queries are entirely case insensitive.
- Only simple queries can be performed. Nested queries are not allowed.
- Error handling is implemented with sufficient error debugging details.

### Run Query
Run `python3 20161120.py "<sql_query>"`

### Valid queries
- Normal select queries
  - `select * from table1;`
  - `select A, B from table1;`
- Aggregate functions like `min`, `max`, `avg`, `sum`, `count`
  - `select max(A) from table1;`
- Select distinct values of a column
  - `select distinct A from table1;`
- Conditional select with at most one conditions joined by `and` or `or`
  - `select A from table1 where A = 10;`
  - `select A, B from table1 where A = C or B = 5`

### Additional Comments
- Run on python3.
- All SQL statements should end with semicolon(;).