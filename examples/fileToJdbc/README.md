## Sample File to JDBC Example

This example shows how to read files from HDFS, parse into POJOs and then insert into a table in MySQL.

Given various parsing demands, we give two applications under this package, `FileToJdbcCsvParser` and `FileToJdbcCustomParser`. 

`CsvParser` allows you to parse only CSV format input files. For more complex input format, `CustomParser` allows you to set custom regex to parse. 

Accordingly, we have two additional configuration files (`src/site/conf/exampleCsvParser.xml` and `src/site/conf/exampleCustomParser.xml`) besides the common properties file (`/src/main/resources/META-INF/properties.xml`). 

Users can choose which applicaiton and which addtional configuration file to use during launch time.


####**Update Properties:**

- Update these common properties in the file `/src/main/resources/META-INF/properties.xml`:

| Property Name  | Description |
| -------------  | ----------- |
| dt.operator.FileReader.prop.directory |HDFS input directory path 
|dt.operator.JdbcOutput.prop.store.databaseUrl | database URL of the form `jdbc:mysql://hostName:portNumber/dbName` |
| dt.operator.JdbcOutput.prop.store.userName | MySQL user name |
| dt.operator.JdbcOutput.prop.store.password | MySQL user password |
| dt.operator.JdbcOutput.prop.tablename   | MySQL output table name |

- Using CustomParser: update `regexStr` in file `src/site/conf/exampleCustomParser.xml`


####**Sample Input:**

- To set up MySQL database and create table, check `src/test/resources/example.sql` 
- To run this example, create files using this format: 

```
    1,User1,1000
    2,User2,2000
    3,User3,3000
    4,User4,4000
    5,User5,5000
    6,User6,6000
    7,User7,7000
    8,User8,8000
    9,User9,9000
    10,User10,10000
```
- To change input format, update `PojoEvent` class and `addFieldInfos()` method in `src/main/java/com/example/FileToJdbcApp`. If using CsvParser, also update `src/main/resources/schema.json`.

####**Sample Output:**

- After running successfully, verify
that the database table has the expected output: 

```	
    mysql> select * from table_name;
    +------------+--------+--------+
    | ACCOUNT_NO | NAME   | AMOUNT |
    +------------+--------+--------+
    |          1 | User1  |   1000 |
    |          2 | User2  |   2000 |
    |          3 | User3  |   3000 |
    |          4 | User4  |   4000 |
    |          5 | User5  |   5000 |
    |          6 | User6  |   6000 |
    |          7 | User7  |   7000 |
    |          8 | User8  |   8000 |
    |          9 | User9  |   9000 |
    |         10 | User10 |  10000 |
    +------------+--------+--------+
    10 rows in set (0.00 sec)
```
