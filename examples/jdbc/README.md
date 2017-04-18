## File to JDBC Example (FileToJdbcApp)

This example shows how to read files from HDFS, parse into POJOs and then insert into a table in a database.

Given various parsing demands, we give two applications under this package, `FileToJdbcCsvParser` and `FileToJdbcCustomParser`. 

`CsvParser` allows you to parse only CSV format input files. For more complex input format, `CustomParser` allows you to set custom regex to parse. 

A sample properties file (`/src/main/resources/META-INF/properties-FileToJdbcApp.xml`) is provided for these applications and would need to be
customized according to the user's environment.

The applications can then be launched using the apex command line interface and selecting the above configuration file using a parameter during 
launch.

####**Update Properties:**

- Update these common properties in the file `/src/main/resources/META-INF/properties-FileToJdbcApp.xml`:

| Property Name  | Description |
| -------------  | ----------- |
| dt.operator.FileReader.prop.directory |HDFS input directory path 
| dt.operator.JdbcOutput.prop.store.databaseUrl | database URL |
| dt.operator.JdbcOutput.prop.store.userName | database user name |
| dt.operator.JdbcOutput.prop.store.password | database user password |
| dt.operator.JdbcOutput.prop.tablename   | database output table name |
| dt.operator.CustomParser.prop.regexStr   | update regexStr if needed|

####**Sample Input:**

- To set up database and create table, check `src/test/resources/example-FileToJdbcApp-sql.txt` 
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
- To change input format, update `PojoEvent` class and `addFieldInfos()` method in `src/main/java/org/apache/apex/examples/FileToJdbcApp`. 
If using CsvParser, also update `src/main/resources/schema.json`.

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


## JDBC ingestion examples

This project contains two applications to read records from a table in database, create POJOs and write them to a file
in the user specified directory in HDFS.

1. SimpleJdbcToHDFSApp: Reads table records as per given query and emits them as POJOs.
2. PollJdbcToHDFSApp: Reads table records using partitions in parallel fashion also polls for newly **appended** records and emits them as POJOs.

Follow these steps to run these applications:

**Step 1**: Update these properties in the file `src/main/resources/META_INF/properties-<applicationName>.xml`, where <applicationName> represents 
the application name and is one of two names above:

| Property Name  | Description |
| -------------  | ----------- |
| dt.application.<applicationName>.operator.JdbcInput.prop.store.databaseUrl | database URL, for example `jdbc:hsqldb:mem:test` |
| dt.application.<applicationName>.operator.JdbcInput.prop.store.userName | database user name |
| dt.application.<applicationName>.operator.JdbcInput.prop.store.password | database user password |
| dt.application.<applicationName>.operator.FileOutputOperator.filePath   | HDFS output directory path |

**Step 2**: Create database table and add entries

Go to the database console and run (where _{path}_ is a suitable prefix):

    source {path}/src/test/resources/example.sql

After this, please verify that `testDev.test_event_table` is created and has 10 rows:

    select count(*) from testDev.test_event_table;
    +----------+
    | count(*) |
    +----------+
    |       10 |
    +----------+

**Step 3**: Create HDFS output directory if not already present (_{path}_ should be the same as specified in `META_INF/properties-<applicationName>.xml`):

    hadoop fs -mkdir -p {path}

**Step 4**: Build the code:

    mvn clean install

**Step 5**: During launch use `src/main/resources/META_INF/properties-<applicationName>.xml` as a custom configuration file; then verify
that the output directory has the expected output:

    hadoop fs -cat <hadoop directory path>/2_op.dat.* | wc -l

This should return 10 as the count.

Sample Output:

    hadoop fs -cat <path_to_file>/2_op.dat.0
    PojoEvent [accountNumber=1, name=User1, amount=1000]
    PojoEvent [accountNumber=2, name=User2, amount=2000]
    PojoEvent [accountNumber=3, name=User3, amount=3000]
    PojoEvent [accountNumber=4, name=User4, amount=4000]
    PojoEvent [accountNumber=5, name=User5, amount=5000]
    PojoEvent [accountNumber=6, name=User6, amount=6000]
    PojoEvent [accountNumber=7, name=User7, amount=7000]
    PojoEvent [accountNumber=8, name=User8, amount=8000]
    PojoEvent [accountNumber=9, name=User9, amount=9000]
    PojoEvent [accountNumber=10, name=User10, amount=1000]


## JdbcToJdbc App

This application reads from a source table in a database, creates POJO's and writes the POJO's to another table in a database.

Steps :

Step 1 : Update the below properties in the properties file - `src/main/resources/META_INF/properties-JdbcToJdbcApp.xml`

1.dt.application.JdbcToJdbcApp.operator.JdbcInput.prop.store.databaseUrl
- data base URL for your database, for example jdbc:hsqldb:mem:test
2.dt.application.JdbcToJdbcApp.operator.JdbcInput.prop.store.userName
- mysql user name
3.dt.application.JdbcToJdbcApp.operator.JdbcInput.prop.store.password
- password
4.dt.application.JdbcToJdbcApp.operator.JdbcOutput.prop.store.databaseUrl
- data base URL for your database, for example jdbc:jdbc:hsqldb:mem:test
5.dt.application.JdbcToJdbcApp.operator.JdbcOutput.prop.store.userName
- mysql user name
6.dt.application.JdbcToJdbcApp.operator.JdbcOutput.prop.store.password
- password

Step 2: Create database, table and add entries

Load into your database the contents of the following sql file
<path to > src/test/resources/example-JdbcToJdbc-sql.txt

After this is done, please verify that testDev.test_event_table is created and has 10 rows.It will also create an output table by the name testDev.test_output_event_table

select count(*) from testDev.test_event_table;
+----------+
| count(*) |
+----------+
|       10 |
+----------+

Step 3: Build the code,
shell>mvn clean install 

This will compile the project and create the application package in the target folder.

Step 4 : Launch the application package with the apex command line interface and 
select the above configuration file during launch.

Verification :

Log on to the mysql console

select count(*) from testDev.test_event_table;
+----------+
| count(*) |
+----------+
|       10 |
+----------+



