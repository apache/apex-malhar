JdbcToJdbc App

This application reads from a source table in MySQL, creates POJO's and writes the POJO's to another table in MySQL.

Steps :

Step 1 : Update the below properties in the properties file - src/site/conf/example.xml

1.dt.application.JdbcToJdbcApp.operator.JdbcInput.prop.store.databaseUrl
- data base URL of the form jdbc:mysql://hostName:portNumber/dbName
2.dt.application.JdbcToJdbcApp.operator.JdbcInput.prop.store.userName
- mysql user name
3.dt.application.JdbcToJdbcApp.operator.JdbcInput.prop.store.password
- password
4.dt.application.JdbcToJdbcApp.operator.JdbcOutput.prop.store.databaseUrl
- data base URL of the form jdbc:mysql://hostName:portNumber/dbName
5.dt.application.JdbcToJdbcApp.operator.JdbcOutput.prop.store.userName
- mysql user name
6.dt.application.JdbcToJdbcApp.operator.JdbcOutput.prop.store.password
- password

Step 2: Create database, table and add entries

Go to mysql console and run the below command,
mysql> source <path to > src/test/resources/example.sql

After this is done, please verify that testDev.test_event_table is created and has 10 rows.It will also create an output table by the name testDev.test_output_event_table

mysql> select count(*) from testDev.test_event_table;
+----------+
| count(*) |
+----------+
|       10 |
+----------+

Step 3: Build the code,
shell> mvn clean install 

Upload the target/jdbcInput-1.0-SNAPSHOT.apa to the gateway

Step 4 : During launch use "Specify custom properties" option and select example.xml

Verification :

Log on to the mysql console

mysql> select count(*) from testDev.test_event_table;
+----------+
| count(*) |
+----------+
|       10 |
+----------+



