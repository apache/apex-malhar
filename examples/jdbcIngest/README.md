## Sample mysql implementation

This project contains two applications to read records from a table in `MySQL`, create POJOs and write them to a file
in the user specified directory in HDFS.

1. SimpleJdbcToHDFSApp: Reads table records as per given query and emits them as POJOs.
2. PollJdbcToHDFSApp: Reads table records using partitions in parallel fashion also polls for newly **appended** records and emits them as POJOs.

Follow these steps to run these applications:

**Step 1**: Update these properties in the file `src/main/resources/META_INF/properties-<applicationName>.xml`:

| Property Name  | Description |
| -------------  | ----------- |
| dt.application.<applicationName>.operator.JdbcInput.prop.store.databaseUrl | database URL of the form `jdbc:mysql://hostName:portNumber/dbName` |
| dt.application.<applicationName>.operator.JdbcInput.prop.store.userName | MySQL user name |
| dt.application.<applicationName>.operator.JdbcInput.prop.store.password | MySQL user password |
| dt.application.<applicationName>.operator.FileOutputOperator.filePath   | HDFS output directory path |

**Step 2**: Create database table and add entries

Go to the MySQL console and run (where _{path}_ is a suitable prefix):

    mysql> source {path}/src/test/resources/example.sql

After this, please verify that `testDev.test_event_table` is created and has 10 rows:

    mysql> select count(*) from testDev.test_event_table;
    +----------+
    | count(*) |
    +----------+
    |       10 |
    +----------+

**Step 3**: Create HDFS output directory if not already present (_{path}_ should be the same as specified in `META_INF/properties-<applicationName>.xml`):

    hadoop fs -mkdir -p {path}

**Step 4**: Build the code:

    shell> mvn clean install

Upload the `target/jdbcInput-1.0-SNAPSHOT.apa` to the UI console if available or launch it from
the commandline using `apexcli`.

**Step 5**: During launch use `src/main/resources/META_INF/properties-<applicationName>.xml` as a custom configuration file; then verify
that the output directory has the expected output:

    shell> hadoop fs -cat <hadoop directory path>/2_op.dat.* | wc -l

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
