# Filter operator example

Sample application to show how to use the filter operator.

The application reads transaction records from a csv file using `FSRecordReaderModule`. Then converts these records into plain old java objects (POJO) using CSVParser. These POJOs are filtered based on given condition. POJOs meeting the filter criteria are written to `selected.txt`. POJOs not meeting filter criteria are written to `rejected.txt`. Writing POJO output to file is done using `CSVFormatter` and `StringFileOutputOperator`

### How to configure
The properties file META-INF/properties.xml shows how to configure the respective operators.

### How to compile
`shell> mvn clean package`

This will generate application package filter-1.0-SNAPSHOT.apa inside target directory.

### How to run
Use the application package generated above to launch the application from UI console(if available) or apex command line interface.

`apex> launch target/filter-1.0-SNAPSHOT.apa`

You may also the run the application in local mode within your IDE by simply running the method ApplicationTest.testApplication().
