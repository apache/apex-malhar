This example shows how to use `FSRecordReaderModule` to read newline delimited records from a file convert them to plain old java objects (POJO) using `CSVParser`. These POJOs are converted to String using `CsvFormatter` and then written to output file using `StringFileOutputOperator`.

The properties file `META-INF/properties.xml` shows how to configure the respective operators.

The application can be run on an actual cluster or in local mode within your IDE by
simply running the method `ApplicationTest.testApplication()`.

One may tweak this example to add operator of their choice in between `CSVParser` and `CsvFormatter` to achieve functionality to suit your need. 
