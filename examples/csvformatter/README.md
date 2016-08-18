## CsvFormatter Example

`CsvFormatter` converts the incoming POJO to CSV(by default) or custom delimiter('|', ':' etc.) separated string.  

Accordingly, we have additional config file for formatting preferences (`/src/main/resources/schema.json`) besides the common properties file (`/src/main/resources/META-INF/properties.xml`). 

Users can choose the application and additional configuration file to use during launch time. In this example, we use the files mentioned above to customise the schema and configure the operator properties.


#### **Update Properties from properties.xml - This is needed to run the example:**

- Update these common properties in the file `/src/main/resources/META-INF/properties.xml`:

| Property Name  | Description |
| -------------  | ----------- |
| dt.application.CustomOutputFormatter.operator.jsonParser.prop.sleepTime | sleep time for the container |
| dt.application.CustomOutputFormatter.operator.jsonParser.port.out.attr.TUPLE_CLASS | expected POJO object output of the JSONParser operator |
| dt.application.CustomOutputFormatter.operator.HDFSOutputOperator.prop.filePath | output file path for the records after formatting |
| dt.application.CustomOutputFormatter.operator.HDFSOutputOperator.prop.outFileName | output file name for the records to be written after formatting |


#### **Update Properties from Application.java - This is needed to customise Formatter:**

```
    CsvFormatter formatter = dag.addOperator("formatter", CsvFormatter.class);
    formatter.setSchema(SchemaUtils.jarResourceFileToString(filename));
    dag.setInputPortAttribute(formatter.in, PortContext.TUPLE_CLASS, PojoEvent.class); 
```

'filename' above is the variable for storing schema file in the example.

The input port attribute can be configured using properties.xml by setting the below property as well.

`dt.application.CustomOutputFormatter.operator.formatter.port.input.attr.TUPLE_CLASS`




#### **Sample Run:**

- This example generates the JsonData, inputs the generated data to JsonParser which creates the POJO that will be passed to CsvFormatter operator and the formatted output is written to a hdfs location.

- Configure JsonGenerator.java to generate needed data and define the schema of the POJO class PojoEvent.java which is input to the CsvFormatter. 
  
- The schema defined in the `/src/main/resources/schema.json` will be used to evaluate the fields from the object in the CsvFormatter, the field name should match with the field name from the schema.json string.

- The formatting order depends on the order defined in the schema.json string.
  
- You can build the project and run the example as it is once you configure properties.xml. You can also customise the same app to the schema needed by configuring the PojoEvent.java and schema.json.


#### **Sample Output:**

- After running successfully, verify that the hdfs files has the similar output : 

```	
   1234|SimpleCsvFormatterExample|10000.0|false|APEX
   1234|SimpleCsvFormatterExample|10000.0|false|APEX
   1234|SimpleCsvFormatterExample|10000.0|false|APEX
   1234|SimpleCsvFormatterExample|10000.0|false|APEX
   1234|SimpleCsvFormatterExample|10000.0|false|APEX
   ```

In case you have issues configuring the operator or running the application, please send an email to users@apache.apex.org.
