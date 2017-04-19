This project contains applications showcasing different **Parsers** and **Formatters** present in the Malhar Library. For all the apps, parameters are configurable in META_INF/properties.xml.

* **Json Parser App**  
This app showcases **Json Parser**. Data generator sends Json data to the Json Parser which emits each record as POJO on the *output* port. The parser also has *parsedOutput* port that outputs each record as JSONObject and *error* port that emits a error records as key value pair.

* **Csv Parser App**

This application showcases how to use [CsvParser](https://datatorrent.com/docs/apidocs/com/datatorrent/contrib/parser/CsvParser.html) from [Apex Malhar](https://github.com/apache/apex-malhar) library. The CsvParser converts your delimited data to a key-value pair map or concrete java class also know as [POJO](https://en.wikipedia.org/wiki/Plain_Old_Java_Object). The parser emits key-value pair map on *parsedOutput* port. It emits POJO on *out* and error records on *err* port.

Follow these steps to run this application:

**Step 1**: Build the code:

    shell> mvn clean install

**Step 2**: Upload the `target/parser-1.0-SNAPSHOT.apa` to the UI console if available or launch it from
the commandline using `apex` cli script.

**Step 3**: During launch use `src/main/resources/META-INF/properties-csvParseApplication.xml` as a custom configuration file; then verify
that the output by checking hdfs file path configured in properties-csvParseApplication.xml

* **Xml Parser App**

This application showcases how to use [XmlParser](https://datatorrent.com/docs/apidocs/com/datatorrent/lib/parser/XmlParser.html)
from [Apex Malhar](https://github.com/apache/apex-malhar) library. The XmlParser Operator converts XML string to POJO.
The parser emits dom based Document on *parsedOutput* port. It emits POJO on *out* and error records on *err* port.
Follow these steps to run this application:

**Step 1**: Build the code:

    shell> mvn clean install

**Step 2**: Upload the `target/parser-1.0-SNAPSHOT.apa` to the UI console if available or launch it from
the commandline using `apex` cli script.

**Step 3**: During launch use `src/main/resources/META-INF/properties-xmlParseApplication.xml` as a custom configuration file; then verify
that the output by checking hdfs file path configured in properties-xmlParseApplication.xml

* **RegexParser App**

This application showcases how to use [RegexParser](https://datatorrent.com/docs/apidocs/com/datatorrent/contrib/parser/RegexParser.html) from [Apex Malhar](https://github.com/apache/apex-malhar) library.

Follow these steps to run this application:

**Step 1**: Build the code:

    shell> mvn clean install

**Step 2**: Upload the `target/parser-1.0-SNAPSHOT.apa` to the UI console if available or launch it from
the commandline using `apex` cli script.

**Step 3**: During launch use `properties-regexParserApplication.xml` as a custom configuration file; then verify
that the output by checking hdfs file path configured in properties-regexParserApplication.xml