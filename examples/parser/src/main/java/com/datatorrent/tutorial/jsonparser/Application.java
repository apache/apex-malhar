package com.datatorrent.tutorial.jsonparser;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.parser.JsonParser;
import com.datatorrent.lib.formatter.JsonFormatter;
import com.datatorrent.lib.io.ConsoleOutputOperator;

@ApplicationAnnotation(name = "JsonProcessor")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {

    JsonGenerator generator = dag.addOperator("JsonGenerator", JsonGenerator.class);
    JsonParser parser = dag.addOperator("JsonParser", JsonParser.class);
    JsonFormatter formatter = dag.addOperator("JsonFormatter", JsonFormatter.class);

    ConsoleOutputOperator jsonString = dag.addOperator("JsonString", ConsoleOutputOperator.class);
    ConsoleOutputOperator jsonObject = dag.addOperator("JsonObject", ConsoleOutputOperator.class);
    ConsoleOutputOperator error = dag.addOperator("Error", ConsoleOutputOperator.class);

    dag.addStream("json", generator.out, parser.in);
    dag.addStream("pojo", parser.out, formatter.in);
    dag.addStream("jsonString", formatter.out, jsonString.input);
    dag.addStream("jsonObject", parser.parsedOutput, jsonObject.input);
    dag.addStream("error", parser.err, error.input);

  }
}
