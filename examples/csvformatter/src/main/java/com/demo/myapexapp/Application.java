package com.demo.myapexapp;

import java.util.Arrays;

import com.datatorrent.contrib.parser.JsonParser;

import org.apache.apex.malhar.contrib.parser.StreamingJsonParser;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StatsListener;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.formatter.CsvFormatter;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.io.fs.AbstractFileInputOperator.DirectoryScanner;
import com.datatorrent.lib.partitioner.StatelessThroughputBasedPartitioner;

@ApplicationAnnotation(name = "CustomOutputFormatter")
public class Application implements StreamingApplication
{
  //Set the delimiters and schema structure  for the custom output in schema.json
  private static final String filename = "schema.json";

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    JsonGenerator generator = dag.addOperator("JsonGenerator", JsonGenerator.class);
    JsonParser jsonParser = dag.addOperator("jsonParser", JsonParser.class);

    CsvFormatter formatter = dag.addOperator("formatter", CsvFormatter.class);
    formatter.setSchema(SchemaUtils.jarResourceFileToString(filename));
    dag.setInputPortAttribute(formatter.in, PortContext.TUPLE_CLASS, PojoEvent.class);

    HDFSOutputOperator<String> hdfsOutput = dag.addOperator("HDFSOutputOperator", HDFSOutputOperator.class);
    hdfsOutput.setLineDelimiter("");

    dag.addStream("parserStream", generator.out, jsonParser.in);
    dag.addStream("formatterStream", jsonParser.out, formatter.in);
    dag.addStream("outputStream", formatter.out, hdfsOutput.input);

  }
}
