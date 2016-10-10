package com.example.transform;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.common.partitioner.StatelessPartitioner;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.transform.TransformOperator;

@ApplicationAnnotation(name="TransformExample")
public class Application implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    POJOGenerator input = dag.addOperator("Input", new POJOGenerator());
    TransformOperator transform = dag.addOperator("Process", new TransformOperator());
    // Set expression map
    Map<String, String> expMap = new HashMap<>();
    expMap.put("name", "{$.firstName}.concat(\" \").concat({$.lastName})");
    expMap.put("age", "(new java.util.Date()).getYear() - {$.dateOfBirth}.getYear()");
    expMap.put("address", "{$.address}.toLowerCase()");
    transform.setExpressionMap(expMap);
    ConsoleOutputOperator output = dag.addOperator("Output", new ConsoleOutputOperator());

    dag.addStream("InputToTransform", input.output, transform.input);
    dag.addStream("TransformToOutput", transform.output, output.input);

    dag.setInputPortAttribute(transform.input, Context.PortContext.TUPLE_CLASS, CustomerEvent.class);
    dag.setOutputPortAttribute(transform.output, Context.PortContext.TUPLE_CLASS, CustomerInfo.class);
    dag.setAttribute(transform, Context.OperatorContext.PARTITIONER, new StatelessPartitioner<TransformOperator>(2));
  }
}
