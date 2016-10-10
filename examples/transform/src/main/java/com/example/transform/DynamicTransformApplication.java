package com.example.transform;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StatsListener;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.common.partitioner.StatelessPartitioner;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.partitioner.StatelessThroughputBasedPartitioner;
import com.datatorrent.lib.transform.TransformOperator;

@ApplicationAnnotation(name="DynamicTransformApp")
public class DynamicTransformApplication implements StreamingApplication
{
  private static String COOL_DOWN_MILLIS = "dt.cooldown";
  private static String MAX_THROUGHPUT = "dt.maxThroughput";
  private static String MIN_THROUGHPUT = "dt.minThroughput";

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

    StatelessThroughputBasedPartitioner<TransformOperator> partitioner = new StatelessThroughputBasedPartitioner<>();
    partitioner.setCooldownMillis(conf.getLong(COOL_DOWN_MILLIS, 10000));
    partitioner.setMaximumEvents(conf.getLong(MAX_THROUGHPUT, 30000));
    partitioner.setMinimumEvents(conf.getLong(MIN_THROUGHPUT, 10000));
    dag.setAttribute(transform, Context.OperatorContext.STATS_LISTENERS, Arrays.asList(new StatsListener[]{partitioner}));
    dag.setAttribute(transform, Context.OperatorContext.PARTITIONER, partitioner);
  }
}
