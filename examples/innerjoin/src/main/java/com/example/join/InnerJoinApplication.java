package com.example.join;

import org.apache.apex.malhar.lib.join.POJOInnerJoinOperator;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.common.partitioner.StatelessPartitioner;
import com.datatorrent.lib.io.ConsoleOutputOperator;

@ApplicationAnnotation(name="InnerJoinExample")
public class InnerJoinApplication implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    // SalesEvent Generator
    POJOGenerator salesGenerator = dag.addOperator("Input1", new POJOGenerator());
    // ProductEvent Generator
    POJOGenerator productGenerator = dag.addOperator("Input2", new POJOGenerator());
    productGenerator.setSalesEvent(false);

    // Inner join Operator
    POJOInnerJoinOperator join = dag.addOperator("Join", new POJOInnerJoinOperator());
    ConsoleOutputOperator output = dag.addOperator("Output", new ConsoleOutputOperator());

    // Streams
    dag.addStream("SalesToJoin", salesGenerator.output, join.input1);
    dag.addStream("ProductToJoin", productGenerator.output, join.input2);
    dag.addStream("JoinToConsole", join.outputPort, output.input);

    // Setting tuple class properties to the ports of join operator
    dag.setInputPortAttribute(join.input1, Context.PortContext.TUPLE_CLASS, POJOGenerator.SalesEvent.class);
    dag.setInputPortAttribute(join.input2, Context.PortContext.TUPLE_CLASS, POJOGenerator.ProductEvent.class);
    dag.setOutputPortAttribute(join.outputPort,Context.PortContext.TUPLE_CLASS, POJOGenerator.SalesEvent.class);
  }
}
