/**
 * Put your copyright and license info here.
 */
package com.datatorrent.ada;


import org.apache.hadoop.conf.Configuration;


import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

@ApplicationAnnotation(name="ADA")
public class ADA implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    //WebSocketServerInputOperator wssio = dag.addOperator("console", new WebSocketServerInputOperator());
  }
}
