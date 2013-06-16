/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.demos.performance;

import com.malhartech.api.ApplicationFactory;
import com.malhartech.api.DAG;
import com.malhartech.lib.stream.DevNullCounter;
import com.malhartech.lib.testbench.EventGenerator;

import org.apache.hadoop.conf.Configuration;

/**
 * Example of application configuration in Java.<p>
 */
public class ApplicationEventGenerator implements ApplicationFactory
{
  private static final boolean inline = true;


  public EventGenerator getLoadGenerator(String name, DAG dag) {
    EventGenerator oper = dag.addOperator(name, EventGenerator.class);
    int numchars = 1024;
    char[] chararray = new char[numchars + 1];
    for (int i = 0; i < numchars; i++) {
      chararray[i] = 'a';
    }
    chararray[numchars] = '\0';
    String key = new String(chararray);
    oper.setKeys(key);
    oper.setTuplesBlast(1000);
    oper.setRollingWindowCount(10);

//    oper.setProperty("spinMillis", "2");
//    int i = 10 * 1024 * 1024;
//    String ival = Integer.toString(i);
//    oper.setProperty("bufferCapacity", ival);
    return oper;
  }

  public DevNullCounter getDevNull(String name, DAG dag)
  {
    return dag.addOperator(name, DevNullCounter.class);
  }

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    dag.getAttributes().attr(DAG.CHECKPOINT_WINDOW_COUNT).set(0); // disable operator state saving
    EventGenerator lgen = getLoadGenerator("lgen", dag);
    DevNullCounter devnull = getDevNull("devnull", dag);
    dag.addStream("lgen2devnull", lgen.string_data, devnull.data).setInline(inline);
  }
}
