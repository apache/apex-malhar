/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.demos.performance;

import com.malhartech.api.Context.PortContext;
import com.malhartech.api.DAG;
import com.malhartech.api.LocalMode;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test the DAG declaration in local mode.
 */
public class ApplicationFixedTest
{
  @Test
  public void testApplication() throws IOException, Exception
  {
    LocalMode lma = LocalMode.newInstance();
    new ApplicationFixed().populateDAG(lma.getDAG(), new Configuration(false));

    DAG dag = lma.cloneDAG();
    FixedTuplesInputOperator wordGenerator = (FixedTuplesInputOperator)dag.getOperatorMeta("WordGenerator").getOperator();
    Assert.assertEquals("Queue Capacity", ApplicationFixed.QUEUE_CAPACITY, (int)dag.getMeta(wordGenerator).getMeta(wordGenerator.output).attrValue(PortContext.QUEUE_CAPACITY, 0));

    LocalMode.Controller lc = lma.getController();
    lc.run(60000);
  }
}
