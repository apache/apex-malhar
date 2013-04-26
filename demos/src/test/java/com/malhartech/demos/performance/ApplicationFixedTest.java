/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.demos.performance;

import com.malhartech.api.Context.PortContext;
import com.malhartech.api.DAG;
import com.malhartech.stram.StramLocalCluster;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the DAG declaration in local mode.
 */
public class ApplicationFixedTest
{
  @Test
  public void testApplication() throws IOException, Exception
  {
    ApplicationFixed app = new ApplicationFixed();
    DAG dag = cloneDAG(app.getApplication(new Configuration(false)));

    FixedTuplesInputOperator wordGenerator = (FixedTuplesInputOperator)dag.getOperatorMeta("WordGenerator").getOperator();
    Assert.assertEquals("Queue Capacity", ApplicationFixed.QUEUE_CAPACITY, dag.getOperatorMeta(wordGenerator).getOutputPortMeta(wordGenerator.output).getAttributes().attr(PortContext.QUEUE_CAPACITY));
    
    final StramLocalCluster lc = new StramLocalCluster(dag);
    lc.run(60000);
  }

  public DAG cloneDAG(DAG dag) throws Exception
  {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DAG.write(dag, bos);
    bos.flush();
    logger.debug("serialized size: {}", bos.toByteArray().length);
    ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
    DAG dagClone = DAG.read(bis);
    return dagClone;
  }

  private static final Logger logger = LoggerFactory.getLogger(ApplicationFixedTest.class);
}
