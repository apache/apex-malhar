/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.lib.dedup;

import java.io.IOException;
import java.util.Date;
import java.util.Random;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.apex.malhar.lib.helper.OperatorContextTestHelper;
import org.apache.apex.malhar.lib.testbench.CollectorTestSink;
import org.apache.apex.malhar.lib.util.TestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.stram.engine.PortContext;

import static org.apache.apex.malhar.lib.helper.OperatorContextTestHelper.mockOperatorContext;

public class DeduperBoundedPOJOImplTest
{
  private static String applicationPath;
  private static final String APPLICATION_PATH_PREFIX = "target/DeduperBoundedPOJOImplTest";
  private static final String APP_ID = "DeduperBoundedPOJOImplTest";
  private static final int OPERATOR_ID = 0;
  private static BoundedDedupOperator deduper;
  private static final int NUM_BUCKETS = 10;

  @Before
  public void setup()
  {
    applicationPath = OperatorContextTestHelper.getUniqueApplicationPath(APPLICATION_PATH_PREFIX);
    deduper = new BoundedDedupOperator();
    deduper.setKeyExpression("key");
    deduper.setNumBuckets(NUM_BUCKETS);
  }

  @Test
  public void testDedup()
  {
    com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap attributes =
        new com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap();
    attributes.put(DAG.APPLICATION_ID, APP_ID);
    attributes.put(DAG.APPLICATION_PATH, applicationPath);
    attributes.put(DAG.InputPortMeta.TUPLE_CLASS, TestPojo.class);
    OperatorContext context = mockOperatorContext(OPERATOR_ID, attributes);
    deduper.setup(context);
    deduper.input.setup(new PortContext(attributes, context));
    deduper.activate(context);
    CollectorTestSink<TestPojo> uniqueSink = new CollectorTestSink<TestPojo>();
    TestUtils.setSink(deduper.unique, uniqueSink);
    CollectorTestSink<TestPojo> duplicateSink = new CollectorTestSink<TestPojo>();
    TestUtils.setSink(deduper.duplicate, duplicateSink);
    CollectorTestSink<TestPojo> expiredSink = new CollectorTestSink<TestPojo>();
    TestUtils.setSink(deduper.expired, expiredSink);

    deduper.beginWindow(0);
    Random r = new Random();
    int k = 1;
    for (int i = 1; i <= 1000; i++) {
      TestPojo pojo = new TestPojo(i, new Date(), k++);
      deduper.input.process(pojo);
      if (i % 10 == 0) {
        int dupId = r.nextInt(i);
        TestPojo pojoDuplicate = new TestPojo(dupId == 0 ? 1 : dupId, new Date(), k++);
        deduper.input.process(pojoDuplicate);
      }
    }
    deduper.handleIdleTime();
    deduper.endWindow();

    Assert.assertTrue(uniqueSink.collectedTuples.size() == 1000);
    Assert.assertTrue(duplicateSink.collectedTuples.size() == 100);

    deduper.teardown();
  }

  @After
  public void teardown()
  {
    Path root = new Path(applicationPath);
    try {
      FileSystem fs = FileSystem.newInstance(root.toUri(), new Configuration());
      fs.delete(root, true);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
