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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.apex.malhar.lib.fileaccess.FileAccessFSImpl;
import org.apache.apex.malhar.lib.fileaccess.TFileImpl;
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

public class DeduperTimeBasedPOJOImplTest
{
  private static String applicationPath;
  private static final String APPLICATION_PATH_PREFIX = "target/DeduperPOJOImplTest";
  private static final String APP_ID = "DeduperPOJOImplTest";
  private static final int OPERATOR_ID = 0;
  private static TimeBasedDedupOperator deduper;

  @Before
  public void setup()
  {
    applicationPath = OperatorContextTestHelper.getUniqueApplicationPath(APPLICATION_PATH_PREFIX);
    deduper = new TimeBasedDedupOperator();
    deduper.setKeyExpression("key");
    deduper.setTimeExpression("date.getTime()");
    deduper.setBucketSpan(10);
    deduper.setExpireBefore(60);
    FileAccessFSImpl fAccessImpl = new TFileImpl.DTFileImpl();
    fAccessImpl.setBasePath(applicationPath + "/bucket_data");
    deduper.managedState.setFileAccess(fAccessImpl);
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

    long millis = System.currentTimeMillis();
    for (int i = 0; i < 100; i++) {
      TestPojo pojo = new TestPojo(i, new Date(millis + i));
      deduper.input.process(pojo);
    }
    TestPojo expiredPojo = new TestPojo(100, new Date(millis - 1000 * 60));
    deduper.input.process(expiredPojo);
    for (int i = 90; i < 200; i++) {
      TestPojo pojo = new TestPojo(i, new Date(millis + i));
      deduper.input.process(pojo);
    }
    deduper.handleIdleTime();
    deduper.endWindow();
    Assert.assertTrue(uniqueSink.collectedTuples.size() == 200);
    Assert.assertTrue(duplicateSink.collectedTuples.size() == 10);
    Assert.assertTrue(expiredSink.collectedTuples.size() == 1);

    deduper.teardown();
  }

  @Test
  public void testDedupDifferentWindowSameKey()
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

    long millis = System.currentTimeMillis();
    deduper.input.process( new TestPojo(10, new Date(millis)));
    deduper.input.process( new TestPojo(11, new Date(millis + 10000)));
    deduper.input.process( new TestPojo(12, new Date(millis + 20000)));
    deduper.input.process( new TestPojo(13, new Date(millis + 30000)));
    deduper.input.process( new TestPojo(14, new Date(millis + 40000)));
    deduper.input.process( new TestPojo(15, new Date(millis + 50000)));
    deduper.input.process( new TestPojo(10, new Date(millis))); //Duplicate
    deduper.input.process( new TestPojo(16, new Date(millis + 60000)));
    deduper.input.process( new TestPojo(10, new Date(millis + 70000))); // New tuple with same key but outside expired window.
    deduper.input.process( new TestPojo(10, new Date(millis))); // Earlier tuple with earlier time -- Expired
    deduper.input.process( new TestPojo(10, new Date(millis + 70000))); // New tuple repeated again - Duplicate

    deduper.handleIdleTime();
    deduper.endWindow();

    Assert.assertTrue(uniqueSink.collectedTuples.size() == 8);
    Assert.assertTrue(duplicateSink.collectedTuples.size() == 2);
    Assert.assertTrue(expiredSink.collectedTuples.size() == 1);

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
