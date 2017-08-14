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
package org.apache.apex.malhar.lib.db.jdbc;

import java.io.File;
import java.io.IOException;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.apache.apex.malhar.lib.db.jdbc.AbstractJdbcPollInputOperator.WindowData;
import org.apache.apex.malhar.lib.helper.TestPortContext;
import org.apache.apex.malhar.lib.testbench.CollectorTestSink;
import org.apache.apex.malhar.lib.util.FieldInfo;
import org.apache.apex.malhar.lib.util.KeyValPair;
import org.apache.apex.malhar.lib.wal.WindowDataManager;
import org.apache.commons.io.FileUtils;

import com.google.common.collect.Lists;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Partitioner;

import static org.apache.apex.malhar.lib.helper.OperatorContextTestHelper.mockOperatorContext;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class JdbcPojoPollableOpeartorTest extends JdbcOperatorTest
{
  public String dir = null;
  @Mock
  private ScheduledExecutorService mockscheduler;
  @Mock
  private ScheduledFuture futureTaskMock;
  @Mock
  private WindowDataManager windowDataManagerMock;

  @Before
  public void beforeTest() throws IOException
  {
    dir = "target/" + APP_ID + "/";
    FileUtils.deleteDirectory(new File(dir));

    MockitoAnnotations.initMocks(this);
    when(mockscheduler.scheduleWithFixedDelay(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class)))
        .thenReturn(futureTaskMock);
  }

  @After
  public void afterTest() throws IOException
  {
    cleanTable();
    FileUtils.deleteDirectory(new File(dir));
  }

  @Test
  public void testDBPoller() throws Exception
  {
    insertEvents(10, true, 0);

    JdbcStore store = new JdbcStore();
    store.setDatabaseDriver(DB_DRIVER);
    store.setDatabaseUrl(URL);

    List<FieldInfo> fieldInfos = getFieldInfos();

    Attribute.AttributeMap.DefaultAttributeMap portAttributes = new Attribute.AttributeMap.DefaultAttributeMap();
    portAttributes.put(Context.PortContext.TUPLE_CLASS, TestPOJOEvent.class);
    TestPortContext tpc = new TestPortContext(portAttributes);

    JdbcPOJOPollInputOperator inputOperator = new JdbcPOJOPollInputOperator();
    inputOperator.setStore(store);
    inputOperator.setTableName(TABLE_POJO_NAME);
    inputOperator.setKey("id");
    inputOperator.setFieldInfos(fieldInfos);
    inputOperator.setFetchSize(100);
    inputOperator.setBatchSize(100);
    inputOperator.setPartitionCount(2);

    Collection<com.datatorrent.api.Partitioner.Partition<AbstractJdbcPollInputOperator<Object>>> newPartitions = inputOperator
        .definePartitions(new ArrayList<Partitioner.Partition<AbstractJdbcPollInputOperator<Object>>>(), null);

    int operatorId = 0;
    for (com.datatorrent.api.Partitioner.Partition<AbstractJdbcPollInputOperator<Object>> partition : newPartitions) {

      Attribute.AttributeMap.DefaultAttributeMap partitionAttributeMap = new Attribute.AttributeMap.DefaultAttributeMap();
      partitionAttributeMap.put(DAG.APPLICATION_ID, APP_ID);
      partitionAttributeMap.put(Context.DAGContext.APPLICATION_PATH, dir);

      OperatorContext partitioningContext = mockOperatorContext(operatorId++, partitionAttributeMap);

      JdbcPOJOPollInputOperator parition = (JdbcPOJOPollInputOperator)partition.getPartitionedInstance();
      parition.outputPort.setup(tpc);
      parition.setScheduledExecutorService(mockscheduler);
      parition.setup(partitioningContext);
      parition.activate(partitioningContext);
    }

    Iterator<com.datatorrent.api.Partitioner.Partition<AbstractJdbcPollInputOperator<Object>>> itr = newPartitions
        .iterator();
    // First partition is for range queries,last is for polling queries
    JdbcPOJOPollInputOperator firstInstance = (JdbcPOJOPollInputOperator)itr.next().getPartitionedInstance();
    CollectorTestSink<Object> sink1 = new CollectorTestSink<>();
    firstInstance.outputPort.setSink(sink1);
    firstInstance.beginWindow(0);
    firstInstance.pollRecords();
    try {
      firstInstance.pollRecords();
      // non-poller partition
      Assert.fail("expected closed connection");
    } catch (Exception e) {
      // expected
    }
    firstInstance.emitTuples();
    firstInstance.endWindow();

    Assert.assertEquals("rows from db", 5, sink1.collectedTuples.size());
    for (Object tuple : sink1.collectedTuples) {
      TestPOJOEvent pojoEvent = (TestPOJOEvent)tuple;
      Assert.assertTrue("date", pojoEvent.getStartDate() instanceof Date);
      Assert.assertTrue("date", pojoEvent.getId() < 5);
    }

    JdbcPOJOPollInputOperator secondInstance = (JdbcPOJOPollInputOperator)itr.next().getPartitionedInstance();
    CollectorTestSink<Object> sink2 = new CollectorTestSink<>();
    secondInstance.outputPort.setSink(sink2);
    secondInstance.beginWindow(0);
    secondInstance.pollRecords();
    secondInstance.emitTuples();
    secondInstance.endWindow();

    Assert.assertEquals("rows from db", 5, sink2.collectedTuples.size());
    for (Object tuple : sink2.collectedTuples) {
      TestPOJOEvent pojoEvent = (TestPOJOEvent)tuple;
      Assert.assertTrue("date", pojoEvent.getId() < 10 && pojoEvent.getId() >= 5);
    }

    insertEvents(4, false, 10);
    JdbcPOJOPollInputOperator thirdInstance = (JdbcPOJOPollInputOperator)itr.next().getPartitionedInstance();
    CollectorTestSink<Object> sink3 = new CollectorTestSink<>();
    thirdInstance.outputPort.setSink(sink3);
    thirdInstance.beginWindow(0);
    thirdInstance.pollRecords();
    thirdInstance.pollRecords();
    thirdInstance.emitTuples();
    thirdInstance.endWindow();

    Assert.assertEquals("rows from db", 4, sink3.collectedTuples.size());
  }

  @Test
  public void testRecovery() throws IOException
  {
    int operatorId = 1;
    when(windowDataManagerMock.getLargestCompletedWindow()).thenReturn(1L);
    when(windowDataManagerMock.retrieve(1)).thenReturn(WindowData.of(null, 0, 4));

    insertEvents(10, true, 0);

    JdbcStore store = new JdbcStore();
    store.setDatabaseDriver(DB_DRIVER);
    store.setDatabaseUrl(URL);

    List<FieldInfo> fieldInfos = getFieldInfos();

    Attribute.AttributeMap.DefaultAttributeMap portAttributes = new Attribute.AttributeMap.DefaultAttributeMap();
    portAttributes.put(Context.PortContext.TUPLE_CLASS, TestPOJOEvent.class);
    TestPortContext tpc = new TestPortContext(portAttributes);

    Attribute.AttributeMap.DefaultAttributeMap partitionAttributeMap = new Attribute.AttributeMap.DefaultAttributeMap();
    partitionAttributeMap.put(DAG.APPLICATION_ID, APP_ID);
    partitionAttributeMap.put(Context.DAGContext.APPLICATION_PATH, dir);

    OperatorContext context = mockOperatorContext(operatorId, partitionAttributeMap);

    JdbcPOJOPollInputOperator inputOperator = new JdbcPOJOPollInputOperator();
    inputOperator.setStore(store);
    inputOperator.setTableName(TABLE_POJO_NAME);
    inputOperator.setKey("id");
    inputOperator.setFieldInfos(fieldInfos);
    inputOperator.setFetchSize(100);
    inputOperator.setBatchSize(100);
    inputOperator.lastEmittedRow = 0; //setting as not calling partition logic
    inputOperator.isPollerPartition = true;
    inputOperator.rangeQueryPair = new KeyValPair<>(0, 8);

    inputOperator.outputPort.setup(tpc);
    inputOperator.setScheduledExecutorService(mockscheduler);
    inputOperator.setup(context);
    inputOperator.setWindowManager(windowDataManagerMock);
    inputOperator.activate(context);

    CollectorTestSink<Object> sink = new CollectorTestSink<>();
    inputOperator.outputPort.setSink(sink);
    inputOperator.beginWindow(0);
    verify(mockscheduler, times(0)).scheduleAtFixedRate(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class));
    verify(mockscheduler, times(0)).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
    inputOperator.emitTuples();
    inputOperator.endWindow();
    inputOperator.beginWindow(1);
    verify(mockscheduler, times(1)).scheduleAtFixedRate(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class));
    verify(mockscheduler, times(0)).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));

  }

  @Test
  public void testDBPollerExtraField() throws Exception
  {
    insertEvents(10, true, 0);

    JdbcStore store = new JdbcStore();
    store.setDatabaseDriver(DB_DRIVER);
    store.setDatabaseUrl(URL);

    List<FieldInfo> fieldInfos = getFieldInfos();

    Attribute.AttributeMap.DefaultAttributeMap portAttributes = new Attribute.AttributeMap.DefaultAttributeMap();
    portAttributes.put(Context.PortContext.TUPLE_CLASS, TestPOJOEvent.class);
    TestPortContext tpc = new TestPortContext(portAttributes);

    JdbcPOJOPollInputOperator inputOperator = new JdbcPOJOPollInputOperator();
    inputOperator.setStore(store);
    inputOperator.setTableName(TABLE_POJO_NAME);
    inputOperator.setColumnsExpression("ID,STARTDATE,STARTTIME,STARTTIMESTAMP");
    inputOperator.setKey("id");
    inputOperator.setFieldInfos(fieldInfos);
    inputOperator.setFetchSize(100);
    inputOperator.setBatchSize(100);
    inputOperator.setPartitionCount(2);

    Collection<com.datatorrent.api.Partitioner.Partition<AbstractJdbcPollInputOperator<Object>>> newPartitions = inputOperator
        .definePartitions(new ArrayList<Partitioner.Partition<AbstractJdbcPollInputOperator<Object>>>(), null);

    int operatorId = 0;
    for (com.datatorrent.api.Partitioner.Partition<AbstractJdbcPollInputOperator<Object>> partition : newPartitions) {

      Attribute.AttributeMap.DefaultAttributeMap partitionAttributeMap = new Attribute.AttributeMap.DefaultAttributeMap();
      partitionAttributeMap.put(DAG.APPLICATION_ID, APP_ID);
      partitionAttributeMap.put(Context.DAGContext.APPLICATION_PATH, dir);

      OperatorContext partitioningContext = mockOperatorContext(operatorId++, partitionAttributeMap);

      JdbcPOJOPollInputOperator parition = (JdbcPOJOPollInputOperator)partition.getPartitionedInstance();
      parition.outputPort.setup(tpc);
      parition.setScheduledExecutorService(mockscheduler);
      parition.setup(partitioningContext);
      parition.activate(partitioningContext);
    }

    Iterator<com.datatorrent.api.Partitioner.Partition<AbstractJdbcPollInputOperator<Object>>> itr = newPartitions
        .iterator();
    // First partition is for range queries,last is for polling queries
    JdbcPOJOPollInputOperator firstInstance = (JdbcPOJOPollInputOperator)itr.next().getPartitionedInstance();
    CollectorTestSink<Object> sink1 = new CollectorTestSink<>();
    firstInstance.outputPort.setSink(sink1);
    firstInstance.beginWindow(0);
    Assert.assertFalse(firstInstance.ps.isClosed());
    firstInstance.pollRecords();
    Assert.assertTrue(firstInstance.ps.isClosed());
    firstInstance.emitTuples();
    firstInstance.endWindow();

    Assert.assertEquals("rows from db", 5, sink1.collectedTuples.size());
    for (Object tuple : sink1.collectedTuples) {
      TestPOJOEvent pojoEvent = (TestPOJOEvent)tuple;
      Assert.assertTrue("date", pojoEvent.getStartDate() instanceof Date);
      Assert.assertTrue("date", pojoEvent.getId() < 5);
    }

    JdbcPOJOPollInputOperator secondInstance = (JdbcPOJOPollInputOperator)itr.next().getPartitionedInstance();
    CollectorTestSink<Object> sink2 = new CollectorTestSink<>();
    secondInstance.outputPort.setSink(sink2);
    secondInstance.beginWindow(0);
    secondInstance.pollRecords();
    secondInstance.emitTuples();
    secondInstance.endWindow();

    Assert.assertEquals("rows from db", 5, sink2.collectedTuples.size());
    for (Object tuple : sink2.collectedTuples) {
      TestPOJOEvent pojoEvent = (TestPOJOEvent)tuple;
      Assert.assertTrue("date", pojoEvent.getId() < 10 && pojoEvent.getId() >= 5);
    }

    insertEvents(4, false, 10);
    JdbcPOJOPollInputOperator thirdInstance = (JdbcPOJOPollInputOperator)itr.next().getPartitionedInstance();
    CollectorTestSink<Object> sink3 = new CollectorTestSink<>();
    thirdInstance.outputPort.setSink(sink3);
    thirdInstance.beginWindow(0);
    thirdInstance.pollRecords();
    thirdInstance.emitTuples();
    thirdInstance.endWindow();

    Assert.assertEquals("rows from db", 4, sink3.collectedTuples.size());
  }

  private List<FieldInfo> getFieldInfos()
  {
    List<FieldInfo> fieldInfos = Lists.newArrayList();
    fieldInfos.add(new FieldInfo("ID", "id", null));
    fieldInfos.add(new FieldInfo("STARTDATE", "startDate", null));
    fieldInfos.add(new FieldInfo("STARTTIME", "startTime", null));
    fieldInfos.add(new FieldInfo("STARTTIMESTAMP", "startTimestamp", null));
    fieldInfos.add(new FieldInfo("NAME", "name", null));
    return fieldInfos;
  }

  @Test
  public void testPollWithOffsetRebase() throws Exception
  {
    insertEvents(0, true, 0); // clear table

    JdbcStore store = new JdbcStore();
    store.setDatabaseDriver(DB_DRIVER);
    store.setDatabaseUrl(URL);

    List<FieldInfo> fieldInfos = getFieldInfos();

    Attribute.AttributeMap.DefaultAttributeMap portAttributes = new Attribute.AttributeMap.DefaultAttributeMap();
    portAttributes.put(Context.PortContext.TUPLE_CLASS, TestPOJOEvent.class);
    TestPortContext tpc = new TestPortContext(portAttributes);

    JdbcPOJOPollInputOperator inputOperator = new JdbcPOJOPollInputOperator();
    inputOperator.setStore(store);
    inputOperator.setTableName(TABLE_POJO_NAME);
    inputOperator.setColumnsExpression("ID,STARTDATE,STARTTIME,STARTTIMESTAMP");
    inputOperator.setKey("id");
    inputOperator.setFieldInfos(fieldInfos);
    inputOperator.setFetchSize(100);
    inputOperator.setBatchSize(100);
    inputOperator.setPartitionCount(1);
    inputOperator.setRebaseOffset(true);

    Collection<com.datatorrent.api.Partitioner.Partition<AbstractJdbcPollInputOperator<Object>>> newPartitions = inputOperator
        .definePartitions(new ArrayList<Partitioner.Partition<AbstractJdbcPollInputOperator<Object>>>(), null);

    int operatorId = 0;
    for (com.datatorrent.api.Partitioner.Partition<AbstractJdbcPollInputOperator<Object>> partition : newPartitions) {

      Attribute.AttributeMap.DefaultAttributeMap partitionAttributeMap = new Attribute.AttributeMap.DefaultAttributeMap();
      partitionAttributeMap.put(DAG.APPLICATION_ID, APP_ID);
      partitionAttributeMap.put(Context.DAGContext.APPLICATION_PATH, dir);

      OperatorContext partitioningContext = mockOperatorContext(operatorId++, partitionAttributeMap);

      JdbcPOJOPollInputOperator parition = (JdbcPOJOPollInputOperator)partition.getPartitionedInstance();
      parition.outputPort.setup(tpc);
      parition.setScheduledExecutorService(mockscheduler);
      parition.setup(partitioningContext);
      parition.activate(partitioningContext);
    }

    Iterator<com.datatorrent.api.Partitioner.Partition<AbstractJdbcPollInputOperator<Object>>> itr = newPartitions
        .iterator();
    // First partition is for range queries,last is for polling queries
    JdbcPOJOPollInputOperator firstInstance = (JdbcPOJOPollInputOperator)itr.next().getPartitionedInstance();

    int rows = 0;
    int windowId = 0;
    insertEvents(4, false, rows);
    rows += 4;
    JdbcPOJOPollInputOperator poller = (JdbcPOJOPollInputOperator)itr.next().getPartitionedInstance();
    CollectorTestSink<Object> sink3 = new CollectorTestSink<>();
    poller.outputPort.setSink(sink3);
    poller.beginWindow(windowId++);
    poller.pollRecords();
    poller.emitTuples();
    Assert.assertEquals("emitted", rows, sink3.collectedTuples.size());
    poller.endWindow();

    insertEvents(1, false, rows);
    rows += 1;
    poller.beginWindow(windowId++);
    poller.pollRecords(); // offset rebase, fetch 1 record
    poller.emitTuples();
    Assert.assertEquals("emitted", rows, sink3.collectedTuples.size());
    poller.endWindow();

  }

}
