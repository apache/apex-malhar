/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.aerospike;


import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.datatorrent.api.Attribute.AttributeMap;
import com.datatorrent.api.DAG;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import java.util.List;

/**
 * Tests for {@link AbstractAerosipkeTransactionalPutOperator} and {@link AbstractAerospikeGetOperator}
 */
public class AerospikeOperatorTest {

  public static final String NODE = "127.0.0.1";
  public static final String NAMESPACE = "test";
  public static final int PORT = 3000;

  private static final String SET_NAME = "test_event_set";
  private static String APP_ID = "AerospikeOperatorTest";
  private static int OPERATOR_ID = 0;

  private static class TestEvent {

    int id;

    TestEvent(int id) {
      this.id = id;
    }
  }

  private static void cleanTable() {

    try {
      AerospikeClient client = new AerospikeClient(NODE, PORT);

      Statement stmnt = new Statement();
      stmnt.setNamespace(NAMESPACE);
      stmnt.setSetName(SET_NAME);

      RecordSet rs = client.query(null, stmnt);
      while(rs.next()){
        client.delete(null, rs.getKey());
      }
    }
    catch (AerospikeException e) {
      throw new RuntimeException(e);
    }
  }


  private static class TestOutputOperator extends AbstractAerospikeTransactionalPutOperator<TestEvent> {

    TestOutputOperator() {
      cleanTable();
    }

    public long getNumOfEventsInStore() {
      try {
        int count =0;
        AerospikeClient client = new AerospikeClient(NODE, PORT);
        Statement stmnt = new Statement();
        stmnt.setNamespace(NAMESPACE);
        stmnt.setSetName(SET_NAME);

        RecordSet rs = client.query(null, stmnt);
        while(rs.next()){
          count++;
        }
        return count;
      }
      catch (AerospikeException e) {
        throw new RuntimeException("fetching count", e);
      }
    }

    @Override
    protected Key getUpdatedBins(TestEvent tuple, List<Bin> bins)
        throws AerospikeException {
      Key key = new Key(NAMESPACE,SET_NAME,String.valueOf(tuple.id));
      bins.add(new Bin("ID",tuple.id));
      return key;
    }
  }

  private static class TestInputOperator extends AbstractAerospikeGetOperator<TestEvent> {

    TestInputOperator() {
      cleanTable();
    }

    @Override
    public TestEvent getTuple(Record record) {

      return new TestEvent((Integer) record.getValue("ID"));
    }

    @Override
    public Statement queryToRetrieveData() {

      Statement stmnt = new Statement();
      stmnt.setNamespace(NAMESPACE);
      stmnt.setSetName(SET_NAME);

      return stmnt;
    }

    public void insertEventsInTable(int numEvents) {

      try {
        AerospikeClient client = new AerospikeClient(NODE, PORT);
        Key key;
        Bin bin;
        for (int i = 0; i < numEvents; i++) {
          key = new Key(NAMESPACE,SET_NAME,String.valueOf(i));
          bin = new Bin("ID",i);
          client.put(null, key, bin);
        }
      }
      catch (AerospikeException e) {
        throw new RuntimeException(e);
      }
    }

  }

  @Test
  public void TestAerospikeOutputOperator() {

    AerospikeTransactionalStore transactionalStore = new AerospikeTransactionalStore();
    transactionalStore.setNode(NODE);
    transactionalStore.setPort(PORT);
    transactionalStore.setNamespace(NAMESPACE);

    AttributeMap.DefaultAttributeMap attributeMap = new AttributeMap.DefaultAttributeMap();
    attributeMap.put(DAG.APPLICATION_ID, APP_ID);
    OperatorContextTestHelper.TestIdOperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID, attributeMap);

    TestOutputOperator outputOperator = new TestOutputOperator();

    outputOperator.setStore(transactionalStore);

    outputOperator.setup(context);

    List<TestEvent> events = Lists.newArrayList();
    for (int i = 0; i < 10; i++) {
      events.add(new TestEvent(i));
    }

    outputOperator.beginWindow(0);
    for (TestEvent event : events) {
      outputOperator.input.process(event);
    }
    outputOperator.endWindow();

    Assert.assertEquals("rows in db", 10, outputOperator.getNumOfEventsInStore());
  }

  @Test
  public void TestAerospikeInputOperator() {

    AerospikeStore store = new AerospikeStore();
    store.setNode(NODE);
    store.setPort(PORT);

    AttributeMap.DefaultAttributeMap attributeMap = new AttributeMap.DefaultAttributeMap();
    attributeMap.put(DAG.APPLICATION_ID, APP_ID);
    OperatorContextTestHelper.TestIdOperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID, attributeMap);

    TestInputOperator inputOperator = new TestInputOperator();
    inputOperator.setStore(store);
    inputOperator.insertEventsInTable(10);

    CollectorTestSink<Object> sink = new CollectorTestSink<Object>();
    inputOperator.outputPort.setSink(sink);

    inputOperator.setup(context);
    inputOperator.beginWindow(0);
    inputOperator.emitTuples();
    inputOperator.endWindow();

    Assert.assertEquals("rows from db", 10, sink.collectedTuples.size());
  }

}
