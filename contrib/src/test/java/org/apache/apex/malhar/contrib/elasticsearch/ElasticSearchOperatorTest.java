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
package org.apache.apex.malhar.contrib.elasticsearch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.index.query.FilterBuilders;
import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.api.Sink;

/**
 * Unit test for ElasticSearch operator. It assumes that you have running instance of elasticsearch on localhost:9300
 */
public class ElasticSearchOperatorTest
{

  protected static final String ID_FIELD = "id";
  protected static final String INDEX_NAME = "dt-test";
  protected static final String TYPE = "testMessages";
  private static final String VALUE = "value";
  private static final String WINDOW_ID = "Window ID";
  private static final String POST_DATE = "post_date";

  ElasticSearchConnectable createStore()
  {
    ElasticSearchConnectable store = new ElasticSearchConnectable();
    store.setHostName("localhost");
    store.setPort(9300);
    return store;
  }

  @Test
  public void testReadAfterWrite()
  {
    try {
      long testStartTime = System.currentTimeMillis();
      List<String> writtenTupleIDs = writeToES();

      try {
        // Wait for 1 sec to give time for elasticSearch to make
        // inserted
        // data available through NRT
        Thread.sleep(1 * 1000);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      List<String> tupleIDsRead = readFromES(writtenTupleIDs, testStartTime);

      Collections.sort(writtenTupleIDs);
      Collections.sort(tupleIDsRead);
      Assert.assertEquals("Data inconsistency in Elastic Search", writtenTupleIDs, tupleIDsRead);
    } catch (NoNodeAvailableException e) {
      //This indicates that elasticsearch is not running on a particular machine.
      //Silently ignore in this case.
    }
  }

  /**
   * @return
   */
  private List<String> writeToES()
  {

    ElasticSearchMapOutputOperator<Map<String, Object>> operator = new ElasticSearchMapOutputOperator<Map<String, Object>>()
    {
      /*
       * (non-Javadoc)
       *
       * @see org.apache.apex.malhar.contrib.elasticsearch. AbstractElasticSearchOutputOperator #processTuple(java.lang.Object)
       */
      @Override
      public void processTuple(Map<String, Object> tuple)
      {
        super.processTuple(tuple);
      }
    };

    List<String> tupleIDs = new ArrayList<String>();

    operator.setIdField(ID_FIELD);
    operator.setIndexName(INDEX_NAME);
    operator.setType(TYPE);
    operator.setStore(createStore());

    operator.setup(null);
    for (int windowId = 1; windowId <= 5; windowId++) {
      operator.beginWindow(windowId);
      for (int i = 0; i < 3; i++) {
        Map<String, Object> tuple = new HashMap<String, Object>();
        tuple.put(VALUE, i);
        tuple.put(WINDOW_ID, windowId);
        long time = System.currentTimeMillis();
        String id = UUID.randomUUID().toString();
        tupleIDs.add(id);
        tuple.put(ID_FIELD, id);
        tuple.put(POST_DATE, time);
        operator.processTuple(tuple);
      }
      operator.endWindow();
    }
    operator.teardown();
    return tupleIDs;
  }

  /**
   * Read data written to elastic search
   *
   * @param tupleIDs
   * @param testStartTime
   */
  private List<String> readFromES(List<String> writtenTupleIDs, final long testStartTime)
  {
    ElasticSearchMapInputOperator<Map<String, Object>> operator = new ElasticSearchMapInputOperator<Map<String, Object>>()
    {
      /**
       * Set SearchRequestBuilder parameters specific to current window.
       *
       * @see org.apache.apex.malhar.contrib.elasticsearch.ElasticSearchMapInputOperator#getSearchRequestBuilder()
       */
      @Override
      protected SearchRequestBuilder getSearchRequestBuilder()
      {
        long time = System.currentTimeMillis();
        return searchRequestBuilder.setPostFilter(FilterBuilders.rangeFilter(POST_DATE).from(testStartTime).to(time)) // Filter
            .setSize(15).setExplain(false);
      }
    };

    operator.setIndexName(INDEX_NAME);
    operator.setType(TYPE);
    operator.setStore(createStore());

    final List<String> tupleIDsRead = new ArrayList<String>();

    Sink sink = new Sink<Map<String, Object>>()
    {
      @Override
      public void put(Map<String, Object> tuple)
      {
        tupleIDsRead.add(tuple.get(ID_FIELD).toString());
      }

      @Override
      public int getCount(boolean reset)
      {
        return 0;
      }
    };
    operator.outputPort.setSink(sink);

    operator.setup(null);
    for (int windowId = 1; windowId <= 1; windowId++) {
      operator.beginWindow(windowId);
      operator.emitTuples();
      operator.endWindow();
    }

    operator.teardown();
    return tupleIDsRead;
  }

}
