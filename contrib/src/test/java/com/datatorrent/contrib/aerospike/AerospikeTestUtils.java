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
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.datatorrent.api.Attribute.AttributeMap;
import com.datatorrent.api.DAG;
import com.datatorrent.lib.helper.OperatorContextTestHelper;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility class encapsulating code used by several tests
 */
public class AerospikeTestUtils {
  public static final String NODE = "127.0.0.1";

  public static final String NAMESPACE = "test";
  public static final int PORT = 3000;

  public static final String SET_NAME = "test_event_set";
  public static final int OPERATOR_ID = 0;
  public static final int NUM_TUPLES = 10;

  // removes all records from set SET_NAME in namespace NAMESPACE
  static void cleanTable() {

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

  // removes all records from set AerospikeTransactionalStore.DEFAULT_META_SET (used to store
  // committed window ids) in namespace NAMESPACE
  //
  static void cleanMetaTable() {

    try {
      AerospikeClient client = new AerospikeClient(NODE, PORT);

      Statement stmnt = new Statement();
      stmnt.setNamespace(NAMESPACE);
      stmnt.setSetName(AerospikeTransactionalStore.DEFAULT_META_SET);

      RecordSet rs = client.query(null, stmnt);
      while(rs.next()){
        client.delete(null, rs.getKey());
      }
    }
    catch (AerospikeException e) {
      throw new RuntimeException(e);
    }
  }

  // returns the number of records in set SET_NAME in namespace NAMESPACE
  static long getNumOfEventsInStore() {
    try {
      long count = 0;
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

  static AerospikeStore getStore()
  {
    AerospikeStore result = new AerospikeStore();
    result.setNode(NODE);
    result.setPort(PORT);
    //result.setNamespace(NAMESPACE);    // add if needed
    return result;
  }

  static AerospikeTransactionalStore getTransactionalStore()
  {
    AerospikeTransactionalStore result = new AerospikeTransactionalStore();
    result.setNode(NODE);
    result.setPort(PORT);
    result.setNamespace(NAMESPACE);    // used by AerospikeTransactionalStore.createIndexes()
    return result;
  }

  static OperatorContextTestHelper.TestIdOperatorContext getOperatorContext(final String app_id)
  {
    AttributeMap.DefaultAttributeMap attributeMap = new AttributeMap.DefaultAttributeMap();
    attributeMap.put(DAG.APPLICATION_ID, app_id);
    return new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID, attributeMap);
  }

  static ArrayList<String> getExpressions()
  {
    ArrayList<String> result = new ArrayList<String>();
    result.add("getKey()");
    result.add("getBins()");
    return result;
  }

  static List<TestPOJO> getEvents()
  {
    List<TestPOJO> result = new ArrayList<TestPOJO>();
    for (int i = 0; i < NUM_TUPLES; i++) {
      result.add(new TestPOJO(i));
    }
    return result;
  }

  // needs to be public for PojoUtils to work
  public static class TestPOJO {

    int id;

    TestPOJO(int id) {
      this.id = id;
    }

    public Key getKey() {
      try {
        Key key = new Key(NAMESPACE, SET_NAME, String.valueOf(id));
        return key;
      }
      catch (AerospikeException e) {
        throw new RuntimeException("getKey failed: ", e);
      }
    }

    public List<Bin> getBins() {
      List<Bin> list = new ArrayList<Bin>();
      list.add(new Bin("ID",id));
      return list;
    }
  }

}
