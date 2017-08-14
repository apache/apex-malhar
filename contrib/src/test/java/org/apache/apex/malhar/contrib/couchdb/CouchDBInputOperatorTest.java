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
package org.apache.apex.malhar.contrib.couchdb;

import java.util.Map;

import org.ektorp.ViewQuery;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.apex.malhar.lib.testbench.CollectorTestSink;

import static org.apache.apex.malhar.lib.helper.OperatorContextTestHelper.mockOperatorContext;

import com.google.common.collect.Maps;


/**
 * @since 0.3.5
 */
public class CouchDBInputOperatorTest
{
  private static Logger LOG = LoggerFactory.getLogger(CouchDBInputOperatorTest.class);

  private class TestMapBasedCouchInputOperatorTest extends AbstractMapBasedInputOperator
  {
    @Override
    public ViewQuery getViewQuery()
    {
      return CouchDBTestHelper.createAndFetchViewQuery();
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Test
  public void testCouchDBInputOperator()
  {
    String testDocumentId = "TestDocument1";
    Map<String, String> mapTuple = Maps.newHashMap();
    mapTuple.put("_id", testDocumentId);
    mapTuple.put("name", "TD1");
    mapTuple.put("type", "test");
    CouchDBTestHelper.insertDocument(mapTuple);

    TestMapBasedCouchInputOperatorTest operatorTest = new TestMapBasedCouchInputOperatorTest();
    CouchDbStore store = new CouchDbStore();
    store.setDbName(CouchDBTestHelper.TEST_DB);
    operatorTest.setStore(store);

    CollectorTestSink sink = new CollectorTestSink();
    operatorTest.outputPort.setSink(sink);
    operatorTest.setup(mockOperatorContext(2));

    operatorTest.beginWindow(0);
    operatorTest.emitTuples();
    operatorTest.endWindow();
    Assert.assertTrue("number emitted tuples", sink.collectedTuples.size() > 0);

    boolean found = false;
    for (Object o : sink.collectedTuples) {
      LOG.debug(o.toString());
      Map<Object, Object> document = (Map<Object, Object>)o;
      if (document.get("_id").equals(testDocumentId)) {
        found = true;
        Assert.assertEquals("name in document", "TD1", document.get("name"));
        Assert.assertEquals("type of ducement", "test", document.get("type"));
      }
    }
    Assert.assertTrue("inserted tuple was found", found);
  }

  @BeforeClass
  public static void setup()
  {
    CouchDBTestHelper.setup();
  }

  @AfterClass
  public static void teardown()
  {
    CouchDBTestHelper.teardown();
  }
}
