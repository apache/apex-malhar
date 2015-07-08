/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.contrib.couchbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;

import com.couchbase.client.protocol.views.DesignDocument;
import com.couchbase.client.protocol.views.ViewDesign;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.testbench.CollectorTestSink;

import com.datatorrent.api.Attribute.AttributeMap;
import com.datatorrent.api.DAG;

import com.datatorrent.netlet.util.DTThrowable;

public class CouchBasePOJOTest
{
  private static final Logger logger = LoggerFactory.getLogger(CouchBaseInputOperatorTest.class);
  private static final String APP_ID = "CouchBaseInputOperatorTest";
  private static final String bucket = "default";
  private static final String password = "";
  private static final int OPERATOR_ID = 0;
  protected static ArrayList<String> keyList;
  private static final String uri = "localhost:8091";
  private static final String DESIGN_DOC_ID1 = "dev_test1";
  private static final String TEST_VIEW1 = "testView1";

  @Test
  public void TestCouchBaseInputOperator()
  {
    CouchBaseWindowStore store = new CouchBaseWindowStore();
    System.setProperty("viewmode", "development");
    keyList = new ArrayList<String>();
    store.setBucket(bucket);
    store.setPassword(password);
    store.setUriString(uri);
    try {
      store.connect();
    }
    catch (IOException ex) {
      DTThrowable.rethrow(ex);
    }
    store.getInstance().flush();

    AttributeMap.DefaultAttributeMap attributeMap = new AttributeMap.DefaultAttributeMap();
    attributeMap.put(DAG.APPLICATION_ID, APP_ID);
    OperatorContextTestHelper.TestIdOperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID, attributeMap);
    TestInputOperator inputOperator = new TestInputOperator();
    inputOperator.setStore(store);
    inputOperator.setOutputClass("com.datatorrent.contrib.couchbase.TestComplexPojoInput");
    inputOperator.insertEventsInTable(2);
    try {
      Thread.sleep(10000);
    }
    catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }
    inputOperator.createAndFetchViewQuery1();

    try {
      Thread.sleep(1000);
    }
    catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }
    CollectorTestSink<Object> sink = new CollectorTestSink<Object>();
    inputOperator.outputPort.setSink(sink);

    inputOperator.setup(context);
    inputOperator.setDesignDocumentName(DESIGN_DOC_ID1);
    inputOperator.setViewName(TEST_VIEW1);
    inputOperator.beginWindow(0);
    inputOperator.emitTuples();
    inputOperator.endWindow();

    logger.debug("collected tuples are {}", sink.collectedTuples.size());

    int count = 0;
    for (Object o: sink.collectedTuples) {
      count++;
      TestComplexPojoInput object = (TestComplexPojoInput)o;
      if (count == 1) {
        Assert.assertEquals("name set in testpojo", "test", object.getName());
        Assert.assertEquals("map in testpojo", "{test=12345}", object.getMap().toString());
        Assert.assertEquals("age in testpojo", "23", object.getAge().toString());
      }
      if (count == 2) {
        Assert.assertEquals("name set in testpojo", "test1", object.getName());
        Assert.assertEquals("map in testpojo", "{test2=12345}", object.getMap().toString());
        Assert.assertEquals("age in testpojo", "12", object.getAge().toString());
      }
    }
    sink.clear();
    store.client.deleteDesignDoc(DESIGN_DOC_ID1);
    inputOperator.teardown();
  }

  public static class TestInputOperator extends CouchBasePOJOInputOperator
  {

    private void insertEventsInTable(int numEvents)
    {
      logger.info("number of events is" + numEvents);
      try {
        store.client.set("Key1", 431);
        store.client.set("Key2", "{\"name\":\"test\",\"map\":{\"test\":12345},\"age\":23}").get();
        store.client.set("Key3", "{\"name\":\"test1\",\"map\":{\"test2\":12345},\"age\":12}").get();
      }
      catch (InterruptedException ex) {
        DTThrowable.rethrow(ex);
      }
      catch (ExecutionException ex) {
        DTThrowable.rethrow(ex);
      }

    }

    public void createAndFetchViewQuery1()
    {
      DesignDocument designDoc = new DesignDocument(DESIGN_DOC_ID1);
      String viewName = TEST_VIEW1;
      String mapFunction
              = "function (doc, meta) {\n"
              + "  if( meta.type == \"json\") {\n"
              + "    emit(doc.key,doc);\n"
              + "  }\n"
              + " }";

      ViewDesign viewDesign = new ViewDesign(viewName, mapFunction);
      designDoc.getViews().add(viewDesign);
      store.client.createDesignDoc(designDoc);
    }

  }

}
