/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.couchdb;

import java.util.ArrayList;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.testbench.CollectorTestSink;

public class CouchDBPOJOInputOperatorTest
{

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Test
  public void testCouchDBInputOperator()
  {
    String testDocumentId1 = "TestDocument1";
    String testDocumentId2 = "TestDocument2";
    TestInputPOJO test1 = new TestInputPOJO();
    TestInputPOJO.InnerObj innerObj = new TestInputPOJO.InnerObj();
    innerObj.setIntVal(12);
    test1.setAge(23);
    innerObj.setCharVal('d');
    innerObj.setStringVal("testString1");
    test1.setInnerObj(innerObj);
    test1.setId(testDocumentId1);
    test1.setName("test1");
    TestInputPOJO test2 = new TestInputPOJO();
    innerObj = new TestInputPOJO.InnerObj();
    innerObj.setIntVal(24);
    innerObj.setCharVal('p');
    innerObj.setStringVal("testString2");
    test2.setInnerObj(innerObj);
    test2.setId(testDocumentId2);
    test2.setAge(32);
    test2.setName("test2");
   // In case user can specify class objects.
   // Class<TestInputPOJO.InnerObj> innerObjClass = TestInputPOJO.InnerObj.class;
   // Class<String> stringClass = String.class;
    CouchDBTestHelper.insertDocument(test1);
    CouchDBTestHelper.insertDocument(test2);
    CouchDBPOJOInputOperator operatorTest = new CouchDBPOJOInputOperator();
    CouchDbStore store = new CouchDbStore();
    store.setDbName(CouchDBTestHelper.TEST_DB);
    operatorTest.setStore(store);

    CollectorTestSink sink = new CollectorTestSink();
    operatorTest.setOutputClass("com.datatorrent.contrib.couchdb.TestInputPOJO");
    operatorTest.setExpressionForDocId("id");

    ArrayList<String> expressions = new ArrayList<String>();
    expressions.add("name");
    expressions.add("innerObj");
    expressions.add("age");
    operatorTest.setExpressions(expressions);
    List<String> columns = new ArrayList<String>();
    columns.add("name");
    columns.add("innerObj");
    columns.add("age");

    operatorTest.setColumns(columns);
    CouchDBTestHelper.createAndFetchViewQuery();
    operatorTest.setDesignDocumentName(CouchDBTestHelper.DESIGN_DOC_ID);
    operatorTest.setViewName(CouchDBTestHelper.TEST_VIEW);
    operatorTest.setStartKey(testDocumentId1);
    operatorTest.outputPort.setSink(sink);
    operatorTest.setup(new OperatorContextTestHelper.TestIdOperatorContext(2));

    operatorTest.beginWindow(0);
    operatorTest.emitTuples();
    operatorTest.emitTuples();
    operatorTest.endWindow();
    Assert.assertEquals("number emitted tuples", 2, sink.collectedTuples.size());

    for (Object o: sink.collectedTuples) {

      TestInputPOJO document = (TestInputPOJO)o;
      if (document.getId().equals(testDocumentId1)) {
        Assert.assertEquals("test1", document.getName());
        Assert.assertEquals("charvalue in document", 'd', document.getInnerObj().getCharVal());
        Assert.assertEquals("name of ducement", "testString1", document.getInnerObj().getStringVal());
        Assert.assertEquals("age of ducement", 23, document.getAge());
        Assert.assertEquals("intvalue of ducement", 12, document.getInnerObj().getIntVal());
      }
      if (document.getId().equals(testDocumentId2)) {
        Assert.assertEquals("test2", document.getName());
        Assert.assertEquals("char in document", 'p', document.getInnerObj().getCharVal());
        Assert.assertEquals("name of ducement", "testString2", document.getInnerObj().getStringVal());
        Assert.assertEquals("intvalue of ducement", 24, document.getInnerObj().getIntVal());
        Assert.assertEquals("age of ducement", 32, document.getAge());
      }
    }
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
